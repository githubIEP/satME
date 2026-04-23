"""Sentinel-2 L2A source via the Copernicus Data Space Ecosystem (CDSE).

Used as an automatic fallback when the GEE ``S2_SR_HARMONIZED`` archive is
incomplete — typically before 2019 for many sub-Saharan Africa tiles, where
ESA's retroprocessed L2A products exist on CDSE but were not fully ingested
into GEE.

API used
--------
OData product catalogue (``catalogue.dataspace.copernicus.eu/odata/v1``)
    Product search and Nodes navigation.  The STAC API at this domain does not
    index Sentinel-2; OData is the authoritative product search interface.

Flow
----
Phase 1 (filter)
    search_products()        — OData search with AOI + date + cloud filters.
    compute_aoi_cloud_pct()  — Reads only the SCL band over the AOI to decide
                               whether each candidate passes the AOI cloud
                               threshold.  Small window read; fast for tiny AOIs.

Phase 2 (compute)
    compute_stats_for_item() — Reads the needed spectral bands over the AOI,
                               applies the SCL cloud mask, computes index arrays,
                               and returns the same statistics dict format as the
                               GEE path (mean, std, min, max, p10–p90).

Band URL construction
---------------------
Band file names are derivable from the product name:
  ``{tile}_{sensing_datetime}_{band}_{resolution}m.jp2``
The granule directory name (which encodes the absolute orbit and tile-sensing
time) is fetched once per product via the OData Nodes API and cached.

Authentication
--------------
Bearer token required for Nodes API calls and band data downloads.
Injected via GDAL_HTTP_HEADERS so rasterio's /vsicurl/ driver can read
auth-gated JP2 files by streaming only the pixels over the AOI.

Reflectance scale
-----------------
S2 L2A DN values are scaled 0–10 000, representing [0.0, 1.0].
"""

import contextlib
import logging
import os
from typing import Any

import numpy as np
import requests

logger = logging.getLogger(__name__)

# ── CDSE API endpoints ────────────────────────────────────────────────────────
_ODATA_URL   = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
_NODES_BASE  = "https://catalogue.dataspace.copernicus.eu/odata/v1"

# ── Microsoft Planetary Computer — primary COG backend ────────────────────────
# Hosts Sentinel-2 L2A as cloud-optimised GeoTIFFs on Azure Blob Storage.
# Coverage: global, 2016-present.  Access requires a free, anonymous SAS token
# obtained from the /api/sas/v1/token endpoint (no account needed, ~24h TTL).
# Band asset names on MPC match our band identifiers directly (B02, B08, SCL…).
_MPC_STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
_MPC_SAS_URL  = "https://planetarycomputer.microsoft.com/api/sas/v1/token/sentinel-2-l2a"

# ── Element84 Earth Search — fallback COG backend on AWS ─────────────────────
# Coverage starts from ~late 2018 for many tiles; used only when MPC lookup
# fails.  NOTE: Earth Search does not expose an s2:mgrs_tile filter field;
# bbox filtering alone is sufficient and more reliable.
_EARTH_SEARCH_URL = "https://earth-search.aws.element84.com/v1/search"

# Maps our band identifiers to Earth Search asset names
_ES_ASSET: dict[str, str] = {
    "B02": "blue",  "B03": "green", "B04": "red",
    "B08": "nir",   "B8A": "nir08", "B11": "swir16",
    "B12": "swir22", "SCL": "scl",
}

# ── SCL classes treated as "clear" (same logic as GEE SCL mask) ──────────────
# 4 = vegetation, 5 = bare soil / sparse vegetation, 6 = water,
# 7 = unclassified (not cloud), 11 = snow / ice
_SCL_CLEAR = frozenset({4, 5, 6, 7, 11})

# ── S2 L2A DN → reflectance scale ────────────────────────────────────────────
_S2_SCALE = 10_000.0

# ── Band resolution lookup (metres) ──────────────────────────────────────────
_BAND_RES: dict[str, int] = {
    "B02": 10, "B03": 10, "B04": 10, "B08": 10,
    "B8A": 20, "B11": 20, "B12": 20, "SCL": 20,
}

# ── Bands needed per index ────────────────────────────────────────────────────
_INDEX_BANDS: dict[str, list[str]] = {
    "NDVI":  ["B04", "B08"],
    "NDWI":  ["B03", "B08"],
    "NDMI":  ["B8A", "B11"],
    "EVI":   ["B02", "B04", "B08"],
    "SAVI":  ["B04", "B08"],
    "MNDWI": ["B03", "B11"],
    "NDBI":  ["B08", "B11"],
    "BSI":   ["B02", "B04", "B08", "B11"],
    "GNDVI": ["B03", "B08"],
}


# ─────────────────────────────────────────────────────────────────────────────
# COG URL lookup — Microsoft Planetary Computer (primary) + Earth Search (fallback)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_mpc_sas_token(session: "requests.Session | None" = None) -> str | None:
    """Fetch a free anonymous SAS token for the Planetary Computer Sentinel-2 container.

    The token is valid for ~24 hours and requires no account.  Returns None on
    any network error so callers can fall through to the Earth Search path.
    """
    _http = session or requests
    try:
        resp = _http.get(_MPC_SAS_URL, timeout=15)
        resp.raise_for_status()
        return resp.json()["token"]
    except Exception as exc:
        logger.warning("Planetary Computer SAS token fetch failed: %s", exc)
        return None


def _query_mpc_cog_index(
    bounds_wgs84: tuple[float, float, float, float],
    start_date: str,
    end_date: str,
    sas_token: str,
    session: "requests.Session | None" = None,
) -> dict[str, dict[str, str]]:
    """Query Planetary Computer STAC and return a {YYYYMMDD: {band: signed_url}} index.

    Band asset names on MPC match our identifiers directly (B02, B03, B04,
    B08, B8A, B11, B12, SCL).  URLs are signed with the SAS token by appending
    ``?{sas_token}`` so GDAL /vsicurl/ can read them without any auth header.
    """
    west, south, east, north = bounds_wgs84
    _http = session or requests
    cog_index: dict[str, dict[str, str]] = {}

    # Use a high limit to minimise pagination round-trips.
    # ~3 years × 36–73 passes/year = at most ~200 items; 500 safely covers any range.
    base_body: dict = {
        "collections": ["sentinel-2-l2a"],
        "bbox": [west, south, east, north],
        "datetime": f"{start_date}T00:00:00Z/{end_date}T23:59:59Z",
        "limit": 500,
    }

    next_href: str | None = _MPC_STAC_URL

    while next_href:
        try:
            # MPC pagination: POST the original body to the "next" link URL.
            # The next link includes a ?token=... query param that encodes the
            # cursor; the body must be re-sent unchanged.
            resp = _http.post(next_href, json=base_body, timeout=60)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("Planetary Computer STAC query failed: %s", exc)
            break

        data     = resp.json()
        features = data.get("features", [])

        for item in features:
            dt_str   = item.get("properties", {}).get("datetime", "")
            date_key = dt_str[:10].replace("-", "")   # "2016-01-05T..." → "20160105"
            assets   = item.get("assets", {})
            # MPC band names match our identifiers; sign each URL with SAS token
            band_urls = {
                band: f"{assets[band]['href']}?{sas_token}"
                for band in ("B02", "B03", "B04", "B08", "B8A", "B11", "B12", "SCL")
                if band in assets
            }
            if band_urls and len(band_urls) > len(cog_index.get(date_key, {})):
                cog_index[date_key] = band_urls

        # Pagination — MPC uses a "next" link with a cursor token in the URL
        next_link = next(
            (lnk for lnk in data.get("links", []) if lnk.get("rel") == "next"),
            None,
        )
        next_href = next_link.get("href") if next_link and features else None

    return cog_index


def _query_es_cog_index(
    bounds_wgs84: tuple[float, float, float, float],
    start_date: str,
    end_date: str,
    session: "requests.Session | None" = None,
) -> dict[str, dict[str, str]]:
    """Query Element84 Earth Search STAC and return a {YYYYMMDD: {band: url}} index.

    Note: Earth Search coverage starts from ~late 2018 for many non-US tiles.
    The ``s2:mgrs_tile`` field is NOT a valid Earth Search filter — bbox is used
    instead, which is sufficient for a tile-specific AOI.
    """
    west, south, east, north = bounds_wgs84
    _http = session or requests
    cog_index: dict[str, dict[str, str]] = {}
    es_token: str | None = None

    while True:
        body: dict = {
            "collections": ["sentinel-2-l2a"],
            "bbox": [west, south, east, north],
            "datetime": f"{start_date}T00:00:00Z/{end_date}T23:59:59Z",
            "limit": 100,
        }
        if es_token:
            body["token"] = es_token

        try:
            resp = _http.post(_EARTH_SEARCH_URL, json=body, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("Earth Search query failed: %s", exc)
            break

        data     = resp.json()
        features = data.get("features", [])

        for item in features:
            dt_str   = item.get("properties", {}).get("datetime", "")
            date_key = dt_str[:10].replace("-", "")
            assets   = item.get("assets", {})
            band_urls = {
                band: assets[name]["href"]
                for band, name in _ES_ASSET.items()
                if name in assets
            }
            if band_urls and len(band_urls) > len(cog_index.get(date_key, {})):
                cog_index[date_key] = band_urls

        next_link = next(
            (lnk for lnk in data.get("links", []) if lnk.get("rel") == "next"),
            None,
        )
        if not next_link or not features:
            break
        from urllib.parse import parse_qs, urlparse
        qs       = parse_qs(urlparse(next_link.get("href", "")).query)
        es_token = qs.get("token", [None])[0]
        if not es_token:
            break

    return cog_index


def _attach_cog_urls(
    products: list[dict],
    bounds_wgs84: tuple[float, float, float, float],
    start_date: str,
    end_date: str,
    session: "requests.Session | None" = None,
) -> None:
    """Attach COG asset URLs to each product dict for band reads.

    Sets ``product["_cog_urls"] = {band: url}`` in-place.  Products without a
    match keep ``_cog_urls`` unset and fall back to the CDSE Nodes path (which
    often fails with DAT-ZIP-604 on corporate networks).

    Backend priority
    ----------------
    1. Microsoft Planetary Computer — global coverage from 2016, free anonymous
       SAS token, band names match our identifiers directly.
    2. Element84 Earth Search — fallback for any dates MPC missed; note that
       Earth Search coverage typically starts from late 2018 for non-US tiles.
    """
    if not products:
        return

    # ── 1. Planetary Computer (primary) ──────────────────────────────────────
    mpc_sas = _fetch_mpc_sas_token(session)
    mpc_index: dict[str, dict[str, str]] = {}
    if mpc_sas:
        mpc_index = _query_mpc_cog_index(
            bounds_wgs84, start_date, end_date, mpc_sas, session
        )
        logger.info(
            "Planetary Computer: %d dates indexed (%s – %s)",
            len(mpc_index), start_date, end_date,
        )
    else:
        logger.warning("Planetary Computer SAS token unavailable — skipping MPC lookup")

    # ── 2. Earth Search (fallback for dates not found on MPC) ────────────────
    # Build the set of sensing dates still unmatched after MPC
    sensing_dates = set()
    for product in products:
        pparts = product.get("Name", "").replace(".SAFE", "").split("_")
        if len(pparts) >= 3:
            sensing_dates.add(pparts[2][:8])

    unmatched_dates = sensing_dates - set(mpc_index)
    es_index: dict[str, dict[str, str]] = {}
    if unmatched_dates:
        es_index = _query_es_cog_index(bounds_wgs84, start_date, end_date, session)
        logger.info(
            "Earth Search: %d dates indexed (%s – %s)",
            len(es_index), start_date, end_date,
        )

    cog_index = {**es_index, **mpc_index}   # MPC takes precedence on overlap

    # ── Attach to each CDSE product ───────────────────────────────────────────
    matched = 0
    for product in products:
        pparts = product.get("Name", "").replace(".SAFE", "").split("_")
        if len(pparts) >= 3:
            sensing_date = pparts[2][:8]
            if sensing_date in cog_index:
                product["_cog_urls"] = cog_index[sensing_date]
                matched += 1

    logger.info(
        "COG lookup: %d/%d CDSE products matched (MPC: %d dates, ES: %d dates)",
        matched, len(products), len(mpc_index), len(es_index),
    )


# ─────────────────────────────────────────────────────────────────────────────
# OData product search
# ─────────────────────────────────────────────────────────────────────────────

def search_products(
    bounds_wgs84: tuple[float, float, float, float],
    start_date: str,
    end_date: str,
    max_tile_cloud_pct: float = 100.0,
    session: "requests.Session | None" = None,
) -> list[dict]:
    """Search CDSE OData API for Sentinel-2 L2A products intersecting the AOI.

    Parameters
    ----------
    bounds_wgs84 : (west, south, east, north) in EPSG:4326.
    start_date, end_date : ISO date strings (YYYY-MM-DD), inclusive.
    max_tile_cloud_pct : scene-level cloud cover ceiling (0–100).
    session : requests.Session with proxy configured (from copernicus_auth).

    Returns
    -------
    list[dict]
        OData Product dicts, sorted by acquisition date.
    """
    west, south, east, north = bounds_wgs84
    # WKT polygon for the OData INTERSECTS filter
    wkt = (
        f"POLYGON(({west} {south},{east} {south},"
        f"{east} {north},{west} {north},{west} {south}))"
    )

    odata_filter = (
        "Collection/Name eq 'SENTINEL-2' "
        "and Attributes/OData.CSC.StringAttribute/any("
        "    att:att/Name eq 'productType' "
        "    and att/OData.CSC.StringAttribute/Value eq 'S2MSI2A') "
        f"and ContentDate/Start ge {start_date}T00:00:00.000Z "
        f"and ContentDate/Start lt {end_date}T23:59:59.000Z "
        f"and OData.CSC.Intersects(area=geography'SRID=4326;{wkt}') "
        "and Attributes/OData.CSC.DoubleAttribute/any("
        "    att:att/Name eq 'cloudCover' "
        f"   and att/OData.CSC.DoubleAttribute/Value le {max_tile_cloud_pct})"
    )

    _http = session or requests
    items: list[dict] = []
    skip = 0
    top  = 100

    while True:
        try:
            resp = _http.get(
                _ODATA_URL,
                params={
                    "$filter":  odata_filter,
                    "$top":     top,
                    "$skip":    skip,
                    "$orderby": "ContentDate/Start asc",
                    "$expand":  "Attributes",
                },
                timeout=60,
            )
        except requests.exceptions.ConnectionError as exc:
            raise RuntimeError(
                f"Cannot reach CDSE OData API: {exc}\n"
                "  Check connectivity to catalogue.dataspace.copernicus.eu.\n"
                "  On a corporate network, set auth.https_proxy in your YAML."
            ) from exc
        resp.raise_for_status()
        batch = resp.json().get("value", [])
        items.extend(batch)
        if len(batch) < top:
            break
        skip += top

    logger.info(
        "CDSE OData: %d L2A products found (%s – %s, tile cloud ≤ %.0f%%)",
        len(items), start_date, end_date, max_tile_cloud_pct,
    )

    # Attach Earth Search COG URLs for band reads (CDSE download auth is broken)
    _attach_cog_urls(items, bounds_wgs84, start_date, end_date, session)

    return items


# ─────────────────────────────────────────────────────────────────────────────
# Band URL construction via Nodes API
# ─────────────────────────────────────────────────────────────────────────────

def _granule_dir_from_product(product: dict) -> str | None:
    """Derive the GRANULE subdirectory name from OData product metadata.

    Avoids the Nodes API entirely by constructing the compact-format granule
    directory name from fields returned by the OData search (with
    ``$expand=Attributes``).

    S2 L2A compact SAFE granule directory naming convention
    -------------------------------------------------------
    ``L2A_T{tile}_{orbit_tag}_{sensing_dt}``

    where:
      - ``tile``       = MGRS tile ID including the "T" prefix, e.g. ``T37MCU``
      - ``orbit_tag``  = ``A`` + absolute orbit number zero-padded to 6 digits
      - ``sensing_dt`` = sensing start in ``YYYYMMDDTHHmmSS`` format

    Both ``tile`` and ``sensing_dt`` come from the product ``Name``; the
    absolute orbit number comes from the ``Attributes`` list returned by OData
    when ``$expand=Attributes`` is included in the search query.

    Returns ``None`` if any required field is missing or unparseable.
    """
    safe_name = product.get("Name", "")
    parts = safe_name.replace(".SAFE", "").split("_")
    # parts: [S2A, MSIL2A, YYYYMMDDTHHMMSS, Nxxx, Rxxx, Ttile, YYYYMMDDTHHMMSS]
    if len(parts) < 6:
        logger.debug("Cannot parse safe_name parts: %s", safe_name)
        return None

    sensing_dt = parts[2]   # e.g. 20160105T075212
    tile       = parts[5]   # e.g. T37MCU

    # Extract absoluteOrbitNumber from the Attributes array.
    # CDSE OData returns this field as "absoluteOrbitNumber"; "orbitNumber"
    # is an alias that some older responses used — try both for resilience.
    abs_orbit = None
    for attr in product.get("Attributes", []):
        if attr.get("Name") in ("absoluteOrbitNumber", "orbitNumber"):
            try:
                abs_orbit = int(float(attr["Value"]))
            except (KeyError, ValueError, TypeError):
                pass
            break

    if abs_orbit is None:
        attr_names = [a.get("Name") for a in product.get("Attributes", [])]
        logger.warning(
            "absoluteOrbitNumber not in Attributes for %s — available: %s",
            safe_name, attr_names,
        )
        return None

    granule_dir = f"L2A_{tile}_A{abs_orbit:06d}_{sensing_dt}"
    logger.debug("Derived granule dir: %s", granule_dir)
    return granule_dir


def _get_granule_dir(
    product_id: str,
    safe_name: str,
    token: str,
    session: "requests.Session | None" = None,
) -> str | None:
    """Return the granule directory name for a product.

    Tries attribute-based derivation first (no network call, no auth required).
    The product dict must have been fetched with ``$expand=Attributes`` for this
    to work — which ``search_products`` does by default.

    The Nodes API fallback is retained but CDSE's download microservice
    (DAT-ZIP) currently rejects standard OAuth Bearer tokens with error
    DAT-ZIP-604 ("Token not found"), so it will rarely succeed on corporate
    networks with SSL-inspecting proxies (e.g. Zscaler).
    """
    # Primary: derive from OData attributes (no auth, no network call)
    # We don't have the full product dict here so this is handled upstream;
    # this function is only reached as a fallback when the cache miss occurs.
    # See compute_aoi_cloud_pct and compute_stats_for_item which call
    # _granule_dir_from_product first.
    _http = session or requests
    url = (
        f"{_NODES_BASE}/Products('{product_id}')"
        f"/Nodes('{safe_name}')/Nodes('GRANULE')/Nodes"
    )
    for attempt, headers in enumerate([
        {},
        {"Authorization": f"Bearer {token}"},
    ]):
        try:
            resp = _http.get(url, headers=headers, timeout=15)
        except Exception as exc:
            logger.debug("Nodes API connection error (attempt %d) for %s: %s",
                         attempt + 1, safe_name, exc)
            continue
        if resp.status_code == 200:
            body   = resp.json()
            result = body.get("result") or body.get("value") or []
            if result:
                return result[0]["Name"]
            return None
        if resp.status_code in (401, 403) and attempt == 0:
            continue
        logger.debug("Nodes API HTTP %d for %s (attempt %d)",
                     resp.status_code, safe_name, attempt + 1)
    return None


def _make_band_url(
    product_id: str,
    safe_name: str,
    granule_dir: str,
    band: str,
) -> str:
    """Construct the OData download URL for a specific JP2 band file.

    Band file names follow ESA's naming convention:
      ``{tile}_{sensing_datetime}_{band}_{resolution}m.jp2``
    where tile and sensing_datetime are parsed from the product safe name.
    """
    # safe_name format: S2A_MSIL2A_YYYYMMDDTHHMMSS_Nxxx_Rxxx_Txxxxx_YYYYMMDDTHHMMSS.SAFE
    parts       = safe_name.replace(".SAFE", "").split("_")
    sensing_dt  = parts[2]   # e.g. 20160613T073612
    tile        = parts[5]   # e.g. T37MCU  (includes T prefix)
    resolution  = _BAND_RES.get(band, 20)
    filename    = f"{tile}_{sensing_dt}_{band}_{resolution}m.jp2"

    return (
        f"{_NODES_BASE}/Products('{product_id}')"
        f"/Nodes('{safe_name}')"
        f"/Nodes('GRANULE')"
        f"/Nodes('{granule_dir}')"
        f"/Nodes('IMG_DATA')"
        f"/Nodes('R{resolution}m')"
        f"/Nodes('{filename}')/$value"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Band reading
# ─────────────────────────────────────────────────────────────────────────────

@contextlib.contextmanager
def _gdal_auth_headers(token: str):
    """Inject bearer token into GDAL HTTP headers, then restore original value."""
    old_hdrs    = os.environ.get("GDAL_HTTP_HEADERS")
    old_readdir = os.environ.get("GDAL_DISABLE_READDIR_ON_OPEN")
    os.environ["GDAL_HTTP_HEADERS"]           = f"Authorization: Bearer {token}"
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
    try:
        yield
    finally:
        if old_hdrs is None:
            os.environ.pop("GDAL_HTTP_HEADERS", None)
        else:
            os.environ["GDAL_HTTP_HEADERS"] = old_hdrs
        if old_readdir is None:
            os.environ.pop("GDAL_DISABLE_READDIR_ON_OPEN", None)
        else:
            os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = old_readdir


def _read_band_window(
    href: str,
    bounds_wgs84: tuple[float, float, float, float],
    token: str,
    session: "requests.Session | None" = None,
) -> np.ndarray | None:
    """Read AOI pixels from a CDSE band asset.

    Primary path — GDAL /vsicurl/ with HTTP range requests (only downloads
    the compressed JP2 tiles that overlap the AOI, typically a few KB).

    Fallback path — requests download into a MemoryFile.  CDSE's Nodes
    ``/$value`` endpoint redirects to ``download.dataspace.copernicus.eu``,
    which GDAL/libcurl cannot resolve on some corporate networks (Zscaler
    routes the catalog subdomain but not the download subdomain at the DNS
    level).  Python's ``requests`` library operates at the application layer
    and follows the redirect through Zscaler's WFP driver, which handles the
    redirect transparently.  For the SCL band (~3–5 MB compressed JP2) this
    is acceptable; for spectral bands (10–30 MB) it adds a few seconds per
    image.
    """
    import io
    import rasterio
    from rasterio.io import MemoryFile
    from rasterio.windows import from_bounds
    from rasterio.warp import transform_bounds

    def _window_from_src(src):
        aoi_in_src = transform_bounds(
            "EPSG:4326", src.crs,
            bounds_wgs84[0], bounds_wgs84[1],
            bounds_wgs84[2], bounds_wgs84[3],
        )
        return from_bounds(*aoi_in_src, transform=src.transform)

    # ── Primary: GDAL /vsicurl/ (range-request streaming) ────────────────────
    vsi_href = "/vsicurl/" + href if href.startswith("http") else href
    # Only inject Bearer auth for CDSE catalogue URLs; Earth Search COGs are
    # public S3 objects and must NOT receive a Bearer header (AWS ignores it
    # but some edge-proxies reject it).
    needs_auth = "catalogue.dataspace.copernicus.eu" in href
    try:
        ctx = _gdal_auth_headers(token) if needs_auth else contextlib.nullcontext()
        with ctx:
            with rasterio.open(vsi_href) as src:
                arr = src.read(1, window=_window_from_src(src))
        return arr
    except Exception as exc:
        exc_str = str(exc)
        # Fall through to requests fallback for redirect/DNS/auth issues on the
        # download host.  GDAL forwards our Bearer token to the redirect target
        # (download.dataspace.copernicus.eu) which rejects it with 401; requests
        # strips Authorization on cross-domain redirects so the CDN may serve
        # the file without auth.
        _fallback_triggers = ("301", "401", "403", "resolve")
        if not any(t in exc_str.lower() for t in _fallback_triggers):
            logger.debug("Band read failed (%s): %s", href.split("/")[-2], exc)
            return None
        logger.debug(
            "vsicurl failed (%s) for %s — trying requests fallback",
            exc_str[:60], href.split("/")[-2],
        )

    # ── Fallback: download via requests into MemoryFile ───────────────────────
    # Send the Bearer token to the catalogue endpoint only.  Python requests
    # strips Authorization on cross-domain redirects (catalogue → download),
    # which is intentional: the download CDN serves data publicly and rejects
    # forwarded Bearer tokens with 401.
    _http = session or requests
    try:
        resp = _http.get(
            href,
            headers={"Authorization": f"Bearer {token}"},
            timeout=120,
            stream=False,
        )
        if resp.status_code != 200:
            logger.warning(
                "Requests fallback HTTP %d for %s — body: %s",
                resp.status_code, href.split("/")[-2], resp.text[:200],
            )
            return None
        logger.debug(
            "Requests fallback downloaded %d bytes for %s",
            len(resp.content), href.split("/")[-2],
        )
        with MemoryFile(io.BytesIO(resp.content)) as memfile:
            with memfile.open() as src:
                arr = src.read(1, window=_window_from_src(src))
        return arr
    except Exception as exc2:
        logger.warning(
            "Requests fallback failed (%s): %s", href.split("/")[-2], exc2
        )
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Cloud assessment (Phase 1)
# ─────────────────────────────────────────────────────────────────────────────

def compute_aoi_cloud_pct(
    product: dict,
    bounds_wgs84: tuple[float, float, float, float],
    token: str,
    session: "requests.Session | None" = None,
) -> float | None:
    """Return AOI cloud % by reading only the SCL band.

    Returns None if the SCL band could not be read (product excluded).
    Uses ``product["_granule_dir"]`` if already cached by the caller to
    avoid a redundant Nodes API round-trip.
    """
    product_id  = product["Id"]
    safe_name   = product["Name"]

    # Prefer Earth Search COG URL — public S3, no auth required, no granule_dir needed.
    # Fall back to constructing a CDSE Nodes URL only when no COG URL was found by
    # _attach_cog_urls (e.g. the tile predates the Earth Search index).
    cog_urls = product.get("_cog_urls", {})
    if "SCL" in cog_urls:
        scl_url = cog_urls["SCL"]
    else:
        # Derive granule dir from OData attributes (no auth, no network call).
        # Falls back to the Nodes API if attributes are missing, but that path
        # often fails on corporate networks (DAT-ZIP-604 / 401).
        granule_dir = (
            product.get("_granule_dir")
            or _granule_dir_from_product(product)
            or _get_granule_dir(product_id, safe_name, token, session)
        )
        if not granule_dir:
            logger.warning(
                "No granule dir and no COG URL for %s — excluding from cloud check",
                safe_name,
            )
            return None
        scl_url = _make_band_url(product_id, safe_name, granule_dir, "SCL")

    scl     = _read_band_window(scl_url, bounds_wgs84, token, session)
    if scl is None or scl.size == 0:
        logger.warning("SCL read returned no data for %s — excluding", safe_name)
        return None

    clear_px = np.isin(scl, list(_SCL_CLEAR)).sum()
    cloud_pct = float((1.0 - clear_px / scl.size) * 100.0)
    logger.debug("SCL cloud check %s → %.1f%% cloudy (%d px)", safe_name, cloud_pct, scl.size)
    return cloud_pct


# ─────────────────────────────────────────────────────────────────────────────
# Index computation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _compute_index(bands: dict[str, np.ndarray], name: str) -> np.ndarray | None:
    eps = 1e-10
    b = bands
    if name == "NDVI"  and {"B04", "B08"} <= b.keys():
        return (b["B08"] - b["B04"]) / (b["B08"] + b["B04"] + eps)
    if name == "NDWI"  and {"B03", "B08"} <= b.keys():
        return (b["B03"] - b["B08"]) / (b["B03"] + b["B08"] + eps)
    if name == "NDMI"  and {"B8A", "B11"} <= b.keys():
        return (b["B8A"] - b["B11"]) / (b["B8A"] + b["B11"] + eps)
    if name == "MNDWI" and {"B03", "B11"} <= b.keys():
        return (b["B03"] - b["B11"]) / (b["B03"] + b["B11"] + eps)
    if name == "NDBI"  and {"B08", "B11"} <= b.keys():
        return (b["B11"] - b["B08"]) / (b["B11"] + b["B08"] + eps)
    if name == "GNDVI" and {"B03", "B08"} <= b.keys():
        return (b["B08"] - b["B03"]) / (b["B08"] + b["B03"] + eps)
    if name == "SAVI"  and {"B04", "B08"} <= b.keys():
        return 1.5 * (b["B08"] - b["B04"]) / (b["B08"] + b["B04"] + 0.5 + eps)
    if name == "EVI"   and {"B02", "B04", "B08"} <= b.keys():
        return (
            2.5 * (b["B08"] - b["B04"])
            / (b["B08"] + 6 * b["B04"] - 7.5 * b["B02"] + 1 + eps)
        )
    if name == "BSI"   and {"B02", "B04", "B08", "B11"} <= b.keys():
        num = (b["B11"] + b["B04"]) - (b["B08"] + b["B02"])
        den = (b["B11"] + b["B04"]) + (b["B08"] + b["B02"])
        return num / (den + eps)
    return None


def _array_stats(arr: np.ndarray, percentiles: list[int]) -> dict[str, float]:
    return {
        "mean": float(np.mean(arr)),
        "std":  float(np.std(arr)),
        "min":  float(np.min(arr)),
        "max":  float(np.max(arr)),
        **{f"p{p}": float(np.percentile(arr, p)) for p in percentiles},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Full stats computation (Phase 2)
# ─────────────────────────────────────────────────────────────────────────────

def compute_stats_for_item(
    item: dict,
    bounds_wgs84: tuple[float, float, float, float],
    indices: list[str],
    stats_cfg: dict,
    token: str,
    session: "requests.Session | None" = None,
) -> dict[str, Any]:
    """Compute index statistics for one OData product over the AOI.

    Returns a dict with ``{INDEX}_mean`` etc. keys (same format as GEE path).
    May be empty if no valid pixels were found or band reads failed.
    """
    percentiles  = stats_cfg.get("percentiles", [10, 25, 50, 75, 90])
    product_id   = item["Id"]
    safe_name    = item["Name"]

    # Prefer Earth Search COG URLs — public S3, no auth, no granule_dir required.
    cog_urls = item.get("_cog_urls", {})

    # Only resolve granule_dir when at least one band lacks a COG URL.
    def _band_url(band: str) -> str | None:
        if band in cog_urls:
            return cog_urls[band]
        return _make_band_url(product_id, safe_name, _granule_dir_val, band) \
            if _granule_dir_val else None

    # Resolve granule_dir lazily — skip when all needed bands have COG URLs.
    needed_bands: set[str] = {"SCL"}
    for idx in indices:
        needed_bands.update(_INDEX_BANDS.get(idx, []))

    if all(b in cog_urls for b in needed_bands):
        _granule_dir_val = None  # not needed
    else:
        _granule_dir_val = (
            item.get("_granule_dir")
            or _granule_dir_from_product(item)
            or _get_granule_dir(product_id, safe_name, token, session)
        )
        if not _granule_dir_val and not cog_urls:
            logger.warning("compute_stats: no granule dir and no COG URLs for %s", safe_name)
            return {}

    # ── SCL cloud mask ────────────────────────────────────────────────────────
    scl_url = _band_url("SCL")
    if not scl_url:
        logger.warning("compute_stats: no SCL URL for %s", safe_name)
        return {}
    scl_arr = _read_band_window(scl_url, bounds_wgs84, token, session)
    if scl_arr is None or scl_arr.size == 0:
        return {}

    clear_mask_full = np.isin(scl_arr, list(_SCL_CLEAR))

    # ── Read spectral bands ───────────────────────────────────────────────────
    needed: set[str] = set()
    for idx in indices:
        needed.update(_INDEX_BANDS.get(idx, []))

    band_arrays: dict[str, np.ndarray] = {}
    for band in needed:
        url = _band_url(band)
        if not url:
            logger.debug("compute_stats: no URL for band %s in %s", band, safe_name)
            continue
        arr = _read_band_window(url, bounds_wgs84, token, session)
        if arr is not None:
            band_arrays[band] = arr.astype(float) / _S2_SCALE

    # ── Compute index statistics ──────────────────────────────────────────────
    result: dict[str, Any] = {}
    for idx_name in indices:
        idx_arr = _compute_index(band_arrays, idx_name)
        if idx_arr is None:
            logger.debug("Cannot compute %s — missing bands", idx_name)
            continue

        # Resize clear_mask to index array dimensions if resolutions differ.
        # SCL is always 20 m; 10 m bands (B02/B03/B04/B08) produce arrays
        # roughly 2× larger.  Both were read from identical geographic bounds,
        # so this is a pure array resize — no CRS reprojection needed.
        # Nearest-neighbour via numpy index mapping avoids the rasterio.warp
        # dependency on src/dst transforms (which are unavailable here).
        if idx_arr.shape != clear_mask_full.shape:
            src_rows, src_cols = clear_mask_full.shape
            dst_rows, dst_cols = idx_arr.shape
            row_idx = (np.arange(dst_rows) * src_rows / dst_rows).astype(int)
            col_idx = (np.arange(dst_cols) * src_cols / dst_cols).astype(int)
            clear_mask = clear_mask_full[np.ix_(row_idx, col_idx)]
        else:
            clear_mask = clear_mask_full

        valid = idx_arr[clear_mask & np.isfinite(idx_arr)]
        valid = valid[(valid >= -1.0) & (valid <= 1.0)]
        if len(valid) == 0:
            continue

        for stat_key, val in _array_stats(valid, percentiles).items():
            result[f"{idx_name}_{stat_key}"] = val

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Metadata conversion
# ─────────────────────────────────────────────────────────────────────────────

def item_to_meta(
    product: dict,
    aoi_cloud_pct: float | None = None,
    granule_dir: str | None = None,
) -> dict:
    """Convert an OData product dict into a pipeline-compatible metadata dict."""
    # ContentDate.Start format: "2016-06-13T07:36:12.000Z"
    dt_raw   = product.get("ContentDate", {}).get("Start", "")
    img_date = dt_raw[:10] if dt_raw else ""

    # Tile and orbit from product name:
    # S2A_MSIL2A_YYYYMMDDTHHMMSS_Nxxx_Rxxx_Txxxxx_...
    name_parts = product.get("Name", "").replace(".SAFE", "").split("_")
    mgrs_tile  = name_parts[5] if len(name_parts) > 5 else None
    orbit_rel  = name_parts[4].lstrip("R") if len(name_parts) > 4 else None

    return {
        "image_id":      product.get("Name", product.get("Id", "")),
        "date":          img_date,
        "tile_cloud_pct": product.get("CloudCover"),  # may be None from OData
        "aoi_cloud_pct": aoi_cloud_pct,
        "aoi_covered":   True,
        "orbit_number":  orbit_rel,
        "mgrs_tile":     mgrs_tile,
        "_cdse":         True,        # routing flag
        "_cdse_item":    product,     # full product dict for Phase 2
        "_granule_dir":  granule_dir, # cached from Phase 1 to avoid repeat call
    }
