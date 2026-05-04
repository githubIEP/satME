"""Sentinel-2 ingestion for the analysis layer.

Date-split strategy mirroring satme/sources/copernicus_s2.py in the main
satme pipeline:

  ── on or after GEE_CUTOFF_DATE  →  Google Earth Engine
                                     (COPERNICUS/S2_SR_HARMONIZED)
  ── before GEE_CUTOFF_DATE       →  Microsoft Planetary Computer
                                     (sentinel-2-l2a STAC + COGs)

GEE's harmonised collection is incomplete for many MGRS tiles before ~2019,
so falling back to MPC for the pre-cutoff range is what the satme main
pipeline already does — we mirror that here so Phase 4 sees a complete
archive.

Both branches return raw band datasets (B3, B4, B8, B11, SCL) on the same
spatial grid; the post-concat ``_finalize`` step computes NDVI / MNDWI /
valid / period / doy / is_drought once over the merged cube.
"""

from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import logging

import ee
import numpy as np
import pandas as pd
import requests
import rioxarray  # noqa: F401
import xarray as xr
from tqdm import tqdm

from satme.image_filter import prefilter_by_aoi_cloud, batch_image_metadata
from satme.sources.sentinel2 import Sentinel2Source

from . import cache, indices, _runtime_config as config

logger = logging.getLogger(__name__)

# Bands needed for NDVI (B4, B8), MNDWI (B3, B11), valid-mask (SCL).
_BANDS_GEE = ["B3", "B4", "B8", "B11", "SCL"]
_BANDS_MPC = ["B03", "B04", "B08", "B11", "SCL"]
# Variable name in the merged dataset (after we rename MPC's B0X → BX)
_BAND_VARS = ["B3", "B4", "B8", "B11", "SCL"]


# ─────────────────────────────────────────────────────────────────────────────
# Geometry helpers
# ─────────────────────────────────────────────────────────────────────────────

def _bbox_wgs84(lat: float, lon: float, half_m: float) -> tuple[float, float, float, float]:
    """Return (west, south, east, north) bbox around (lat, lon) in WGS84."""
    half_lat = half_m / 111_320.0
    half_lon = half_m / (111_320.0 * math.cos(math.radians(lat)))
    return (lon - half_lon, lat - half_lat, lon + half_lon, lat + half_lat)


def _utm_epsg(lat: float, lon: float) -> str:
    """Return the WGS84/UTM EPSG code for a lat/lon."""
    zone = int(math.floor((lon + 180) / 6) + 1)
    return f"EPSG:326{zone:02d}" if lat >= 0 else f"EPSG:327{zone:02d}"


def _split_date_range(date_range: dict, cutoff_iso: str) -> tuple[dict | None, dict | None]:
    """Split a date_range dict at the cutoff. Returns (mpc_dr, gee_dr)."""
    start = pd.Timestamp(date_range["start"])
    end   = pd.Timestamp(date_range["end"])
    cutoff = pd.Timestamp(cutoff_iso)
    mpc_dr: dict | None = None
    gee_dr: dict | None = None
    if start < cutoff:
        mpc_dr = {"start": start.date().isoformat(),
                  "end":   min(end, cutoff - pd.Timedelta(days=1)).date().isoformat()}
    if end >= cutoff:
        gee_dr = {"start": max(start, cutoff).date().isoformat(),
                  "end":   end.date().isoformat()}
    return mpc_dr, gee_dr


# ─────────────────────────────────────────────────────────────────────────────
# GEE raw-bands fetch (post-cutoff)
# ─────────────────────────────────────────────────────────────────────────────

def _download_geotiff(
    image: ee.Image,
    region: ee.Geometry,
    crs: str,
    scale: int = 10,
    retries: int = 3,
) -> bytes:
    last_exc = None
    for attempt in range(retries):
        try:
            url = image.select(_BANDS_GEE).getDownloadURL({
                "region": region,
                "scale": scale,
                "crs":   crs,
                "format": "GEO_TIFF",
            })
            r = requests.get(url, timeout=180)
            r.raise_for_status()
            return r.content
        except Exception as exc:
            last_exc = exc
            logger.warning("GEE download attempt %d failed: %s", attempt + 1, exc)
    raise RuntimeError(f"GeoTIFF download failed after {retries} attempts: {last_exc}")


def _open_geotiff_bytes(gt_bytes: bytes) -> xr.Dataset:
    da = rioxarray.open_rasterio(BytesIO(gt_bytes), masked=False).load()
    data = {b: da.isel(band=i).drop_vars("band") for i, b in enumerate(_BANDS_GEE)}
    return xr.Dataset(data)


def _fetch_gee_raw(
    name: str,
    geom: ee.Geometry,
    crs: str,
    date_range: dict,
    workers: int = 6,
) -> xr.Dataset | None:
    """Filter + download per-scene GeoTIFFs from GEE. Returns raw-bands ds or None."""
    max_aoi_cloud = (1.0 - config.MIN_VALID_FRACTION) * 100.0
    s2 = Sentinel2Source({
        "max_tile_cloud_pct": config.MAX_SCENE_CLOUD,
        "max_aoi_cloud_pct":  max_aoi_cloud,
        "indices":            ["NDVI"],
    })
    raw = s2.get_collection(geom, date_range)
    clean, _full = prefilter_by_aoi_cloud(
        collection=raw,
        aoi=geom,
        max_aoi_cloud_pct=max_aoi_cloud,
        quality_fn=s2.aoi_quality_fn(),
        scale=10,
    )
    metas = [m for m in batch_image_metadata(clean, source=s2) if m.get("date")]
    metas.sort(key=lambda m: m["date"])
    n = len(metas)
    if n == 0:
        print(f"  GEE: no clean scenes in {date_range['start']}..{date_range['end']}")
        return None
    print(f"  GEE: {n} clean scenes in {date_range['start']}..{date_range['end']}")

    def _fetch_one(m):
        img = ee.Image(
            clean.filter(ee.Filter.eq("system:index", m["image_id"])).first()
        )
        gt = _download_geotiff(img, geom, crs, scale=10)
        return m, _open_geotiff_bytes(gt)

    per_image: list[tuple[dict, xr.Dataset]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_fetch_one, m): m for m in metas}
        for fut in tqdm(as_completed(futs), total=n,
                         desc=f"{name} GEE", unit="img"):
            m = futs[fut]
            try:
                per_image.append(fut.result())
            except Exception as exc:
                logger.warning("skip %s — %s", m["image_id"], exc)

    if not per_image:
        return None

    per_image.sort(key=lambda kv: kv[0]["date"])
    times = [pd.Timestamp(m["date"]) for m, _ in per_image]
    datasets = [ds.expand_dims(time=[t]) for (_, ds), t in zip(per_image, times)]
    return xr.concat(datasets, dim="time", join="override")


# ─────────────────────────────────────────────────────────────────────────────
# MPC raw-bands fetch (pre-cutoff)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_mpc_raw(
    name: str,
    bbox: tuple[float, float, float, float],
    crs: str,
    date_range: dict,
) -> xr.Dataset | None:
    """Search MPC STAC + load bands via odc.stac.load. Returns raw-bands ds or None.

    Filters scenes by ``MAX_SCENE_CLOUD`` (tile-level) and ``MIN_VALID_FRACTION``
    (per-pixel SCL count) — same thresholds as the GEE branch.
    """
    try:
        import odc.stac
        import planetary_computer
        import pystac_client
    except ImportError as exc:
        raise SystemExit(
            "MPC fallback requires pystac-client, planetary-computer, and odc-stac. "
            "Add these to requirements.txt or run `pip install pystac-client "
            "planetary-computer odc-stac`."
        ) from exc

    print(f"  MPC: searching {date_range['start']}..{date_range['end']} (bbox={bbox})")
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )

    # Year-chunked search with retries (large date ranges occasionally time out)
    start_year = pd.Timestamp(date_range["start"]).year
    end_year   = pd.Timestamp(date_range["end"]).year
    items = []
    for year in range(start_year, end_year + 1):
        for attempt in range(3):
            try:
                yr = list(
                    catalog.search(
                        collections=["sentinel-2-l2a"],
                        bbox=list(bbox),
                        datetime=f"{max(date_range['start'], f'{year}-01-01')}/"
                                 f"{min(date_range['end'],   f'{year}-12-31')}",
                        query={"eo:cloud_cover": {"lt": config.MAX_SCENE_CLOUD}},
                    ).items()
                )
                items.extend(yr)
                break
            except Exception as exc:
                logger.warning("MPC %d attempt %d failed: %s", year, attempt + 1, exc)
        else:
            logger.warning("MPC %d: gave up after 3 attempts", year)
    if not items:
        print(f"  MPC: no candidates")
        return None
    print(f"  MPC: {len(items)} candidate scenes — loading SCL for AOI cloud filter")

    # Pass 1 — SCL only, compute valid_fraction
    data_scl = odc.stac.load(
        items,
        bands=["SCL"],
        bbox=list(bbox),
        crs=crs,
        resolution=10,
        groupby="solar_day",
        chunks={"time": 16, "x": 128, "y": 128},
    )
    valid_p1 = indices.valid_mask(data_scl["SCL"])
    total_pixels = valid_p1.sizes["x"] * valid_p1.sizes["y"]
    valid_n_p1 = valid_p1.sum(dim=["x", "y"]).compute()
    valid_frac_p1 = valid_n_p1.values / total_pixels
    keep_times = data_scl.time.values[valid_frac_p1 >= config.MIN_VALID_FRACTION]
    if len(keep_times) == 0:
        print("  MPC: no scenes passed AOI cloud filter")
        return None
    print(f"  MPC: {len(keep_times)} clean scenes after AOI cloud filter")

    # Pass 2 — re-sign and load all bands for kept scenes
    keep_dates = {pd.Timestamp(t).date().isoformat() for t in keep_times}
    kept_items = [
        it for it in items
        if pd.Timestamp(it.datetime).date().isoformat() in keep_dates
    ]
    kept_items = [planetary_computer.sign(it) for it in kept_items]

    data = odc.stac.load(
        kept_items,
        bands=_BANDS_MPC,
        bbox=list(bbox),
        crs=crs,
        resolution=10,
        groupby="solar_day",
        chunks={"time": 8, "x": 128, "y": 128},
    )

    # Rename B0X → BX so the merged cube has consistent variable names.
    rename_map = {"B03": "B3", "B04": "B4", "B08": "B8"}
    data = data.rename({k: v for k, v in rename_map.items() if k in data.data_vars})
    return data


# ─────────────────────────────────────────────────────────────────────────────
# Post-concat finalisation (NDVI / MNDWI / valid / period / etc.)
# ─────────────────────────────────────────────────────────────────────────────

def _finalize(ds_raw: xr.Dataset, name: str, lat: float, lon: float, crs: str) -> xr.Dataset:
    """Compute derived bands + per-time metadata over the merged raw cube."""
    valid = indices.valid_mask(ds_raw["SCL"])
    total_pixels = ds_raw.sizes["x"] * ds_raw.sizes["y"]
    valid_n = valid.sum(dim=["x", "y"]).values
    valid_frac = valid_n / total_pixels

    green = (ds_raw["B3"].where(valid)  / 10000.0).astype("float32")
    red   = (ds_raw["B4"].where(valid)  / 10000.0).astype("float32")
    nir   = (ds_raw["B8"].where(valid)  / 10000.0).astype("float32")
    swir  = (ds_raw["B11"].where(valid) / 10000.0).astype("float32")

    ndvi_da  = indices.ndvi(nir, red)
    mndwi_da = indices.mndwi(green, swir)

    times_pd = pd.to_datetime(ds_raw.time.values)
    doy = np.asarray(times_pd.dayofyear)
    bin_idx = np.minimum(
        (doy - 1) // config.DOY_BIN_SIZE,
        config.N_DOY_BINS - 1,
    ).astype("int8")

    times_arr = times_pd.to_numpy()
    dam_dt = np.datetime64(config.DAM_CONSTRUCTION_DATE)
    period = np.where(
        times_arr < dam_dt, "pre",
        np.where(
            (np.asarray(times_pd.year)  == config.DAM_CONSTRUCTION_DATE.year)
            & (np.asarray(times_pd.month) == config.DAM_CONSTRUCTION_DATE.month),
            "build", "post",
        ),
    )
    is_drought = (
        (times_arr >= np.datetime64(config.DROUGHT_START))
        & (times_arr <= np.datetime64(config.DROUGHT_END))
    )

    out = xr.Dataset(
        {
            "ndvi":           ndvi_da,
            "mndwi":          mndwi_da,
            "valid":          valid,
            "valid_fraction": ("time", valid_frac),
            "doy":            ("time", doy),
            "doy_bin":        ("time", bin_idx),
            "period":         ("time", period.astype("U5")),
            "is_drought":     ("time", is_drought),
        },
        attrs={
            "cache_version":      config.CACHE_VERSION,
            "aoi_name":           name,
            "aoi_center_lat":     lat,
            "aoi_center_lon":     lon,
            "min_valid_fraction": config.MIN_VALID_FRACTION,
            "ndvi_threshold":     config.NDVI_VEG_THRESHOLD,
            "mndwi_threshold":    config.MNDWI_WATER_THRESHOLD,
        },
    )
    return out.rio.write_crs(crs)


# ─────────────────────────────────────────────────────────────────────────────
# Top-level entry points
# ─────────────────────────────────────────────────────────────────────────────

def _build_aoi_cube(name: str, lat: float, lon: float, half_m: float, cfg: dict) -> xr.Dataset:
    """Build the per-AOI dataset (MPC pre-cutoff + GEE post-cutoff) and cache it."""
    print(f"\n--- building cache for {name} ({lat}, {lon}) ---")
    bbox = _bbox_wgs84(lat, lon, half_m)
    crs  = _utm_epsg(lat, lon)
    print(f"  bbox: {bbox}\n  CRS:  {crs}")

    mpc_dr, gee_dr = _split_date_range(cfg["date_range"], config.GEE_CUTOFF_DATE)
    print(f"  date split (cutoff {config.GEE_CUTOFF_DATE}): MPC={mpc_dr}  GEE={gee_dr}")

    parts: list[xr.Dataset] = []

    if mpc_dr is not None:
        ds_mpc = _fetch_mpc_raw(name, bbox, crs, mpc_dr)
        if ds_mpc is not None:
            parts.append(ds_mpc)

    if gee_dr is not None:
        geom = ee.Geometry.Rectangle(list(bbox))
        ds_gee = _fetch_gee_raw(name, geom, crs, gee_dr)
        if ds_gee is not None:
            parts.append(ds_gee)

    if not parts:
        raise SystemExit(f"No clean Sentinel-2 scenes for {name}")

    # Align grids: reproject every part to match the first part's grid.
    if len(parts) > 1:
        ref = parts[0]
        aligned = [ref]
        for p in parts[1:]:
            aligned.append(p.rio.write_crs(crs).rio.reproject_match(ref))
        ds_raw = xr.concat(aligned, dim="time", join="override").sortby("time")
    else:
        ds_raw = parts[0].sortby("time")

    out = _finalize(ds_raw, name, lat, lon, crs)
    # Zarr requires uniform chunk sizes; xr.concat preserves per-source chunks
    # (MPC's 8-per-block + GEE's single 155-block), which fails on write.
    # Rechunk along time only — spatial dims are tiny and stay single-chunk.
    out = out.chunk({"time": 64, "x": -1, "y": -1})
    cache.write_zarr_atomic(out, name)
    return xr.open_zarr(cache.cache_path_for(name), consolidated=False)


def build_or_load(name: str, lat: float, lon: float, half_m: float, cfg: dict) -> xr.Dataset:
    """Return the cached zarr if valid; otherwise build it and cache."""
    cached = cache.load_cached(name)
    if cached is not None:
        return cached
    return _build_aoi_cube(name, lat, lon, half_m, cfg)
