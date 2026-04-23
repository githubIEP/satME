"""Date, season, and quality filtering helpers.

Server-side pre-filtering (the efficient path)
-----------------------------------------------
``prefilter_by_aoi_cloud`` maps an AOI quality computation over an entire
ImageCollection server-side, sets the result as a per-image property, then
filters the collection — all in one GEE operation.

The quality function is pluggable: each source provides its own via
``source.aoi_quality_fn()``.  The default (``scl_quality_fn``) uses the
SCL band for Sentinel-2.  SAR sources return a constant 0 (no cloud).
VIIRS uses cf_cvg (cloud-free coverage count).

Batch metadata fetch
--------------------
``batch_image_metadata`` accepts a ``source`` parameter and uses
``source.gee_metadata_properties()`` + ``source.parse_metadata_row()``
so the fetched properties and parsing logic are fully source-specific.

Round-trip comparison for N=300 images, 3 indices
--------------------------------------------------
Before:   300 (cloud %) + 300 (metadata) + 300 (stats) = 900 round-trips
After :   1  (map+filter) + 1 (batch metadata) + ~135 (stats, clean only)
"""

import logging
from datetime import date

import ee

logger = logging.getLogger(__name__)

# SCL classes considered invalid for Sentinel-2 L2A
_S2_SCL_INVALID = [0, 1, 3, 8, 9, 10]


# ─────────────────────────────────────────────────────────────────────────────
# Pluggable quality functions
# ─────────────────────────────────────────────────────────────────────────────

def scl_quality_fn(image: "ee.Image", aoi: "ee.Geometry", scale: int) -> "ee.Number":
    """SCL-based AOI cloud percentage — default for optical (Sentinel-2) sources.

    Computes the fraction of AOI pixels classified as cloud / shadow / invalid
    by the Scene Classification Layer and converts to a 0–100 score.

    A score of 0 means all pixels are valid; 100 means no valid pixels.
    This is the same arithmetic as the old compute_aoi_cloud_pct() in
    cloud_mask.py, now expressed as a server-side-mappable function.
    """
    scl = image.select("SCL")

    # Binary valid-pixel mask: 1 = valid, 0 = cloud/shadow/invalid
    valid = scl.neq(_S2_SCL_INVALID[0])
    for val in _S2_SCL_INVALID[1:]:
        valid = valid.And(scl.neq(val))

    stats = valid.rename("valid").reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=aoi,
        scale=scale,
        maxPixels=1e9,
        bestEffort=True,
    )
    valid_fraction = stats.get("valid")

    # If reduceRegion returns null (no pixels in AOI) → treat as 100% bad
    return ee.Algorithms.If(
        ee.Algorithms.IsEqual(valid_fraction, None),
        ee.Number(100),
        ee.Number(1).subtract(ee.Number(valid_fraction)).multiply(100),
    )


def sar_quality_fn(image: "ee.Image", aoi: "ee.Geometry", scale: int) -> "ee.Number":
    """SAR quality function — always returns 0 (radar is unaffected by cloud).

    All Sentinel-1 images pass the AOI cloud filter.  aoi_covered is still
    computed separately so partial footprints can be flagged.
    """
    return ee.Number(0)


def viirs_quality_fn(min_cf_cvg: int = 1):
    """Factory returning a VIIRS quality function for the given min_cf_cvg threshold.

    Quality score = fraction of AOI pixels with cf_cvg < min_cf_cvg, × 100.
    A score of 0 means every pixel has at least min_cf_cvg cloud-free nights.
    """
    def _fn(image: "ee.Image", aoi: "ee.Geometry", scale: int) -> "ee.Number":
        cf = image.select("cf_cvg")
        valid = cf.gte(min_cf_cvg)   # 1 = sufficient coverage, 0 = not
        stats = valid.rename("valid").reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=aoi,
            scale=scale,
            maxPixels=1e9,
            bestEffort=True,
        )
        valid_fraction = stats.get("valid")
        return ee.Algorithms.If(
            ee.Algorithms.IsEqual(valid_fraction, None),
            ee.Number(100),
            ee.Number(1).subtract(ee.Number(valid_fraction)).multiply(100),
        )
    return _fn


# ─────────────────────────────────────────────────────────────────────────────
# Server-side AOI quality pre-filter
# ─────────────────────────────────────────────────────────────────────────────

def prefilter_by_aoi_cloud(
    collection: "ee.ImageCollection",
    aoi: "ee.Geometry",
    max_aoi_cloud_pct: float,
    quality_fn=None,
    scale: int = 20,
) -> tuple:
    """Compute AOI quality score for every image server-side and filter.

    Parameters
    ----------
    collection:
        ee.ImageCollection already filtered by date range and tile-level
        metadata (e.g. CLOUDY_PIXEL_PERCENTAGE for Sentinel-2).
    aoi:
        AOI geometry.
    max_aoi_cloud_pct:
        Images with a quality score above this threshold are excluded from
        the clean collection but kept in the full collection for the flag report.
    quality_fn:
        Callable with signature (image, aoi, scale) -> ee.Number (0–100).
        Defaults to ``scl_quality_fn`` (SCL-based, Sentinel-2).
        Pass ``sar_quality_fn`` for Sentinel-1 or ``viirs_quality_fn(n)``
        for VIIRS.  Or use ``source.aoi_quality_fn()`` in the pipeline.
    scale:
        Pixel scale in metres for the reduceRegion call inside quality_fn.

    Returns
    -------
    (clean_collection, full_collection)
        Both are ee.ImageCollections with ``aoi_cloud_pct`` and
        ``aoi_covered`` set as properties on every image.
    """
    if quality_fn is None:
        quality_fn = scl_quality_fn

    def _add_quality(image):
        cloud_pct = quality_fn(image, aoi, scale)
        aoi_covered = image.geometry().contains(aoi, maxError=10)
        return (
            image
            .set("aoi_cloud_pct", cloud_pct)
            .set("aoi_covered", aoi_covered)
        )

    full_col = collection.map(_add_quality)
    clean_col = full_col.filter(ee.Filter.lte("aoi_cloud_pct", max_aoi_cloud_pct))

    logger.debug(
        "prefilter_by_aoi_cloud: max=%.1f scale=%dm quality_fn=%s",
        max_aoi_cloud_pct, scale, getattr(quality_fn, "__name__", "custom"),
    )
    return clean_col, full_col


# ─────────────────────────────────────────────────────────────────────────────
# Batch metadata fetch (one round-trip for all images)
# ─────────────────────────────────────────────────────────────────────────────

def batch_image_metadata(
    collection: "ee.ImageCollection",
    source=None,
) -> list[dict]:
    """Fetch metadata for every image in one .getInfo() call.

    Uses ee.Dictionary + aggregate_array to pull multiple property arrays
    simultaneously.  The source controls which properties are fetched
    and how each row is parsed.

    Parameters
    ----------
    collection:
        ee.ImageCollection with ``aoi_cloud_pct`` and ``aoi_covered`` already
        set as properties (i.e. output of prefilter_by_aoi_cloud).
    source:
        A SatelliteSource instance.  Used to call:
          - source.gee_metadata_properties() → extra property names to fetch
          - source.parse_metadata_row(raw, i) → one dict per image
        If None, only the four base properties are fetched and a minimal
        dict is returned (useful for testing).

    Returns
    -------
    list[dict]
        One dict per image, in collection order.  Always contains:
            image_id, date, tile_cloud_pct, aoi_cloud_pct, aoi_covered
        Plus any source-specific fields added by parse_metadata_row.
    """
    # Always fetch these four — set by prefilter_by_aoi_cloud
    base_props = [
        "system:index",
        "system:time_start",
        "aoi_cloud_pct",
        "aoi_covered",
    ]
    extra_props = source.gee_metadata_properties() if source is not None else []
    all_props = base_props + extra_props

    arrays_dict = ee.Dictionary(
        {prop: collection.aggregate_array(prop) for prop in all_props}
    )
    raw = arrays_dict.getInfo()

    n = len(raw.get("system:index", []))
    if n == 0:
        return []

    rows = []
    for i in range(n):
        if source is not None:
            row = source.parse_metadata_row(raw, i)
        else:
            # Minimal fallback (no source provided)
            ts_ms = raw["system:time_start"][i]
            img_date = (
                date.fromtimestamp(ts_ms / 1000).isoformat()
                if ts_ms is not None else None
            )
            aoi_cloud = raw["aoi_cloud_pct"][i]
            row = {
                "image_id":       raw["system:index"][i],
                "date":           img_date,
                "tile_cloud_pct": None,
                "aoi_cloud_pct":  round(float(aoi_cloud), 2) if aoi_cloud is not None else None,
                "aoi_covered":    bool(raw["aoi_covered"][i]) if raw["aoi_covered"][i] is not None else False,
            }
        rows.append(row)

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Simple collection-level helpers
# ─────────────────────────────────────────────────────────────────────────────

def filter_by_date(
    collection: "ee.ImageCollection", start: str, end: str
) -> "ee.ImageCollection":
    """Filter a collection to the given ISO date range."""
    return collection.filterDate(start, end)


def filter_by_season(
    collection: "ee.ImageCollection",
    target_months: list[int],
) -> "ee.ImageCollection":
    """Hard server-side season filter (use only when flag_only is False)."""
    if not target_months:
        return collection
    month_filter = ee.Filter.calendarRange(target_months[0], target_months[0], "month")
    for m in target_months[1:]:
        month_filter = ee.Filter.Or(
            month_filter, ee.Filter.calendarRange(m, m, "month")
        )
    return collection.filter(month_filter)


def filter_by_tile_cloud(
    collection: "ee.ImageCollection",
    max_cloud_pct: float,
    property_name: str = "CLOUDY_PIXEL_PERCENTAGE",
) -> "ee.ImageCollection":
    """Filter out images where tile-level cloud exceeds the threshold."""
    return collection.filter(ee.Filter.lte(property_name, max_cloud_pct))


def is_in_season(image_date: date, target_months: list[int]) -> bool:
    """Return True if image_date.month is in target_months."""
    if not target_months:
        return True
    return image_date.month in target_months


def collection_size(collection: "ee.ImageCollection") -> int:
    """Return the number of images in a collection (one .getInfo() call)."""
    return int(collection.size().getInfo())
