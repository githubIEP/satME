"""Statistics extraction from GEE — per-image and batch (server-side map).

Preferred path: batch (server-side map)
----------------------------------------
``map_stats_over_collection`` maps index computation + reduceRegion over an
entire ImageCollection server-side.  ``fetch_stats_batch`` then pulls all
results in ONE .getInfo() call via aggregate_array.

This replaces the previous per-image pattern (N round-trips → 2 round-trips).

Fallback: per-image (kept for debugging single images)
-------------------------------------------------------
``extract_image_stats`` performs one reduceRegion per image.  Still useful
when inspecting a specific image interactively.

Master CSV row structure (one row per date, wide format)
---------------------------------------------------------
date | source | image_id | pre_post | aoi_cloud_pct | tile_cloud_pct |
{INDEX}_mean | {INDEX}_std | {INDEX}_min | {INDEX}_max |
{INDEX}_p{N} (for each configured percentile) |
chirps_{N}d_mm | flags | mgrs_tile | orbit_number | processing_baseline
"""

import logging
from typing import Any

import ee

logger = logging.getLogger(__name__)


def _build_combined_reducer(percentiles: list[int]) -> "ee.Reducer":
    """Build a combined reducer for mean, std, min, max, and percentiles."""
    reducers = (
        ee.Reducer.mean()
        .combine(ee.Reducer.stdDev(), sharedInputs=True)
        .combine(ee.Reducer.min(), sharedInputs=True)
        .combine(ee.Reducer.max(), sharedInputs=True)
    )
    if percentiles:
        reducers = reducers.combine(
            ee.Reducer.percentile(percentiles), sharedInputs=True
        )
    return reducers


def _parse_reducer_output(
    raw: dict,
    band_name: str,
    percentiles: list[int],
    stats_cfg: dict,
) -> dict:
    """Extract per-band stats from a raw reduceRegion output dict.

    GEE names combined reducer outputs as ``{band}_{stat}``, e.g.
    ``NDVI_mean``, ``NDVI_p50``.

    Parameters
    ----------
    raw:
        The dict returned by reduceRegion(...).getInfo().
    band_name:
        The index band name (e.g. "NDVI").
    percentiles:
        List of percentile integers (e.g. [10, 25, 50, 75, 90]).
    stats_cfg:
        The ``stats`` block from the config.

    Returns
    -------
    dict
        Flat dict of ``{INDEX}_{stat}: value`` pairs.
    """
    out = {}
    prefix = band_name

    if stats_cfg.get("include_mean", True):
        out[f"{prefix}_mean"] = _round(raw.get(f"{prefix}_mean"))

    if stats_cfg.get("include_stddev", True):
        out[f"{prefix}_std"] = _round(raw.get(f"{prefix}_stdDev"))

    if stats_cfg.get("include_min_max", True):
        out[f"{prefix}_min"] = _round(raw.get(f"{prefix}_min"))
        out[f"{prefix}_max"] = _round(raw.get(f"{prefix}_max"))

    for p in percentiles:
        out[f"{prefix}_p{p}"] = _round(raw.get(f"{prefix}_p{p}"))

    return out


def _round(value: Any, ndigits: int = 6) -> Any:
    """Round a float, pass through None."""
    if value is None:
        return None
    try:
        return round(float(value), ndigits)
    except (TypeError, ValueError):
        return value


def extract_image_stats(
    masked_image: "ee.Image",
    index_images: dict,
    aoi: "ee.Geometry",
    stats_cfg: dict,
    scale: int = 10,
) -> dict:
    """Extract statistics for all indices from a single image.

    Combines all index bands into one image and calls reduceRegion once,
    minimising GEE round-trips.

    Parameters
    ----------
    masked_image:
        Cloud-masked ee.Image (used for pixel-count reference).
    index_images:
        Dict of {index_name: ee.Image} — one single-band image per index.
    aoi:
        AOI geometry.
    stats_cfg:
        The ``stats`` block from the config.
    scale:
        Pixel scale in metres.  Use the coarsest scale among all indices
        (20 m when NDMI is included).

    Returns
    -------
    dict
        Flat dict of all stats for all indices, keyed as ``{INDEX}_{stat}``.
        Returns an empty dict on failure.
    """
    percentiles = stats_cfg.get("percentiles", [10, 25, 50, 75, 90])

    if not index_images:
        return {}

    # Stack all index bands into a single multi-band image
    band_names = list(index_images.keys())
    stacked = ee.Image.cat([img for img in index_images.values()])

    reducer = _build_combined_reducer(percentiles)

    try:
        raw = stacked.reduceRegion(
            reducer=reducer,
            geometry=aoi,
            scale=scale,
            maxPixels=1e9,
            bestEffort=True,
        ).getInfo()
    except Exception as exc:
        logger.warning("reduceRegion failed: %s", exc)
        return {}

    result = {}
    for band in band_names:
        result.update(_parse_reducer_output(raw, band, percentiles, stats_cfg))

    return result


def build_csv_row(
    image_meta: dict,
    flag_meta: dict,
    index_stats: dict,
    chirps_value: float | None,
    chirps_days: int,
    source_name: str,
    aoi_tile: str | None = None,
) -> dict:
    """Assemble a single master CSV row from all extracted data.

    Parameters
    ----------
    image_meta:
        Dict from source.image_metadata() — includes date, image_id, etc.
    flag_meta:
        Dict with keys: flags (list), pre_post (str), aoi_cloud_pct, tile_cloud_pct.
    index_stats:
        Flat dict of {INDEX_stat: value} from extract_image_stats().
    chirps_value:
        Mean accumulated rainfall in mm, or None.
    chirps_days:
        Number of accumulation days (for column naming).
    source_name:
        Source name string (e.g. "sentinel2").
    aoi_tile:
        Tile label when surrounding_boxes is enabled (e.g. "center", "N", "NE").
        Omitted from the row when None (single-AOI runs).

    Returns
    -------
    dict
        Ordered dict representing one CSV row.
    """
    from satme.flags import flags_to_string

    row = {
        "date":            image_meta.get("date"),
        "source":          source_name,
    }

    # aoi_tile appears immediately after source, only when multi-tile mode is active
    if aoi_tile is not None:
        row["aoi_tile"] = aoi_tile

    row["image_id"]        = image_meta.get("image_id")
    row["pre_post"]        = flag_meta.get("pre_post")
    row["aoi_cloud_pct"]   = flag_meta.get("aoi_cloud_pct")
    row["tile_cloud_pct"]  = flag_meta.get("tile_cloud_pct")

    # Index statistics (preserves insertion order from index_stats)
    row.update(index_stats)

    # CHIRPS rainfall
    row[f"chirps_{chirps_days}d_mm"] = chirps_value

    # Flags
    row["flags"] = flags_to_string(flag_meta.get("flags", []))

    # Extra metadata
    row["mgrs_tile"]           = image_meta.get("mgrs_tile")
    row["orbit_number"]        = image_meta.get("orbit_number")
    row["processing_baseline"] = image_meta.get("processing_baseline")

    return row


def rows_to_dataframe(rows: list[dict]) -> "pandas.DataFrame":
    """Convert a list of CSV row dicts to a pandas DataFrame, sorted by date.

    When ``aoi_tile`` is present (surrounding_boxes run), sorts by
    (date, aoi_tile) so each date's 9 tiles appear together.
    """
    import pandas as pd
    df = pd.DataFrame(rows)
    sort_cols = [c for c in ["date", "aoi_tile", "source"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Multi-tile batch path — used when aoi.surrounding_boxes is enabled
# ─────────────────────────────────────────────────────────────────────────────

def map_stats_multi_tile(
    collection: "ee.ImageCollection",
    ee_tiles_fc: "ee.FeatureCollection",
    signal_names: list,
    stats_cfg: dict,
    preprocess_fn: callable,
    compute_fn: callable,
    scale: int = 20,
) -> "ee.FeatureCollection":
    """Map signal computation + reduceRegions over a collection and all AOI tiles.

    Uses GEE's ``reduceRegions`` (plural) to compute stats for all tiles in one
    server-side pass per image, then flattens to a single FeatureCollection.

    The key efficiency gain over running ``map_stats_over_collection`` nine
    times is that the signal computation (cloud masking + index arithmetic)
    happens once per image, and the nine spatial reductions share that result
    in a single ``reduceRegions`` call.

    Parameters
    ----------
    collection:
        Clean ee.ImageCollection (already AOI-quality-filtered on centre tile).
    ee_tiles_fc:
        FeatureCollection with one Feature per tile, each carrying an
        ``aoi_tile`` string property.  Output of ``aoi.build_ee_tiles_fc()``.
    signal_names:
        List of signal names to compute (same as for ``map_stats_over_collection``).
    stats_cfg:
        The ``stats`` block from the config.
    preprocess_fn:
        Callable (ee.Image) -> ee.Image — cloud mask / speckle filter.
    compute_fn:
        Callable (ee.Image, signal_name: str) -> ee.Image.
    scale:
        Pixel scale in metres for ``reduceRegions``.

    Returns
    -------
    ee.FeatureCollection
        Flat collection with ``N_images × N_tiles`` features.  Each feature has:

        - ``_image_id``: the image's ``system:index``
        - ``aoi_tile``: tile label (``"center"``, ``"N"``, ``"NE"`` …)
        - ``{INDEX}_{stat}``: all computed statistics
    """
    percentiles = stats_cfg.get("percentiles", [10, 25, 50, 75, 90])
    reducer = _build_combined_reducer(percentiles)

    # Convert to List so we can map a function that returns FeatureCollections
    image_list = collection.toList(collection.size())

    def _process_one(img):
        img = ee.Image(img)
        preprocessed = preprocess_fn(img)
        stacked = ee.Image.cat([
            compute_fn(preprocessed, name) for name in signal_names
        ])
        features = stacked.reduceRegions(
            collection=ee_tiles_fc,
            reducer=reducer,
            scale=scale,
            tileScale=4,          # reduces memory pressure for large images
        )
        image_id = img.get("system:index")
        return features.map(lambda f: f.set("_image_id", image_id))

    # image_list.map returns a List of FeatureCollections; flatten to one FC
    nested = image_list.map(_process_one)
    return ee.FeatureCollection(nested).flatten()


def fetch_stats_multi_tile_batch(
    all_fc: "ee.FeatureCollection",
    index_names: list,
    stats_cfg: dict,
) -> list[dict]:
    """Fetch all multi-tile stats in ONE .getInfo() call.

    Mirrors ``fetch_stats_batch`` but reads from the flat FeatureCollection
    produced by ``map_stats_multi_tile`` instead of an ImageCollection.

    Parameters
    ----------
    all_fc:
        Output of ``map_stats_multi_tile``.
    index_names:
        Same signal name list passed to ``map_stats_multi_tile``.
    stats_cfg:
        The ``stats`` block from the config.

    Returns
    -------
    list[dict]
        One dict per (image, tile) combination.  Keys:

        - ``image_id``: joins to metadata
        - ``aoi_tile``: tile label for grouping in the CSV
        - ``{INDEX}_{stat}``: all stat values (``_std`` not ``_stdDev``)
    """
    percentiles = stats_cfg.get("percentiles", [10, 25, 50, 75, 90])
    gee_keys = _gee_stat_keys(index_names, stats_cfg, percentiles)
    all_keys = ["_image_id", "aoi_tile"] + gee_keys

    raw = ee.Dictionary(
        {k: all_fc.aggregate_array(k) for k in all_keys}
    ).getInfo()

    n = len(raw.get("_image_id", []))
    if n == 0:
        return []

    rows = []
    for i in range(n):
        row = {
            "image_id": raw["_image_id"][i],
            "aoi_tile": raw["aoi_tile"][i],
        }
        for gee_key in gee_keys:
            val = raw[gee_key][i] if raw.get(gee_key) else None
            csv_key = gee_key.replace("_stdDev", "_std")
            row[csv_key] = _round(val)
        rows.append(row)

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Batch (server-side map) path — preferred for production runs
# ─────────────────────────────────────────────────────────────────────────────

def _gee_stat_keys(index_names: list, stats_cfg: dict, percentiles: list) -> list:
    """Return the property key names GEE sets when using the combined reducer.

    GEE names combined reducer outputs as ``{band}_{reducer}``.  stdDev is
    the GEE internal name (we rename to _std when building CSV rows).
    """
    keys = []
    for idx in index_names:
        if stats_cfg.get("include_mean", True):
            keys.append(f"{idx}_mean")
        if stats_cfg.get("include_stddev", True):
            keys.append(f"{idx}_stdDev")   # GEE naming — renamed to _std in output
        if stats_cfg.get("include_min_max", True):
            keys.append(f"{idx}_min")
            keys.append(f"{idx}_max")
        for p in percentiles:
            keys.append(f"{idx}_p{p}")
    return keys


def map_stats_over_collection(
    collection: "ee.ImageCollection",
    signal_names: list,
    aoi: "ee.Geometry",
    stats_cfg: dict,
    preprocess_fn: callable,
    compute_fn: callable,
    scale: int = 20,
) -> "ee.ImageCollection":
    """Map signal computation + reduceRegion over a collection server-side.

    Returns an ImageCollection where each image has all stat values set as
    properties (e.g. ``NDVI_mean``, ``NDVI_p90``).  No data leaves GEE yet —
    this is a lazy operation that executes when ``fetch_stats_batch`` calls
    ``.getInfo()``.

    Parameters
    ----------
    collection:
        Clean ee.ImageCollection (already AOI-quality-filtered).
    signal_names:
        List of signal names to compute.
        Optical: spectral index names (e.g. ["NDVI", "NDWI", "NDMI"]).
        SAR: backscatter ratio names (e.g. ["RVI", "VH_VV"]).
        VIIRS: band names (e.g. ["avg_rad"]).
    aoi:
        AOI geometry.
    stats_cfg:
        The ``stats`` block from the config.
    preprocess_fn:
        Callable (ee.Image) -> ee.Image applied before signal computation.
        For optical: cloud mask (e.g. source.apply_cloud_mask).
        For SAR: speckle filter.
        For VIIRS: cf_cvg quality mask.
    compute_fn:
        Callable (ee.Image, signal_name: str) -> ee.Image.
        Computes one named signal band from the pre-processed image.
        Use source.compute_index — this decouples stats.py from the
        optical-only satme.indices registry.
    scale:
        Pixel scale in metres for reduceRegion.

    Returns
    -------
    ee.ImageCollection
        Same collection with stat properties attached to each image.
    """
    percentiles = stats_cfg.get("percentiles", [10, 25, 50, 75, 90])
    reducer = _build_combined_reducer(percentiles)

    def _process_image(image):
        preprocessed = preprocess_fn(image)
        # Stack all signal bands into one multi-band image (server-side)
        stacked = ee.Image.cat([
            compute_fn(preprocessed, name) for name in signal_names
        ])
        stats = stacked.reduceRegion(
            reducer=reducer,
            geometry=aoi,
            scale=scale,
            maxPixels=1e9,
            bestEffort=True,
        )
        return image.set(stats)

    return collection.map(_process_image)


def fetch_stats_batch(
    collection_with_stats: "ee.ImageCollection",
    index_names: list,
    stats_cfg: dict,
) -> list[dict]:
    """Fetch all stats for all images in ONE .getInfo() call.

    Uses ee.Dictionary + aggregate_array — one entry per stat column,
    each entry is an array of values in collection order.

    Parameters
    ----------
    collection_with_stats:
        Output of ``map_stats_over_collection`` — images with stat properties.
    index_names:
        Same list passed to ``map_stats_over_collection``.
    stats_cfg:
        The ``stats`` block from the config.

    Returns
    -------
    list[dict]
        One dict per image (in collection order).  Keys use ``_std`` (not
        ``_stdDev``) to match the CSV column names.  Keyed by ``image_id``
        for joining with metadata.
    """
    percentiles = stats_cfg.get("percentiles", [10, 25, 50, 75, 90])
    gee_keys = _gee_stat_keys(index_names, stats_cfg, percentiles)

    # Fetch image IDs alongside stats so we can join to metadata
    all_keys = ["system:index"] + gee_keys

    raw = ee.Dictionary(
        {key: collection_with_stats.aggregate_array(key) for key in all_keys}
    ).getInfo()

    image_ids = raw.get("system:index", [])
    n = len(image_ids)
    if n == 0:
        return []

    rows = []
    for i in range(n):
        row = {"image_id": image_ids[i]}
        for gee_key in gee_keys:
            val = raw[gee_key][i] if raw.get(gee_key) else None
            # Rename GEE's stdDev → std for CSV consistency
            csv_key = gee_key.replace("_stdDev", "_std")
            row[csv_key] = _round(val)
        rows.append(row)

    return rows
