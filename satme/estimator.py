"""Pre-flight cost and quota estimator.

Before any pixel computation or downloads begin, the estimator:
  1. Queries GEE metadata only (no pixels) to count images per source
  2. Calculates AOI area from config
  3. Estimates GEE export tasks vs daily quota
  4. Estimates download size on disk
  5. Estimates wall-clock processing time
  6. Shows a Planet cost placeholder (requires license)
  7. Prints a formatted summary table
  8. Prompts the user: "Proceed? [y/N]"

GEE quota context
-----------------
  Free tier:
    - Export tasks:         3,000 / day
    - Concurrent tasks:     2,000
    - No per-pixel charge (GEE is free for non-commercial research)
  The main constraint for this pipeline is NUMBER OF EXPORT TASKS
  (one per image per index + one per CHIRPS raster if enabled).

Download size estimate
----------------------
  GeoTIFF size ≈ pixels × bands × bytes_per_pixel
  pixels = (AOI_width_m / scale_m) × (AOI_height_m / scale_m)
  bytes_per_pixel = 4 (Float32)
  Compression factor ≈ 0.5 (LZW or DEFLATE)

Time estimate
-------------
  Very rough: ~5–15 seconds per image for stats extraction (GEE round-trip)
  + ~2–10 seconds per GeoTIFF download
  Total wall time ≈ image_count × 15s + geotiff_count × 5s
"""

import math
import logging
from datetime import date

import ee

logger = logging.getLogger(__name__)

# GEE free-tier limits
GEE_DAILY_TASK_LIMIT = 3_000
GEE_CONCURRENT_TASK_LIMIT = 2_000

# Sentinel-2 revisit: 5 days at equator.  For a single point, realistic
# revisit with overlap is ~2.5 days average.  Use 5-day conservative estimate.
S2_REVISIT_DAYS = 5

# Average cloud rejection rate for tropical/subtropical AOIs (rough)
DEFAULT_CLOUD_REJECTION_RATE = 0.55

# Seconds per image for stats extraction (GEE round-trip, conservative)
STATS_SECONDS_PER_IMAGE = 15

# Seconds per GeoTIFF download (getDownloadUrl, small AOI)
GEOTIFF_SECONDS_PER_FILE = 5

# Float32 bytes
BYTES_PER_PIXEL = 4

# GeoTIFF compression factor
COMPRESSION_FACTOR = 0.5


def estimate(cfg: dict, aoi_meta: dict) -> dict:
    """Build a pre-flight estimate without making any pixel requests to GEE.

    Parameters
    ----------
    cfg:
        Full parsed config dict.
    aoi_meta:
        AOI metadata dict from aoi.build() — contains area_km2, radius_m etc.

    Returns
    -------
    dict
        Estimate results — also printed to stdout.
    """
    results = {}

    date_range = cfg["date_range"]
    start = date.fromisoformat(date_range["start"])
    end   = date.fromisoformat(date_range["end"])
    total_days = (end - start).days

    aoi_area_km2 = aoi_meta["area_km2"]
    radius_m = aoi_meta.get("radius_m", math.sqrt(aoi_area_km2 * 1e6) / 2)
    side_m = radius_m * 2  # bounding square side

    target_months = cfg.get("season", {}).get("target_months", [])
    season_fraction = len(target_months) / 12 if target_months else 1.0

    sources_cfg = cfg["sources"]
    stats_cfg = cfg.get("stats", {})
    output_cfg = cfg.get("output", {})

    results["date_range_days"] = total_days
    results["aoi_area_km2"] = aoi_area_km2

    # ------------------------------------------------------------------ #
    # Sentinel-2 estimate                                                  #
    # ------------------------------------------------------------------ #
    s2_results = {}
    if sources_cfg.get("sentinel2", {}).get("enabled", False):
        s2_cfg = sources_cfg["sentinel2"]
        indices = s2_cfg.get("indices", ["NDVI"])
        n_indices = len(indices)

        # Raw image count estimate (before cloud filter)
        raw_image_count = total_days // S2_REVISIT_DAYS

        # After cloud filter (tile-level + AOI-level)
        cloud_fraction = DEFAULT_CLOUD_REJECTION_RATE
        usable_images = int(raw_image_count * (1 - cloud_fraction))

        # Season fraction (flagged but not excluded if flag_only)
        flag_only = cfg.get("season", {}).get("flag_only", True)
        if not flag_only and target_months:
            usable_images = int(usable_images * season_fraction)

        # Export tasks
        n_geotiff_tasks = (
            usable_images * n_indices
            if s2_cfg.get("export_geotiff", True)
            else 0
        )

        # Download size (10 m resolution for most indices)
        pixels_per_image = (side_m / 10) ** 2
        geotiff_bytes = (
            pixels_per_image * BYTES_PER_PIXEL * COMPRESSION_FACTOR * n_geotiff_tasks
        )
        geotiff_mb = geotiff_bytes / (1024 ** 2)

        # Time estimate
        stats_time_s = usable_images * STATS_SECONDS_PER_IMAGE
        dl_time_s = n_geotiff_tasks * GEOTIFF_SECONDS_PER_FILE
        total_time_s = stats_time_s + dl_time_s

        s2_results = {
            "raw_image_estimate":    raw_image_count,
            "usable_image_estimate": usable_images,
            "n_indices":             n_indices,
            "export_tasks":          n_geotiff_tasks,
            "download_size_mb":      round(geotiff_mb, 1),
            "estimated_time_min":    round(total_time_s / 60, 1),
        }

    results["sentinel2"] = s2_results

    # ------------------------------------------------------------------ #
    # CHIRPS estimate                                                      #
    # ------------------------------------------------------------------ #
    chirps_results = {}
    if sources_cfg.get("chirps", {}).get("enabled", False):
        chirps_cfg = sources_cfg["chirps"]
        # One scalar fetch per image date (very cheap — no export task)
        usable = s2_results.get("usable_image_estimate", 0)
        chirps_tasks = usable if chirps_cfg.get("export_geotiff", False) else 0
        chirps_results = {
            "scalar_fetches":   usable,
            "export_tasks":     chirps_tasks,
            "download_size_mb": round(chirps_tasks * 0.01, 2),  # CHIRPS rasters tiny
        }

    results["chirps"] = chirps_results

    # ------------------------------------------------------------------ #
    # Total GEE task count vs quota                                        #
    # ------------------------------------------------------------------ #
    total_tasks = (
        s2_results.get("export_tasks", 0)
        + chirps_results.get("export_tasks", 0)
    )
    results["total_export_tasks"] = total_tasks
    results["within_daily_quota"] = total_tasks <= GEE_DAILY_TASK_LIMIT
    results["batches_needed"] = math.ceil(total_tasks / GEE_DAILY_TASK_LIMIT)

    # ------------------------------------------------------------------ #
    # Planet placeholder                                                   #
    # ------------------------------------------------------------------ #
    results["planet"] = {
        "note": (
            "Planet cost estimation requires an API key and the planet SDK. "
            "Typical PlanetScope resolution: 3 m.  "
            "Contact Planet for current pricing.  Education licenses may be free."
        )
    }

    return results


def _count_images_gee(cfg: dict, aoi: "ee.Geometry") -> dict:
    """Query GEE metadata (no pixels) to get exact image counts.

    This is called after GEE is initialised, complementing the offline
    estimates from estimate() with server-verified counts.

    Returns
    -------
    dict
        {source_name: {"total": N, "after_tile_cloud_filter": N}}
    """
    counts = {}
    date_range = cfg["date_range"]
    sources_cfg = cfg["sources"]

    if sources_cfg.get("sentinel2", {}).get("enabled", False):
        s2_cfg = sources_cfg["sentinel2"]
        max_cloud = s2_cfg.get("max_tile_cloud_pct", 100)

        total_col = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(aoi)
            .filterDate(date_range["start"], date_range["end"])
        )
        filtered_col = total_col.filter(
            ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", max_cloud)
        )
        counts["sentinel2"] = {
            "total":                    int(total_col.size().getInfo()),
            "after_tile_cloud_filter":  int(filtered_col.size().getInfo()),
        }

    if sources_cfg.get("chirps", {}).get("enabled", False):
        chirps_col = (
            ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
            .filterBounds(aoi)
            .filterDate(date_range["start"], date_range["end"])
        )
        counts["chirps"] = {
            "total": int(chirps_col.size().getInfo()),
        }

    return counts


def print_estimate(estimate_results: dict, gee_counts: dict | None = None) -> None:
    """Print a formatted pre-flight summary table to stdout."""
    r = estimate_results
    s2 = r.get("sentinel2", {})
    chirps = r.get("chirps", {})

    sep = "─" * 60
    print(f"\n{'═' * 60}")
    print("  SatME — Pre-flight estimate")
    print(f"{'═' * 60}")
    print(f"  AOI area         : {r['aoi_area_km2']:.4f} km²")
    print(f"  Date range       : {r['date_range_days']} days")
    print()

    if s2:
        print("  SENTINEL-2")
        if gee_counts and "sentinel2" in gee_counts:
            gc = gee_counts["sentinel2"]
            print(f"  {'Raw images (GEE exact)':<35}: {gc['total']}")
            print(f"  {'After tile cloud filter (GEE exact)':<35}: {gc['after_tile_cloud_filter']}")
        else:
            print(f"  {'Raw images (estimated)':<35}: ~{s2.get('raw_image_estimate', '?')}")
            print(f"  {'Usable after cloud filter (est.)':<35}: ~{s2.get('usable_image_estimate', '?')}")
        print(f"  {'Indices':<35}: {s2.get('n_indices', 0)}")
        print(f"  {'GeoTIFF export tasks':<35}: {s2.get('export_tasks', 0)}")
        print(f"  {'Estimated download size':<35}: ~{s2.get('download_size_mb', 0):.1f} MB")
        print(f"  {'Estimated processing time':<35}: ~{s2.get('estimated_time_min', 0):.0f} min")
        print()

    if chirps:
        print("  CHIRPS RAINFALL")
        print(f"  {'Scalar fetches (1 per image date)':<35}: {chirps.get('scalar_fetches', 0)}")
        print(f"  {'GeoTIFF export tasks':<35}: {chirps.get('export_tasks', 0)}")
        print()

    print(sep)
    total_tasks = r.get("total_export_tasks", 0)
    within_quota = r.get("within_daily_quota", True)
    batches = r.get("batches_needed", 1)
    quota_str = "OK" if within_quota else f"EXCEEDS DAILY LIMIT — {batches} batch(es) needed"
    print(f"  {'Total GEE export tasks':<35}: {total_tasks}")
    print(f"  {'GEE daily quota ({GEE_DAILY_TASK_LIMIT:,})':<35}: {quota_str}")
    print()
    print("  PLANET (future)")
    print(f"  {r['planet']['note']}")
    print(f"{'═' * 60}\n")


def confirm_proceed() -> bool:
    """Ask the user to confirm before running.  Returns True if confirmed."""
    try:
        answer = input("  Proceed with this run? [y/N]: ").strip().lower()
        return answer in ("y", "yes")
    except (KeyboardInterrupt, EOFError):
        print()
        return False
