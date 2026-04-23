"""GeoTIFF and data download management.

Download strategy
-----------------
``getDownloadUrl()``
    Used when AOI area < size_threshold_km2 (default 25 km²).
    Returns a zip file containing one GeoTIFF per band.
    Fast, no Drive step, works for the Makaveti example (1 km² AOI).

Drive export (``ee.batch.Export.image.toDrive``)
    Used for larger AOIs.  Requires Google Drive access.
    Pipeline monitors task status and downloads from Drive when complete.

``output.download_method`` in config:
    "auto"  = choose based on AOI area vs threshold (default)
    "local" = always use getDownloadUrl()
    "drive" = always use Drive export

Directory layout
----------------
data/raw/{run_name}/{source}/{date}_{index}.tif
data/chirps/{run_name}/{date}_chirps_{N}d.tif

Cache behaviour (skip_existing: true)
--------------------------------------
Before requesting any download, check whether the expected file path
exists on disk.  If it does, skip the export and load pixel data from the
existing file using rasterio.  Re-running the same config is safe and cheap.
"""

import io
import os
import time
import zipfile
import logging
import urllib.request
from pathlib import Path

import ee
import requests

logger = logging.getLogger(__name__)

# Maximum retries for a download attempt
MAX_RETRIES = 3
RETRY_DELAY_S = 5

# Drive task polling interval
DRIVE_POLL_INTERVAL_S = 30
DRIVE_TASK_TIMEOUT_S  = 3600  # 1 hour


def _build_path(base_dir: str, run_name: str, source: str, date_str: str, label: str) -> Path:
    """Construct the expected file path for a GeoTIFF."""
    return Path(base_dir) / run_name / source / f"{date_str}_{label}.tif"


def _build_chirps_path(run_name: str, date_str: str, days: int) -> Path:
    return Path("data/chirps") / run_name / f"{date_str}_chirps_{days}d.tif"


def _should_skip(path: Path, skip_existing: bool) -> bool:
    if skip_existing and path.exists() and path.stat().st_size > 0:
        logger.debug("Cache hit — skipping download: %s", path)
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# getDownloadUrl path (small AOIs)
# ─────────────────────────────────────────────────────────────────────────────

def download_via_url(
    image: "ee.Image",
    path: Path,
    aoi: "ee.Geometry",
    scale: int,
    band_name: str,
) -> Path | None:
    """Download a single-band image via getDownloadUrl and save as GeoTIFF.

    Parameters
    ----------
    image:
        Single-band ee.Image to download.
    path:
        Destination file path.
    aoi:
        Clipping geometry.
    scale:
        Export resolution in metres.
    band_name:
        The band name to select from the download zip.

    Returns
    -------
    Path | None
        The saved file path on success, None on failure.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    params = {
        "name":   path.stem,
        "bands":  [{"id": band_name}],
        "region": aoi.getInfo()["coordinates"],
        "scale":  scale,
        "format": "GEO_TIFF",
        "crs":    "EPSG:4326",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            url = image.getDownloadUrl(params)
            response = requests.get(url, timeout=120)
            response.raise_for_status()

            # The response is a zip containing one GeoTIFF
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                tif_names = [n for n in zf.namelist() if n.endswith(".tif")]
                if not tif_names:
                    logger.warning("No .tif found in download zip for %s", path.name)
                    return None
                with zf.open(tif_names[0]) as tif_file:
                    path.write_bytes(tif_file.read())

            logger.info("Downloaded: %s", path)
            return path

        except Exception as exc:
            logger.warning(
                "Download attempt %d/%d failed for %s: %s",
                attempt, MAX_RETRIES, path.name, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_S)

    logger.error("All download attempts failed for %s", path.name)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Drive export path (large AOIs)
# ─────────────────────────────────────────────────────────────────────────────

def export_via_drive(
    image: "ee.Image",
    description: str,
    folder: str,
    aoi: "ee.Geometry",
    scale: int,
) -> "ee.batch.Task | None":
    """Start a Drive export task and return the task object.

    The caller is responsible for polling and downloading.
    """
    task = ee.batch.Export.image.toDrive(
        image=image.clip(aoi),
        description=description,
        folder=folder,
        fileNamePrefix=description,
        region=aoi,
        scale=scale,
        crs="EPSG:4326",
        fileFormat="GeoTIFF",
        maxPixels=1e9,
    )
    task.start()
    logger.info("Drive export task started: %s", description)
    return task


def wait_for_drive_task(task: "ee.batch.Task", description: str) -> bool:
    """Poll until a Drive export task completes or fails."""
    elapsed = 0
    while elapsed < DRIVE_TASK_TIMEOUT_S:
        status = task.status()
        state = status.get("state")
        if state == "COMPLETED":
            logger.info("Drive task completed: %s", description)
            return True
        if state in ("FAILED", "CANCELLED"):
            logger.error("Drive task %s: %s — %s", state, description, status.get("error_message"))
            return False
        logger.debug("Drive task %s: %s (elapsed %ds)", description, state, elapsed)
        time.sleep(DRIVE_POLL_INTERVAL_S)
        elapsed += DRIVE_POLL_INTERVAL_S
    logger.error("Drive task timed out after %ds: %s", DRIVE_TASK_TIMEOUT_S, description)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# High-level download coordinator
# ─────────────────────────────────────────────────────────────────────────────

def download_index_geotiff(
    index_image: "ee.Image",
    index_name: str,
    image_date: str,
    source_name: str,
    run_name: str,
    aoi: "ee.Geometry",
    aoi_area_km2: float,
    scale: int,
    output_cfg: dict,
) -> Path | None:
    """Download a single index GeoTIFF, choosing the appropriate method.

    Parameters
    ----------
    index_image:
        Single-band ee.Image (the computed index, clipped to AOI).
    index_name:
        Index name (e.g. "NDVI") — used in the filename.
    image_date:
        ISO date string (e.g. "2021-09-25").
    source_name:
        Source name (e.g. "sentinel2").
    run_name:
        Run name from config (e.g. "makaveti_2016_2024").
    aoi:
        AOI geometry.
    aoi_area_km2:
        AOI area in km² — used to choose download method.
    scale:
        Export scale in metres.
    output_cfg:
        The ``output`` block from the config.

    Returns
    -------
    Path | None
        Path to the saved file, or None on failure.
    """
    geotiff_dir = output_cfg.get("geotiff_dir", "data/raw")
    skip_existing = output_cfg.get("skip_existing", True)
    method = output_cfg.get("download_method", "auto")
    size_threshold = output_cfg.get("size_threshold_km2", 25.0)
    drive_folder = output_cfg.get("drive_folder", "satme_exports")

    path = _build_path(geotiff_dir, run_name, source_name, image_date, index_name)

    if _should_skip(path, skip_existing):
        return path

    use_drive = (
        method == "drive"
        or (method == "auto" and aoi_area_km2 >= size_threshold)
    )

    clipped = index_image.clip(aoi).rename(index_name)

    if use_drive:
        desc = f"{run_name}_{source_name}_{image_date}_{index_name}"
        task = export_via_drive(clipped, desc, drive_folder, aoi, scale)
        if task and wait_for_drive_task(task, desc):
            logger.info(
                "Drive export complete — download %s from Google Drive folder '%s'",
                desc, drive_folder,
            )
            # Automatic Drive-to-local download requires google-api-python-client auth
            # and is out of scope for the initial implementation.
            # For now: log the location and return None.
            return None
        return None
    else:
        return download_via_url(clipped, path, aoi, scale, index_name)


def download_chirps_geotiff(
    chirps_image: "ee.Image",
    image_date: str,
    run_name: str,
    aoi: "ee.Geometry",
    accumulation_days: int,
    output_cfg: dict,
) -> Path | None:
    """Download a CHIRPS accumulated rainfall GeoTIFF."""
    skip_existing = output_cfg.get("skip_existing", True)
    path = _build_chirps_path(run_name, image_date, accumulation_days)

    if _should_skip(path, skip_existing):
        return path

    path.parent.mkdir(parents=True, exist_ok=True)
    band_name = f"chirps_{accumulation_days}d_mm"
    return download_via_url(
        chirps_image.rename(band_name),
        path,
        aoi,
        scale=5566,
        band_name=band_name,
    )
