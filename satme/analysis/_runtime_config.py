"""Runtime-populated config for the analysis layer.

The vendored modules (cache, indices, aggregate, visualisation) read
constants from this module the same way they read from
``david/exploration/config.py`` in the original study. Phase 4 calls
``configure(cfg, out_dir)`` once at the start of the analysis stage to
populate every name from the satme YAML before any vendored function
runs.

Defaults that are not site-specific are kept here as module-level values.
Site-specific values (AOIs, dam date, drought window, dry season,
channel KMLs) start as None and must be set by configure().
"""

from __future__ import annotations

import hashlib
from datetime import date as _date
from pathlib import Path

import pandas as pd

# ─── Site-specific (populated by configure()) ─────────────────────────────────
TREATMENT_NAME: str = "treatment"
TREATMENT_LAT: float | None = None
TREATMENT_LON: float | None = None

CONTROL_NAME: str = "control"
CONTROL_LAT: float | None = None
CONTROL_LON: float | None = None

HALF_SIZE_M: float | None = None

START_YEAR: int | None = None
END_YEAR: int | None = None

DAM_CONSTRUCTION_DATE: pd.Timestamp | None = None
DROUGHT_START: pd.Timestamp | None = None
DROUGHT_END: pd.Timestamp | None = None

DRY_SEASON_MONTHS: list[int] | None = None

KML_PATHS: list[Path] = []

CACHE_DIR: Path | None = None
OUTPUTS_DIR: Path | None = None
PRECIPITATION_CSV: Path | None = None

# ─── Defaults — overridable via report: in satme YAML ─────────────────────────
NDVI_VEG_THRESHOLD: float = 0.30
MNDWI_WATER_THRESHOLD: float = 0.0
PIXEL_AREA_HA: float = 0.01                       # 10 m × 10 m

MAX_SCENE_CLOUD: int = 80
MIN_VALID_FRACTION: float = 0.85

DOY_BIN_SIZE: int = 10
N_DOY_BINS: int = 37
MIN_BINS_REQUIRED: int = 4

BAD_SCL_CLASSES: list[int] = [0, 1, 3, 8, 9, 10, 11]
BANDS: list[str] = ["B02", "B03", "B04", "B08", "B11", "SCL"]

RING_BOUNDS_M: list[int] = [0, 50, 100, 250, 500, 1000]

# Date split: scenes before this date come from MPC (full archive); on or after
# this date come from GEE. GEE's S2_SR_HARMONIZED collection is incomplete for
# many MGRS tiles before ~2019 — this matches satme/sources/copernicus_s2.py.
GEE_CUTOFF_DATE: str = "2019-01-01"

RGB_STRETCH: float = 0.30
NDVI_VMIN: float = -0.2
NDVI_VMAX: float = 0.8
NDVI_DIFF_VMIN: float = -0.5
NDVI_DIFF_VMAX: float = 0.5

CACHE_VERSION: str = ""  # set by configure()


def cache_version() -> str:
    """Stable hash of inputs that invalidate the per-AOI zarr cache."""
    payload = (
        f"{MIN_VALID_FRACTION}|"
        f"{sorted(BAD_SCL_CLASSES)}|"
        f"{sorted(BANDS)}|"
        f"{DOY_BIN_SIZE}|"
        f"{NDVI_VEG_THRESHOLD}|"
        f"{MNDWI_WATER_THRESHOLD}|"
        f"{GEE_CUTOFF_DATE}"
    )
    return hashlib.sha1(payload.encode()).hexdigest()[:8]


def aoi_meta(name: str) -> tuple[str, float, float]:
    """Return (name, lat, lon) for the named AOI."""
    if name == TREATMENT_NAME:
        return (TREATMENT_NAME, TREATMENT_LAT, TREATMENT_LON)
    if name == CONTROL_NAME:
        return (CONTROL_NAME, CONTROL_LAT, CONTROL_LON)
    raise ValueError(f"Unknown AOI: {name}")


def configure(cfg: dict, out_dir: Path) -> None:
    """Populate the module-level config from the satme YAML and run dir.

    Called once by Phase 4 before any vendored analysis function runs.
    """
    global TREATMENT_NAME, TREATMENT_LAT, TREATMENT_LON
    global CONTROL_NAME, CONTROL_LAT, CONTROL_LON
    global HALF_SIZE_M, START_YEAR, END_YEAR
    global DAM_CONSTRUCTION_DATE, DROUGHT_START, DROUGHT_END
    global DRY_SEASON_MONTHS, KML_PATHS
    global CACHE_DIR, OUTPUTS_DIR, PRECIPITATION_CSV
    global NDVI_VEG_THRESHOLD, MNDWI_WATER_THRESHOLD
    global RING_BOUNDS_M, DOY_BIN_SIZE, MIN_BINS_REQUIRED
    global CACHE_VERSION

    aoi = cfg["aoi"]
    rep = cfg["report"]

    treat = aoi["treatment"]
    ctrl  = aoi["control"]

    TREATMENT_NAME = treat.get("name", "treatment")
    TREATMENT_LAT  = float(treat["center"]["lat"])
    TREATMENT_LON  = float(treat["center"]["lon"])

    CONTROL_NAME = ctrl.get("name", "control")
    CONTROL_LAT  = float(ctrl["center"]["lat"])
    CONTROL_LON  = float(ctrl["center"]["lon"])

    # Both AOIs use the same half-size; treatment.radius_m is the source of truth.
    HALF_SIZE_M = float(treat.get("radius_m", 500))

    dr = cfg["date_range"]
    START_YEAR = _date.fromisoformat(dr["start"]).year
    END_YEAR   = _date.fromisoformat(dr["end"]).year

    DAM_CONSTRUCTION_DATE = pd.Timestamp(rep["dam_date"])
    drought = rep["drought_window"]
    DROUGHT_START = pd.Timestamp(drought["start"])
    DROUGHT_END   = pd.Timestamp(drought["end"])

    DRY_SEASON_MONTHS = rep.get("dry_season_months")

    KML_PATHS = [Path(p) for p in aoi.get("channel_kmls", [])]

    OUTPUTS_DIR = Path(out_dir) / "analysis"
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR = Path(out_dir) / "_analysis_cache"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    PRECIPITATION_CSV = CACHE_DIR / "precipitation_daily.csv"

    # Optional overrides
    if "ndvi_veg_threshold" in rep:
        NDVI_VEG_THRESHOLD = float(rep["ndvi_veg_threshold"])
    if "mndwi_water_threshold" in rep:
        MNDWI_WATER_THRESHOLD = float(rep["mndwi_water_threshold"])
    if "ring_bounds_m" in rep:
        RING_BOUNDS_M = list(rep["ring_bounds_m"])
    if "doy_bin_size" in rep:
        DOY_BIN_SIZE = int(rep["doy_bin_size"])
    if "min_bins_required" in rep:
        MIN_BINS_REQUIRED = int(rep["min_bins_required"])
    if "gee_cutoff_date" in rep:
        global GEE_CUTOFF_DATE
        GEE_CUTOFF_DATE = str(rep["gee_cutoff_date"])

    CACHE_VERSION = cache_version()
