"""Centralised configuration for the sand dam exploration pipeline."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

# ---------- AOIs ----------
TREATMENT_NAME = "treatment_dam"
TREATMENT_LAT = -1.5435
TREATMENT_LON = 37.3326

CONTROL_NAME = "control"
CONTROL_LAT = -1.543
CONTROL_LON = 37.378

HALF_SIZE_M = 500                                      # 1 km AOI

# ---------- Period and drought ----------
START_YEAR = 2016
END_YEAR = 2025
DAM_CONSTRUCTION_DATE = pd.Timestamp("2018-10-01")
DROUGHT_START = pd.Timestamp("2020-10-01")
DROUGHT_END = pd.Timestamp("2023-06-30")

# ---------- Index thresholds ----------
NDVI_VEG_THRESHOLD = 0.30
MNDWI_WATER_THRESHOLD = 0.0
PIXEL_AREA_HA = 0.01                                   # 10 m x 10 m

# ---------- Data filtering ----------
MAX_SCENE_CLOUD = 80                                   # whole-tile pre-filter
MIN_VALID_FRACTION = 0.85                              # AOI-level cloud filter

# ---------- DOY binning ----------
DOY_BIN_SIZE = 10
N_DOY_BINS = 37
MIN_BINS_REQUIRED = 4

# ---------- Sentinel-2 bands and SCL ----------
BAD_SCL_CLASSES = [0, 1, 3, 8, 9, 10, 11]
BANDS = ["B02", "B03", "B04", "B08", "B11", "SCL"]

# ---------- Distance bands for corridor analysis ----------
RING_BOUNDS_M = [0, 50, 100, 250, 500, 1000]

# ---------- Visualisation defaults ----------
RGB_STRETCH = 0.30
NDVI_VMIN = -0.2
NDVI_VMAX = 0.8
NDVI_DIFF_VMIN = -0.5
NDVI_DIFF_VMAX = 0.5

# ---------- Paths ----------
PACKAGE_DIR = Path(__file__).parent
BACKEND_DIR = PACKAGE_DIR.parent
REPO_ROOT = BACKEND_DIR.parent

CACHE_DIR = BACKEND_DIR / "_cache_fulltimeseries"
KML_PATHS = [
    BACKEND_DIR / "sanddam.kml",            # downstream / at-dam segment
    BACKEND_DIR / "sandam-upstream.kml",    # upstream segment
]
OUTPUTS_DIR = PACKAGE_DIR / "outputs"
PRECIPITATION_CSV = CACHE_DIR / "precipitation_daily.csv"

# Dry-season filter (Athi-Kapiti long dry, Jun–Sep). Set to None to use all months.
DRY_SEASON_MONTHS = [6, 7, 8, 9]


def cache_version() -> str:
    """Stable hash of config inputs that invalidate the per-scene zarr cache."""
    payload = (
        f"{MIN_VALID_FRACTION}|"
        f"{sorted(BAD_SCL_CLASSES)}|"
        f"{sorted(BANDS)}|"
        f"{DOY_BIN_SIZE}|"
        f"{NDVI_VEG_THRESHOLD}|"
        f"{MNDWI_WATER_THRESHOLD}"
    )
    return hashlib.sha1(payload.encode()).hexdigest()[:8]


CACHE_VERSION = cache_version()


def aoi_meta(name: str) -> tuple[str, float, float]:
    """Return (name, lat, lon) for either the treatment or control AOI."""
    if name == TREATMENT_NAME:
        return (TREATMENT_NAME, TREATMENT_LAT, TREATMENT_LON)
    if name == CONTROL_NAME:
        return (CONTROL_NAME, CONTROL_LAT, CONTROL_LON)
    raise ValueError(f"Unknown AOI: {name}")
