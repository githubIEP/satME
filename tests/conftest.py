"""Shared pytest fixtures and GEE connection management.

Fixtures
--------
cfg             Full config dict loaded from the example YAML.
minimal_cfg     Smallest valid config (narrow date range for fast GEE tests).
gee_connection  Authenticated GEE session; skips the test if auth fails.
geometry        ee.Geometry for the Makaveti AOI (requires gee_connection).
"""

import pytest
import yaml
from pathlib import Path

# ── Path helpers ──────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent
EXAMPLE_CFG  = PROJECT_ROOT / "config" / "makaveti_example.yaml"


# ── Config fixtures (no GEE required) ────────────────────────────────────────

@pytest.fixture(scope="session")
def cfg():
    """Full config dict loaded from the example YAML."""
    with open(EXAMPLE_CFG) as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def minimal_cfg():
    """Minimal config with a narrow date range for fast GEE tests.

    Uses the same Makaveti AOI and GEE project but limits to 6 months
    so collection queries return quickly.
    """
    return {
        "run": {
            "name": "test_run",
            "reference_date": "2018-10-18",
        },
        "auth": {
            "gee_project": "gdelt-proj-489101",
        },
        "aoi": {
            "mode": "point_radius",
            "center": {"lat": -1.54351, "lon": 37.33258},
            "radius_m": 500,
        },
        "date_range": {
            "start": "2020-01-01",
            "end":   "2020-06-30",
        },
        "season": {
            "target_months": [8, 9],
            "flag_only": True,
        },
        "sources": {
            "sentinel2": {
                "enabled": True,
                "collection": "COPERNICUS/S2_SR_HARMONIZED",
                "max_tile_cloud_pct": 50,
                "max_aoi_cloud_pct": 30,
                "indices": ["NDVI", "NDMI"],
                "export_geotiff": False,
            },
            "sentinel1": {
                "enabled": False,
            },
            "viirs": {
                "enabled": False,
            },
            "chirps": {
                "enabled": True,
                "collection": "UCSB-CHG/CHIRPS/DAILY",
                "accumulation_days": 30,
                "export_geotiff": False,
            },
        },
        "output": {
            "base_dir": "outputs/test_runs",
            "stats_csv": True,
            "flag_report": True,
            "skip_existing": False,
            "download_method": "local",
        },
        "stats": {
            "percentiles": [25, 50, 75],
            "include_mean": True,
            "include_stddev": True,
            "include_min_max": True,
        },
    }


# ── GEE fixtures (skipped when no auth available) ─────────────────────────────

@pytest.fixture(scope="session")
def gee_connection(minimal_cfg):
    """Authenticate with GEE; skip all tests in this fixture's chain if it fails.

    Mark any test that needs GEE with @pytest.mark.gee AND include
    `gee_connection` in the test's argument list.
    """
    pytest.importorskip("ee", reason="earthengine-api not installed")
    try:
        from satme import auth
        auth.initialise(
            project_id=minimal_cfg["auth"]["gee_project"],
        )
        conn = auth.verify_connection()
        return conn
    except Exception as exc:
        pytest.skip(f"GEE authentication failed — skipping GEE tests: {exc}")


@pytest.fixture(scope="session")
def geometry(gee_connection, minimal_cfg):
    """ee.Geometry for the test AOI."""
    from satme import aoi as aoi_module
    geom, _ = aoi_module.build(minimal_cfg)
    return geom


@pytest.fixture(scope="session")
def s2_source(minimal_cfg):
    """Sentinel2Source instance for the test config."""
    from satme.sources.sentinel2 import Sentinel2Source
    return Sentinel2Source(minimal_cfg["sources"]["sentinel2"])


@pytest.fixture(scope="session")
def s1_source():
    """Sentinel1Source instance with default settings."""
    from satme.sources.sentinel1 import Sentinel1Source
    return Sentinel1Source({
        "orbit_direction": "ASCENDING",
        "instrument_mode": "IW",
        "polarizations": ["VV", "VH"],
        "speckle_filter": "lee",
        "indices": ["RVI", "VH_VV"],
    })


@pytest.fixture(scope="session")
def viirs_source():
    """VIIRSSource instance with default settings."""
    from satme.sources.viirs import VIIRSSource
    return VIIRSSource({
        "collection": "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG",
        "min_cf_cvg": 1,
        "indices": ["avg_rad"],
        "export_geotiff": False,
    })
