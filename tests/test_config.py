"""Unit tests — config loading and well-formedness.

No GEE connection required.
"""

import yaml
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
EXAMPLE_CFG  = PROJECT_ROOT / "config" / "makaveti_example.yaml"

REQUIRED_TOP_LEVEL_KEYS = ["run", "aoi", "date_range", "sources", "output", "stats"]
REQUIRED_RUN_KEYS       = ["name", "reference_date"]
REQUIRED_AOI_KEYS       = ["mode"]
REQUIRED_DATE_KEYS      = ["start", "end"]


class TestConfigLoading:

    def test_example_yaml_exists(self):
        assert EXAMPLE_CFG.exists(), f"Example config not found at {EXAMPLE_CFG}"

    def test_example_yaml_is_valid_yaml(self):
        with open(EXAMPLE_CFG) as f:
            data = yaml.safe_load(f)
        assert isinstance(data, dict)

    def test_top_level_keys_present(self, cfg):
        for key in REQUIRED_TOP_LEVEL_KEYS:
            assert key in cfg, f"Missing top-level key: '{key}'"

    def test_run_block(self, cfg):
        run = cfg["run"]
        for key in REQUIRED_RUN_KEYS:
            assert key in run, f"Missing run.{key}"

    def test_run_name_is_string(self, cfg):
        assert isinstance(cfg["run"]["name"], str)
        assert len(cfg["run"]["name"]) > 0

    def test_reference_date_is_valid_iso(self, cfg):
        from datetime import date
        d = date.fromisoformat(cfg["run"]["reference_date"])
        assert d.year >= 2000

    def test_aoi_block(self, cfg):
        aoi = cfg["aoi"]
        assert "mode" in aoi
        assert aoi["mode"] in ("point_radius", "polygon"), \
            f"Unknown AOI mode: {aoi['mode']}"

    def test_point_radius_aoi_has_center(self, cfg):
        aoi = cfg["aoi"]
        if aoi["mode"] == "point_radius":
            assert "center" in aoi
            assert "lat" in aoi["center"]
            assert "lon" in aoi["center"]
            assert "radius_m" in aoi
            assert aoi["radius_m"] > 0

    def test_date_range_keys(self, cfg):
        dr = cfg["date_range"]
        assert "start" in dr
        assert "end" in dr

    def test_date_range_start_before_end(self, cfg):
        from datetime import date
        start = date.fromisoformat(cfg["date_range"]["start"])
        end   = date.fromisoformat(cfg["date_range"]["end"])
        assert start < end, "date_range.start must be before date_range.end"

    def test_at_least_one_source_present(self, cfg):
        assert "sources" in cfg
        assert len(cfg["sources"]) > 0

    def test_output_has_base_dir(self, cfg):
        assert "base_dir" in cfg["output"]

    def test_stats_has_percentiles(self, cfg):
        assert "percentiles" in cfg["stats"]
        assert isinstance(cfg["stats"]["percentiles"], list)


class TestSourceConfigs:

    def test_sentinel2_block_structure(self, cfg):
        s2 = cfg["sources"].get("sentinel2", {})
        if not s2.get("enabled", False):
            pytest.skip("Sentinel-2 not enabled in example config")
        assert "indices" in s2, "sentinel2 config must have 'indices' list"
        assert isinstance(s2["indices"], list)
        assert len(s2["indices"]) > 0

    def test_sentinel2_cloud_thresholds_in_range(self, cfg):
        s2 = cfg["sources"].get("sentinel2", {})
        if not s2.get("enabled", False):
            pytest.skip("Sentinel-2 not enabled")
        tile = s2.get("max_tile_cloud_pct", 100)
        aoi  = s2.get("max_aoi_cloud_pct", 100)
        assert 0 <= tile <= 100, f"max_tile_cloud_pct out of range: {tile}"
        assert 0 <= aoi  <= 100, f"max_aoi_cloud_pct out of range: {aoi}"

    def test_sentinel1_block_orbit_direction(self, cfg):
        s1 = cfg["sources"].get("sentinel1", {})
        direction = s1.get("orbit_direction", "ASCENDING")
        assert direction in ("ASCENDING", "DESCENDING"), \
            f"Invalid orbit_direction: {direction}"

    def test_chirps_accumulation_days_positive(self, cfg):
        chirps = cfg["sources"].get("chirps", {})
        if not chirps.get("enabled", False):
            pytest.skip("CHIRPS not enabled")
        days = chirps.get("accumulation_days", 30)
        assert days > 0, f"accumulation_days must be positive, got {days}"

    def test_season_months_in_range(self, cfg):
        months = cfg.get("season", {}).get("target_months", [])
        for m in months:
            assert 1 <= m <= 12, f"Invalid month: {m}"
