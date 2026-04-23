"""Unit tests — flag assignment logic (no GEE required).

Tests every flag condition and edge case in flags.py.
"""

import pytest
from datetime import date
from satme.flags import assign_flags, is_excluded, flags_to_string

# Minimal config for flag tests
_CFG = {
    "run": {"reference_date": "2018-10-18"},
    "season": {"target_months": [8, 9], "flag_only": True},
    "sources": {
        "sentinel2": {
            "enabled": True,
            "max_tile_cloud_pct": 20,
            "max_aoi_cloud_pct": 10,
        }
    },
}


def _flags(image_date_str, tile_cloud=5.0, aoi_cloud=5.0, covered=True, cfg=_CFG):
    """Helper: call assign_flags and return the list."""
    return assign_flags(
        image_date=date.fromisoformat(image_date_str),
        tile_cloud_pct=tile_cloud,
        aoi_cloud_pct=aoi_cloud,
        aoi_fully_covered=covered,
        cfg=cfg,
    )


class TestNoDataFlag:

    def test_no_data_when_aoi_cloud_is_none(self):
        flags = assign_flags(
            image_date=date(2019, 8, 15),
            tile_cloud_pct=5.0,
            aoi_cloud_pct=None,   # triggers NO_DATA
            aoi_fully_covered=True,
            cfg=_CFG,
        )
        assert "NO_DATA" in flags

    def test_no_data_is_only_flag_returned(self):
        """NO_DATA short-circuits — no other flags should be set."""
        flags = assign_flags(
            image_date=date(2019, 8, 15),
            tile_cloud_pct=99.0,
            aoi_cloud_pct=None,
            aoi_fully_covered=False,
            cfg=_CFG,
        )
        assert flags == ["NO_DATA"]


class TestSeasonFlag:

    def test_in_season_no_flag(self):
        flags = _flags("2019-08-15")  # August is in [8, 9]
        assert "OUT_OF_SEASON" not in flags

    def test_out_of_season_flagged(self):
        flags = _flags("2019-03-15")  # March not in [8, 9]
        assert "OUT_OF_SEASON" in flags

    def test_no_season_filter_no_flag(self):
        cfg_no_season = {**_CFG, "season": {"target_months": [], "flag_only": True}}
        flags = _flags("2019-03-15", cfg=cfg_no_season)
        assert "OUT_OF_SEASON" not in flags


class TestCloudFlags:

    def test_high_tile_cloud_flagged(self):
        flags = _flags("2019-08-15", tile_cloud=25.0)  # > 20 threshold
        assert "HIGH_TILE_CLOUD" in flags

    def test_tile_cloud_at_threshold_not_flagged(self):
        flags = _flags("2019-08-15", tile_cloud=20.0)  # exactly at threshold
        assert "HIGH_TILE_CLOUD" not in flags

    def test_high_aoi_cloud_flagged(self):
        flags = _flags("2019-08-15", aoi_cloud=15.0)  # > 10 threshold
        assert "HIGH_AOI_CLOUD" in flags

    def test_aoi_cloud_at_threshold_not_flagged(self):
        flags = _flags("2019-08-15", aoi_cloud=10.0)
        assert "HIGH_AOI_CLOUD" not in flags

    def test_clean_image_has_no_cloud_flags(self):
        flags = _flags("2019-08-15", tile_cloud=5.0, aoi_cloud=3.0)
        assert "HIGH_TILE_CLOUD" not in flags
        assert "HIGH_AOI_CLOUD" not in flags


class TestInterventionFlags:

    def test_pre_intervention(self):
        flags = _flags("2017-06-01")  # well before 2018-10-18
        assert "PRE_INTERVENTION" in flags
        assert "POST_INTERVENTION" not in flags

    def test_post_intervention(self):
        flags = _flags("2020-01-01")  # well after 2018-10-18
        assert "POST_INTERVENTION" in flags
        assert "PRE_INTERVENTION" not in flags

    def test_reference_date_itself_is_post(self):
        flags = _flags("2018-10-18")  # on the reference date
        assert "POST_INTERVENTION" in flags

    def test_near_intervention_pre(self):
        flags = _flags("2018-09-01")  # 47 days before reference
        assert "NEAR_INTERVENTION" in flags
        assert "PRE_INTERVENTION" in flags

    def test_near_intervention_post(self):
        flags = _flags("2018-11-15")  # 28 days after reference
        assert "NEAR_INTERVENTION" in flags
        assert "POST_INTERVENTION" in flags

    def test_not_near_intervention_far_pre(self):
        flags = _flags("2017-01-01")  # > 60 days before
        assert "NEAR_INTERVENTION" not in flags

    def test_not_near_intervention_far_post(self):
        flags = _flags("2020-01-01")  # > 60 days after
        assert "NEAR_INTERVENTION" not in flags


class TestCoverageFlag:

    def test_partial_coverage_flagged(self):
        flags = _flags("2019-08-15", covered=False)
        assert "PARTIAL_AOI_COVERAGE" in flags

    def test_full_coverage_not_flagged(self):
        flags = _flags("2019-08-15", covered=True)
        assert "PARTIAL_AOI_COVERAGE" not in flags


class TestIsExcluded:

    def test_no_data_always_excluded(self):
        assert is_excluded(["NO_DATA"], _CFG) is True

    def test_out_of_season_excluded_when_flag_only_false(self):
        cfg = {**_CFG, "season": {"target_months": [8, 9], "flag_only": False}}
        assert is_excluded(["OUT_OF_SEASON"], cfg) is True

    def test_out_of_season_kept_when_flag_only_true(self):
        cfg = {**_CFG, "season": {"target_months": [8, 9], "flag_only": True}}
        assert is_excluded(["OUT_OF_SEASON"], cfg) is False

    def test_cloudy_image_not_excluded(self):
        assert is_excluded(["HIGH_AOI_CLOUD", "PRE_INTERVENTION"], _CFG) is False

    def test_clean_image_not_excluded(self):
        assert is_excluded(["PRE_INTERVENTION"], _CFG) is False

    def test_empty_flags_not_excluded(self):
        assert is_excluded([], _CFG) is False


class TestFlagsToString:

    def test_empty_flags(self):
        assert flags_to_string([]) == ""

    def test_single_flag(self):
        assert flags_to_string(["PRE_INTERVENTION"]) == "PRE_INTERVENTION"

    def test_multiple_flags_pipe_separated(self):
        result = flags_to_string(["PRE_INTERVENTION", "OUT_OF_SEASON"])
        assert result == "PRE_INTERVENTION|OUT_OF_SEASON"
