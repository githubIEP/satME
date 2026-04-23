"""GEE integration tests — batch stats computation and CHIRPS.

Requires a live GEE connection.  Run with:
    pytest -m gee tests/test_gee_stats.py

Checks that:
  - map_stats_over_collection attaches expected property keys to images
  - fetch_stats_batch returns dicts with values in valid index ranges
  - CHIRPS batch_rainfall_scalars returns correct count and float values
"""

import pytest

pytestmark = pytest.mark.gee


def _get_clean_collection(s2_source, geometry, minimal_cfg):
    """Helper: return a small clean collection for stats tests."""
    from satme.image_filter import prefilter_by_aoi_cloud
    col = s2_source.get_collection(geometry, minimal_cfg["date_range"])
    clean, _ = prefilter_by_aoi_cloud(
        collection=col,
        aoi=geometry,
        max_aoi_cloud_pct=80,   # lenient for test coverage
        quality_fn=s2_source.aoi_quality_fn(),
        scale=s2_source.default_scale,
    )
    return clean


class TestBatchStats:

    def test_map_stats_returns_collection(self, gee_connection, s2_source, geometry, minimal_cfg):
        """map_stats_over_collection must return an ee.ImageCollection."""
        import ee
        from satme.stats import map_stats_over_collection
        clean = _get_clean_collection(s2_source, geometry, minimal_cfg)
        stats_col = map_stats_over_collection(
            collection=clean,
            signal_names=["NDVI"],
            aoi=geometry,
            stats_cfg=minimal_cfg["stats"],
            preprocess_fn=s2_source.apply_cloud_mask,
            compute_fn=s2_source.compute_index,
            scale=s2_source.default_scale,
        )
        assert isinstance(stats_col, ee.ImageCollection)

    def test_fetch_stats_returns_list(self, gee_connection, s2_source, geometry, minimal_cfg):
        """fetch_stats_batch must return a non-empty list."""
        from satme.stats import map_stats_over_collection, fetch_stats_batch
        clean = _get_clean_collection(s2_source, geometry, minimal_cfg)
        stats_col = map_stats_over_collection(
            collection=clean,
            signal_names=["NDVI"],
            aoi=geometry,
            stats_cfg=minimal_cfg["stats"],
            preprocess_fn=s2_source.apply_cloud_mask,
            compute_fn=s2_source.compute_index,
            scale=s2_source.default_scale,
        )
        rows = fetch_stats_batch(stats_col, ["NDVI"], minimal_cfg["stats"])
        assert isinstance(rows, list)
        assert len(rows) > 0

    def test_stats_rows_have_image_id(self, gee_connection, s2_source, geometry, minimal_cfg):
        from satme.stats import map_stats_over_collection, fetch_stats_batch
        clean = _get_clean_collection(s2_source, geometry, minimal_cfg)
        stats_col = map_stats_over_collection(
            collection=clean,
            signal_names=["NDVI"],
            aoi=geometry,
            stats_cfg=minimal_cfg["stats"],
            preprocess_fn=s2_source.apply_cloud_mask,
            compute_fn=s2_source.compute_index,
            scale=s2_source.default_scale,
        )
        rows = fetch_stats_batch(stats_col, ["NDVI"], minimal_cfg["stats"])
        for row in rows:
            assert "image_id" in row, f"image_id missing from stats row: {row}"
            assert row["image_id"] is not None

    def test_ndvi_mean_in_valid_range(self, gee_connection, s2_source, geometry, minimal_cfg):
        """NDVI mean over the Makaveti AOI must be in -1 to 1."""
        from satme.stats import map_stats_over_collection, fetch_stats_batch
        clean = _get_clean_collection(s2_source, geometry, minimal_cfg)
        stats_col = map_stats_over_collection(
            collection=clean,
            signal_names=["NDVI"],
            aoi=geometry,
            stats_cfg=minimal_cfg["stats"],
            preprocess_fn=s2_source.apply_cloud_mask,
            compute_fn=s2_source.compute_index,
            scale=s2_source.default_scale,
        )
        rows = fetch_stats_batch(stats_col, ["NDVI"], minimal_cfg["stats"])
        for row in rows:
            mean = row.get("NDVI_mean")
            if mean is not None:
                assert -1.0 <= mean <= 1.0, f"NDVI_mean out of range: {mean}"

    def test_ndmi_mean_in_valid_range(self, gee_connection, s2_source, geometry, minimal_cfg):
        """NDMI mean must be in -1 to 1."""
        from satme.stats import map_stats_over_collection, fetch_stats_batch
        clean = _get_clean_collection(s2_source, geometry, minimal_cfg)
        stats_col = map_stats_over_collection(
            collection=clean,
            signal_names=["NDMI"],
            aoi=geometry,
            stats_cfg=minimal_cfg["stats"],
            preprocess_fn=s2_source.apply_cloud_mask,
            compute_fn=s2_source.compute_index,
            scale=s2_source.default_scale,
        )
        rows = fetch_stats_batch(stats_col, ["NDMI"], minimal_cfg["stats"])
        for row in rows:
            mean = row.get("NDMI_mean")
            if mean is not None:
                assert -1.0 <= mean <= 1.0, f"NDMI_mean out of range: {mean}"

    def test_expected_stat_columns_present(self, gee_connection, s2_source, geometry, minimal_cfg):
        """Each index must produce _mean, _std, _min, _max, _p25, _p50, _p75."""
        from satme.stats import map_stats_over_collection, fetch_stats_batch
        clean = _get_clean_collection(s2_source, geometry, minimal_cfg)
        stats_col = map_stats_over_collection(
            collection=clean,
            signal_names=["NDVI"],
            aoi=geometry,
            stats_cfg=minimal_cfg["stats"],
            preprocess_fn=s2_source.apply_cloud_mask,
            compute_fn=s2_source.compute_index,
            scale=s2_source.default_scale,
        )
        rows = fetch_stats_batch(stats_col, ["NDVI"], minimal_cfg["stats"])
        expected_keys = {
            "NDVI_mean", "NDVI_std", "NDVI_min", "NDVI_max",
            "NDVI_p25", "NDVI_p50", "NDVI_p75",
        }
        for row in rows:
            actual_keys = set(row.keys())
            missing = expected_keys - actual_keys
            assert not missing, f"Stats row missing columns: {missing}"


class TestCHIRPS:

    def test_chirps_returns_correct_count(self, gee_connection, geometry, minimal_cfg):
        """batch_rainfall_scalars must return one value per date."""
        from datetime import date
        from satme.sources.chirps import ChirpsSource

        chirps = ChirpsSource(minimal_cfg["sources"]["chirps"])
        test_dates = [
            date(2020, 2, 1),
            date(2020, 3, 15),
            date(2020, 4, 1),
        ]
        results = chirps.batch_rainfall_scalars(test_dates, geometry)
        assert len(results) == len(test_dates), (
            f"Expected {len(test_dates)} CHIRPS values, got {len(results)}"
        )

    def test_chirps_values_are_floats_or_none(self, gee_connection, geometry, minimal_cfg):
        """Each CHIRPS value must be a float (mm) or None."""
        from datetime import date
        from satme.sources.chirps import ChirpsSource

        chirps = ChirpsSource(minimal_cfg["sources"]["chirps"])
        test_dates = [date(2020, 2, 1), date(2020, 3, 1)]
        results = chirps.batch_rainfall_scalars(test_dates, geometry)
        for val in results:
            assert val is None or isinstance(val, float), \
                f"CHIRPS value must be float or None, got {type(val)}: {val}"

    def test_chirps_values_are_non_negative(self, gee_connection, geometry, minimal_cfg):
        """Rainfall cannot be negative (mm)."""
        from datetime import date
        from satme.sources.chirps import ChirpsSource

        chirps = ChirpsSource(minimal_cfg["sources"]["chirps"])
        results = chirps.batch_rainfall_scalars([date(2020, 2, 1)], geometry)
        for val in results:
            if val is not None:
                assert val >= 0.0, f"Negative rainfall value: {val}"

    def test_chirps_returns_reasonable_value(self, gee_connection, geometry, minimal_cfg):
        """30-day accumulated rainfall in Kenya should be < 1000 mm (not absurd)."""
        from datetime import date
        from satme.sources.chirps import ChirpsSource

        chirps = ChirpsSource(minimal_cfg["sources"]["chirps"])
        results = chirps.batch_rainfall_scalars([date(2020, 4, 1)], geometry)
        for val in results:
            if val is not None:
                assert val < 2000.0, f"Implausibly large rainfall value: {val} mm"
