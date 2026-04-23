"""GEE integration tests — end-to-end pipeline smoke test.

Requires a live GEE connection.  Run with:
    pytest -m gee tests/test_gee_pipeline.py

Runs the full pipeline on the minimal config (6-month window, no GeoTIFFs)
and verifies that the output files exist and have the correct structure.
"""

import pytest
from pathlib import Path

pytestmark = [pytest.mark.gee, pytest.mark.slow]


class TestPipelineOutputs:

    def _run_pipeline(self, minimal_cfg):
        from satme.pipeline import run
        out_dir = run(minimal_cfg, skip_confirm=True)
        return out_dir

    def test_pipeline_completes_and_returns_path(self, gee_connection, minimal_cfg):
        out_dir = self._run_pipeline(minimal_cfg)
        assert out_dir is not None
        assert isinstance(out_dir, Path)

    def test_stats_csv_exists(self, gee_connection, minimal_cfg):
        out_dir = self._run_pipeline(minimal_cfg)
        stats_path = out_dir / "stats.csv"
        assert stats_path.exists(), f"stats.csv not found at {stats_path}"
        assert stats_path.stat().st_size > 0, "stats.csv is empty"

    def test_flag_report_exists(self, gee_connection, minimal_cfg):
        out_dir = self._run_pipeline(minimal_cfg)
        flag_path = out_dir / "flag_report.csv"
        assert flag_path.exists(), f"flag_report.csv not found at {flag_path}"

    def test_run_metadata_exists(self, gee_connection, minimal_cfg):
        out_dir = self._run_pipeline(minimal_cfg)
        meta_path = out_dir / "run_metadata.json"
        assert meta_path.exists()

    def test_stats_csv_has_expected_columns(self, gee_connection, minimal_cfg):
        import pandas as pd
        out_dir = self._run_pipeline(minimal_cfg)
        df = pd.read_csv(out_dir / "stats.csv")

        required_columns = {
            "date", "source", "image_id", "pre_post",
            "aoi_cloud_pct", "tile_cloud_pct",
            "NDVI_mean", "NDVI_std", "NDMI_mean",
            "chirps_30d_mm", "flags",
        }
        missing = required_columns - set(df.columns)
        assert not missing, f"stats.csv missing columns: {missing}"

    def test_stats_csv_source_column_is_sentinel2(self, gee_connection, minimal_cfg):
        import pandas as pd
        out_dir = self._run_pipeline(minimal_cfg)
        df = pd.read_csv(out_dir / "stats.csv")
        assert set(df["source"].unique()) == {"sentinel2"}, \
            f"Unexpected sources: {df['source'].unique()}"

    def test_stats_csv_pre_post_values(self, gee_connection, minimal_cfg):
        """pre_post column must only contain PRE or POST."""
        import pandas as pd
        out_dir = self._run_pipeline(minimal_cfg)
        df = pd.read_csv(out_dir / "stats.csv")
        valid = {"PRE", "POST"}
        actual = set(df["pre_post"].dropna().unique())
        assert actual.issubset(valid), f"Unexpected pre_post values: {actual - valid}"

    def test_stats_csv_ndvi_in_valid_range(self, gee_connection, minimal_cfg):
        """NDVI mean must be in [-1, 1] for all rows."""
        import pandas as pd
        out_dir = self._run_pipeline(minimal_cfg)
        df = pd.read_csv(out_dir / "stats.csv")
        col = df["NDVI_mean"].dropna()
        if len(col) == 0:
            pytest.skip("No NDVI values in output — all images may be cloudy")
        assert (col >= -1.0).all(), f"NDVI_mean below -1: {col[col < -1.0]}"
        assert (col <=  1.0).all(), f"NDVI_mean above +1: {col[col > 1.0]}"

    def test_stats_csv_dates_in_range(self, gee_connection, minimal_cfg):
        """All dates in stats.csv must fall within the configured date_range."""
        import pandas as pd
        from datetime import date
        out_dir = self._run_pipeline(minimal_cfg)
        df = pd.read_csv(out_dir / "stats.csv")
        if df.empty:
            pytest.skip("No rows in stats.csv")
        start = date.fromisoformat(minimal_cfg["date_range"]["start"])
        end   = date.fromisoformat(minimal_cfg["date_range"]["end"])
        dates = pd.to_datetime(df["date"]).dt.date
        assert (dates >= start).all(), "Some dates before date_range.start"
        assert (dates <= end).all(),   "Some dates after date_range.end"

    def test_flag_report_has_more_rows_than_stats(self, gee_connection, minimal_cfg):
        """flag_report includes rejected images; stats only includes clean ones."""
        import pandas as pd
        out_dir = self._run_pipeline(minimal_cfg)
        stats_df = pd.read_csv(out_dir / "stats.csv")
        flags_df = pd.read_csv(out_dir / "flag_report.csv")
        assert len(flags_df) >= len(stats_df), (
            f"flag_report ({len(flags_df)} rows) should be >= stats ({len(stats_df)} rows)"
        )

    def test_run_metadata_has_config(self, gee_connection, minimal_cfg):
        """run_metadata.json must contain the run config."""
        import json
        out_dir = self._run_pipeline(minimal_cfg)
        with open(out_dir / "run_metadata.json") as f:
            meta = json.load(f)
        assert "config" in meta
        assert "run_name" in meta
        assert "timestamp_utc" in meta
        assert "aoi" in meta
