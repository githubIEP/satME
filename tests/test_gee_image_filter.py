"""GEE integration tests — collection filtering and batch metadata.

Requires a live GEE connection.  Run with:
    pytest -m gee tests/test_gee_image_filter.py

Checks that:
  - Sentinel-2 collection returns images for the test AOI/period
  - prefilter_by_aoi_cloud returns two collections with aoi_cloud_pct set
  - batch_image_metadata returns dicts with all expected keys
  - Returned values are in physically valid ranges
"""

import pytest
from datetime import date

pytestmark = pytest.mark.gee


class TestCollectionQuery:

    def test_s2_collection_returns_images(self, gee_connection, s2_source, geometry, minimal_cfg):
        """Sentinel-2 must find at least one image for the 6-month test period."""
        from satme.image_filter import collection_size
        col = s2_source.get_collection(geometry, minimal_cfg["date_range"])
        n = collection_size(col)
        assert n > 0, (
            f"No Sentinel-2 images found for AOI in "
            f"{minimal_cfg['date_range']['start']} – {minimal_cfg['date_range']['end']}"
        )

    def test_s2_collection_returns_expected_bands(self, gee_connection, s2_source, geometry, minimal_cfg):
        """First image in collection must contain the SCL band."""
        import ee
        col = s2_source.get_collection(geometry, minimal_cfg["date_range"])
        first = ee.Image(col.first())
        band_names = first.bandNames().getInfo()
        assert "SCL" in band_names, f"SCL band missing from collection. Found: {band_names}"

    def test_viirs_collection_returns_images(self, gee_connection, viirs_source, geometry, minimal_cfg):
        """VIIRS must find monthly composites for the test period."""
        from satme.image_filter import collection_size
        col = viirs_source.get_collection(geometry, minimal_cfg["date_range"])
        n = collection_size(col)
        # 6-month window → expect ~6 monthly composites
        assert n > 0, "No VIIRS images found for test period"
        assert n <= 8, f"Unexpectedly many VIIRS images: {n} (expected ~6)"


class TestPrefilter:

    def test_prefilter_returns_two_collections(self, gee_connection, s2_source, geometry, minimal_cfg):
        """prefilter_by_aoi_cloud must return (clean, full) as a tuple of length 2."""
        col = s2_source.get_collection(geometry, minimal_cfg["date_range"])
        result = s2_source.aoi_quality_fn()   # get the fn
        from satme.image_filter import prefilter_by_aoi_cloud
        clean, full = prefilter_by_aoi_cloud(
            collection=col,
            aoi=geometry,
            max_aoi_cloud_pct=50,
            quality_fn=s2_source.aoi_quality_fn(),
            scale=s2_source.default_scale,
        )
        assert clean is not None
        assert full is not None

    def test_clean_collection_size_lte_full(self, gee_connection, s2_source, geometry, minimal_cfg):
        """Clean collection must be a subset of full (never larger)."""
        from satme.image_filter import prefilter_by_aoi_cloud, collection_size
        col = s2_source.get_collection(geometry, minimal_cfg["date_range"])
        clean, full = prefilter_by_aoi_cloud(
            collection=col,
            aoi=geometry,
            max_aoi_cloud_pct=50,
            quality_fn=s2_source.aoi_quality_fn(),
            scale=s2_source.default_scale,
        )
        n_clean = collection_size(clean)
        n_full  = collection_size(full)
        assert n_clean <= n_full, (
            f"Clean collection ({n_clean}) larger than full ({n_full})"
        )

    def test_full_collection_has_aoi_cloud_pct_property(self, gee_connection, s2_source, geometry, minimal_cfg):
        """Every image in full_col must have aoi_cloud_pct set as a property."""
        import ee
        from satme.image_filter import prefilter_by_aoi_cloud
        col = s2_source.get_collection(geometry, minimal_cfg["date_range"])
        _, full = prefilter_by_aoi_cloud(
            collection=col,
            aoi=geometry,
            max_aoi_cloud_pct=100,   # keep everything
            quality_fn=s2_source.aoi_quality_fn(),
            scale=s2_source.default_scale,
        )
        # Pull aoi_cloud_pct from first image
        first_pct = ee.Image(full.first()).get("aoi_cloud_pct").getInfo()
        assert first_pct is not None, "aoi_cloud_pct not set on image"
        assert 0.0 <= float(first_pct) <= 100.0, \
            f"aoi_cloud_pct out of range: {first_pct}"

    def test_sar_quality_fn_returns_zero(self, gee_connection, s1_source, geometry, minimal_cfg):
        """SAR quality function must always return 0 (no cloud filtering)."""
        import ee
        fn = s1_source.aoi_quality_fn()
        # Get any image to pass to the fn — use a VIIRS image as a proxy
        # (the fn doesn't look at pixels for SAR, just returns constant 0)
        col = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(geometry)
            .filterDate("2020-01-01", "2020-02-01")
            .first()
        )
        result = fn(col, geometry, 20).getInfo()
        assert result == 0, f"SAR quality fn should return 0, got {result}"


class TestBatchMetadata:

    def _get_prefiltered(self, s2_source, geometry, minimal_cfg):
        from satme.image_filter import prefilter_by_aoi_cloud
        col = s2_source.get_collection(geometry, minimal_cfg["date_range"])
        _, full = prefilter_by_aoi_cloud(
            collection=col,
            aoi=geometry,
            max_aoi_cloud_pct=100,
            quality_fn=s2_source.aoi_quality_fn(),
            scale=s2_source.default_scale,
        )
        return full

    def test_batch_metadata_returns_list(self, gee_connection, s2_source, geometry, minimal_cfg):
        from satme.image_filter import batch_image_metadata
        full = self._get_prefiltered(s2_source, geometry, minimal_cfg)
        meta = batch_image_metadata(full, source=s2_source)
        assert isinstance(meta, list)
        assert len(meta) > 0

    def test_batch_metadata_has_required_keys(self, gee_connection, s2_source, geometry, minimal_cfg):
        from satme.image_filter import batch_image_metadata
        full = self._get_prefiltered(s2_source, geometry, minimal_cfg)
        meta = batch_image_metadata(full, source=s2_source)
        required = {"image_id", "date", "tile_cloud_pct", "aoi_cloud_pct", "aoi_covered"}
        for row in meta:
            missing = required - set(row.keys())
            assert not missing, f"Metadata row missing keys: {missing}\nRow: {row}"

    def test_batch_metadata_dates_are_valid_iso(self, gee_connection, s2_source, geometry, minimal_cfg):
        from satme.image_filter import batch_image_metadata
        full = self._get_prefiltered(s2_source, geometry, minimal_cfg)
        meta = batch_image_metadata(full, source=s2_source)
        for row in meta:
            if row["date"] is not None:
                d = date.fromisoformat(row["date"])
                assert d.year >= 2015, f"Unexpected date year: {d}"

    def test_batch_metadata_cloud_pct_in_range(self, gee_connection, s2_source, geometry, minimal_cfg):
        from satme.image_filter import batch_image_metadata
        full = self._get_prefiltered(s2_source, geometry, minimal_cfg)
        meta = batch_image_metadata(full, source=s2_source)
        for row in meta:
            pct = row.get("aoi_cloud_pct")
            if pct is not None:
                assert 0.0 <= pct <= 100.0, f"aoi_cloud_pct out of range: {pct}"

    def test_batch_metadata_s2_specific_keys(self, gee_connection, s2_source, geometry, minimal_cfg):
        """S2-specific fields (mgrs_tile etc.) must be present."""
        from satme.image_filter import batch_image_metadata
        full = self._get_prefiltered(s2_source, geometry, minimal_cfg)
        meta = batch_image_metadata(full, source=s2_source)
        s2_keys = {"mgrs_tile", "orbit_number", "processing_baseline"}
        for row in meta:
            missing = s2_keys - set(row.keys())
            assert not missing, f"S2-specific keys missing: {missing}"
