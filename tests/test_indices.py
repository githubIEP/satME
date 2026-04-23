"""Unit tests — spectral index registry and source compute_index (no GEE required).

Tests that every index in the registry has the required structure,
and that source-level index validation works correctly.
"""

import pytest
from satme.indices import REGISTRY, validate_indices


REQUIRED_INDEX_KEYS = {"formula", "bands", "valid_range", "description"}


class TestRegistry:

    def test_registry_is_not_empty(self):
        assert len(REGISTRY) > 0

    def test_all_entries_have_required_keys(self):
        for name, entry in REGISTRY.items():
            missing = REQUIRED_INDEX_KEYS - set(entry.keys())
            assert not missing, f"Index '{name}' missing keys: {missing}"

    def test_all_formulas_are_callable(self):
        for name, entry in REGISTRY.items():
            assert callable(entry["formula"]), \
                f"Index '{name}' formula is not callable"

    def test_all_bands_are_lists(self):
        for name, entry in REGISTRY.items():
            assert isinstance(entry["bands"], list), \
                f"Index '{name}' bands must be a list"
            assert len(entry["bands"]) > 0, \
                f"Index '{name}' bands list is empty"

    def test_all_valid_ranges_are_tuples_of_two(self):
        for name, entry in REGISTRY.items():
            vr = entry["valid_range"]
            assert len(vr) == 2, f"Index '{name}' valid_range must have 2 elements"
            assert vr[0] < vr[1], \
                f"Index '{name}' valid_range min must be < max"

    def test_all_descriptions_are_strings(self):
        for name, entry in REGISTRY.items():
            assert isinstance(entry["description"], str), \
                f"Index '{name}' description must be a string"
            assert len(entry["description"]) > 0

    def test_known_indices_present(self):
        for name in ["NDVI", "NDWI", "NDMI", "EVI", "SAVI"]:
            assert name in REGISTRY, f"Expected index '{name}' not in registry"

    def test_ndvi_bands(self):
        assert set(REGISTRY["NDVI"]["bands"]) == {"B8", "B4"}

    def test_ndwi_bands(self):
        assert set(REGISTRY["NDWI"]["bands"]) == {"B3", "B8"}

    def test_ndmi_bands(self):
        assert set(REGISTRY["NDMI"]["bands"]) == {"B8A", "B11"}

    def test_ndvi_valid_range(self):
        lo, hi = REGISTRY["NDVI"]["valid_range"]
        assert lo == -1.0
        assert hi ==  1.0


class TestValidateIndices:

    def test_all_valid(self):
        available = ["B2", "B3", "B4", "B8", "B8A", "B11"]
        issues = validate_indices(["NDVI", "NDWI", "NDMI"], available)
        assert issues == []

    def test_missing_band_detected(self):
        available = ["B3", "B4"]   # B8 missing — NDVI needs it
        issues = validate_indices(["NDVI"], available)
        assert len(issues) == 1
        assert "NDVI" in issues[0]
        assert "B8" in issues[0]

    def test_unknown_index_detected(self):
        issues = validate_indices(["FAKEIDX"], ["B2", "B3", "B4", "B8"])
        assert len(issues) == 1
        assert "FAKEIDX" in issues[0]

    def test_multiple_issues(self):
        issues = validate_indices(
            ["NDVI", "FAKEIDX"],
            ["B3", "B4"],   # B8 missing for NDVI
        )
        assert len(issues) == 2

    def test_empty_request_no_issues(self):
        issues = validate_indices([], ["B2", "B3", "B4", "B8"])
        assert issues == []


class TestSentinel1Indices:

    def test_s1_source_accepts_valid_indices(self, s1_source):
        # Should not raise
        assert s1_source is not None

    def test_s1_source_rejects_unknown_index(self):
        from satme.sources.sentinel1 import Sentinel1Source
        with pytest.raises(ValueError, match="unknown"):
            Sentinel1Source({"indices": ["NDVI"]})   # NDVI is not a SAR index

    def test_s1_valid_index_names(self):
        from satme.sources.sentinel1 import _VALID_INDICES
        assert "RVI" in _VALID_INDICES
        assert "VH_VV" in _VALID_INDICES
        assert "DPSVI" in _VALID_INDICES


class TestVIIRSSignals:

    def test_viirs_source_accepts_avg_rad(self, viirs_source):
        assert viirs_source is not None

    def test_viirs_rejects_spectral_index(self):
        from satme.sources.viirs import VIIRSSource
        with pytest.raises(ValueError):
            VIIRSSource({"indices": ["NDVI"]})

    def test_viirs_default_scale(self, viirs_source):
        assert viirs_source.default_scale == 500


class TestSourceDefaults:

    def test_s2_default_scale(self, s2_source):
        assert s2_source.default_scale == 20

    def test_s1_default_scale(self, s1_source):
        assert s1_source.default_scale == 10

    def test_s2_export_scale_ndmi(self, s2_source):
        assert s2_source.export_scale("NDMI") == 20

    def test_s2_export_scale_ndvi(self, s2_source):
        assert s2_source.export_scale("NDVI") == 10
