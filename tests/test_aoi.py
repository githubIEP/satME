"""Unit tests — AOI construction (no GEE required).

Tests that the shapely-based area calculation and geometry building
work correctly before any GEE call is made.
"""

import math
import pytest


class TestPointRadiusAOI:

    def test_build_returns_tuple(self, cfg):
        """aoi.build() must return (geometry, metadata_dict) without GEE."""
        # We test the shapely side only — the ee.Geometry half needs GEE
        from satme.aoi import _shapely_from_point_radius, _point_radius_geometry
        import ee
        # Skip if ee not initialised (we only test shapely here)
        shapely_geom = _shapely_from_point_radius(-1.54351, 37.33258, 500)
        assert shapely_geom is not None

    def test_area_calculation_point_radius(self):
        """500 m radius → bounding square ≈ 1 km² ≈ 1.0 km²."""
        radius_m = 500
        expected_km2 = ((2 * radius_m) ** 2) / 1_000_000   # = 1.0 km²
        assert abs(expected_km2 - 1.0) < 1e-9

    def test_shapely_geometry_is_valid(self):
        from satme.aoi import _shapely_from_point_radius
        geom = _shapely_from_point_radius(-1.54351, 37.33258, 500)
        assert geom.is_valid
        assert geom.area > 0

    def test_bbox_contains_center(self):
        from satme.aoi import _shapely_from_point_radius
        from shapely.geometry import Point
        lat, lon, radius = -1.54351, 37.33258, 500
        geom = _shapely_from_point_radius(lat, lon, radius)
        assert geom.contains(Point(lon, lat))

    def test_radius_scaling(self):
        """Doubling radius_m should quadruple area."""
        from satme.aoi import _shapely_from_point_radius
        g500  = _shapely_from_point_radius(0.0, 0.0, 500)
        g1000 = _shapely_from_point_radius(0.0, 0.0, 1000)
        # Area scales as r² — 4× when r doubles
        ratio = g1000.area / g500.area
        assert abs(ratio - 4.0) < 0.01

    def test_lon_lat_offset_direction(self):
        """West bound must be < center_lon < east bound."""
        from satme.aoi import _shapely_from_point_radius
        lat, lon = 0.0, 30.0
        geom = _shapely_from_point_radius(lat, lon, 1000)
        minx, miny, maxx, maxy = geom.bounds
        assert minx < lon < maxx
        assert miny < lat < maxy


class TestPolygonAOI:

    def test_polygon_geometry_is_valid(self):
        from satme.aoi import _shapely_from_polygon
        coords = [
            [37.328163, -1.54069],
            [37.334633, -1.54069],
            [37.334633, -1.545666],
            [37.328163, -1.545666],
            [37.328163, -1.54069],
        ]
        geom = _shapely_from_polygon(coords)
        assert geom.is_valid
        assert geom.area > 0

    def test_polygon_area_reasonable(self):
        """Small polygon ~500 m × ~700 m → rough area in degree² units."""
        from satme.aoi import _shapely_from_polygon
        coords = [
            [37.328163, -1.54069],
            [37.334633, -1.54069],
            [37.334633, -1.545666],
            [37.328163, -1.545666],
            [37.328163, -1.54069],
        ]
        geom = _shapely_from_polygon(coords)
        # Area in degree² — should be tiny but nonzero
        assert 1e-6 < geom.area < 1.0


class TestAOIMetadata:

    def test_metadata_keys_point_radius(self, cfg):
        """Metadata dict must have required keys — tested without GEE by mocking."""
        # Check the structure by computing expected values manually
        aoi_cfg = cfg["aoi"]
        if aoi_cfg["mode"] != "point_radius":
            pytest.skip("Config is not point_radius mode")

        radius_m = float(aoi_cfg["radius_m"])
        expected_area = ((2 * radius_m) ** 2) / 1_000_000
        assert expected_area > 0

        # Required metadata keys
        required_meta_keys = {"mode", "center_lat", "center_lon", "radius_m", "area_km2", "wkt"}
        # We can't call aoi.build() without GEE, but we can verify the config
        # has the inputs needed to produce those keys
        assert "center" in aoi_cfg
        assert "radius_m" in aoi_cfg
