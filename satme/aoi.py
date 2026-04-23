"""AOI construction from config.

Supports two modes driven entirely by the config ``aoi`` block:
  - ``point_radius``: expands a lat/lon centre by a radius in metres into a
    bounding square (ee.Geometry.Rectangle).
  - ``polygon``: passes a coordinate list directly to ee.Geometry.Polygon.

Surrounding boxes (point_radius only)
--------------------------------------
When ``aoi.surrounding_boxes: true``, ``build_tiles()`` returns geometries for
a 3×3 grid of same-size tiles centred on the AOI.  The centre tile is labelled
``"center"``; the eight neighbours use compass labels (N, NE, E, SE, S, SW, W,
NW).  Each tile is offset from the next by exactly ``2 * radius_m``, so the
grid is contiguous with no gaps or overlaps.

Key outputs
-----------
build(cfg) -> (ee.Geometry, dict)
    Returns the centre GEE geometry and a metadata dict (area_km2, wkt, mode).
build_tiles(cfg) -> list[(label, ee.Geometry)]
    Returns 9 (label, geometry) tuples for the full 3×3 grid.
build_ee_tiles_fc(tiles) -> ee.FeatureCollection
    Converts the tile list to a FeatureCollection with an ``aoi_tile`` property.
build_full_extent(tiles) -> ee.Geometry
    Bounding union of all tile geometries — used for filterBounds when pulling
    a collection that must cover the whole grid.
"""

import math
import logging

import ee
from shapely.geometry import box, Polygon

logger = logging.getLogger(__name__)

# Compass offsets for the 3×3 grid — (east_tiles, north_tiles)
# One unit = 2 * radius_m (full tile width)
_TILE_OFFSETS = {
    "center": ( 0,  0),
    "N":      ( 0,  1),
    "NE":     ( 1,  1),
    "E":      ( 1,  0),
    "SE":     ( 1, -1),
    "S":      ( 0, -1),
    "SW":     (-1, -1),
    "W":      (-1,  0),
    "NW":     (-1,  1),
}


def _point_radius_geometry(lat: float, lon: float, radius_m: float) -> ee.Geometry:
    """Convert a centre point + radius to a bounding square ee.Geometry.Rectangle."""
    lat_offset = radius_m / 111_320
    lon_offset = radius_m / (111_320 * math.cos(math.radians(lat)))

    west = lon - lon_offset
    east = lon + lon_offset
    south = lat - lat_offset
    north = lat + lat_offset

    logger.debug(
        "AOI bounding box: W=%.6f E=%.6f S=%.6f N=%.6f", west, east, south, north
    )
    return ee.Geometry.Rectangle([west, south, east, north])


def _polygon_geometry(coordinates: list) -> ee.Geometry:
    """Build an ee.Geometry.Polygon from a coordinate list [[lon, lat], ...]."""
    return ee.Geometry.Polygon(coordinates)


def _shapely_from_point_radius(lat: float, lon: float, radius_m: float) -> Polygon:
    lat_offset = radius_m / 111_320
    lon_offset = radius_m / (111_320 * math.cos(math.radians(lat)))
    return box(lon - lon_offset, lat - lat_offset, lon + lon_offset, lat + lat_offset)


def _shapely_from_polygon(coordinates: list) -> Polygon:
    return Polygon(coordinates)


def build(cfg: dict) -> tuple:
    """Build an ee.Geometry from the ``aoi`` block of the config.

    Parameters
    ----------
    cfg:
        The full parsed config dict.

    Returns
    -------
    (ee.Geometry, dict)
        The GEE geometry and a metadata dict containing:
        - ``mode``: "point_radius" or "polygon"
        - ``area_km2``: approximate AOI area
        - ``wkt``: WKT string of the AOI for archiving in run metadata
        - ``center_lat``, ``center_lon`` (point_radius only)
        - ``radius_m`` (point_radius only)
    """
    aoi_cfg = cfg["aoi"]
    mode = aoi_cfg["mode"]

    if mode == "point_radius":
        lat = float(aoi_cfg["center"]["lat"])
        lon = float(aoi_cfg["center"]["lon"])
        radius_m = float(aoi_cfg["radius_m"])

        geometry = _point_radius_geometry(lat, lon, radius_m)
        shapely_geom = _shapely_from_point_radius(lat, lon, radius_m)

        # Area: a square with side 2*radius_m → (2r)² m² → km²
        area_km2 = ((2 * radius_m) ** 2) / 1_000_000

        meta = {
            "mode": "point_radius",
            "center_lat": lat,
            "center_lon": lon,
            "radius_m": radius_m,
            "area_km2": round(area_km2, 6),
            "wkt": shapely_geom.wkt,
        }

    elif mode == "polygon":
        coordinates = aoi_cfg["coordinates"]
        geometry = _polygon_geometry(coordinates)
        shapely_geom = _shapely_from_polygon(coordinates)

        # Approximate area via Shapely (degree-based — fine for small AOIs)
        # For accurate area, use GEE's geometry.area() server-side
        area_deg2 = shapely_geom.area
        # rough conversion at the AOI's centroid latitude
        centroid = shapely_geom.centroid
        km_per_deg_lat = 111.32
        km_per_deg_lon = 111.32 * math.cos(math.radians(centroid.y))
        area_km2 = area_deg2 * km_per_deg_lat * km_per_deg_lon

        meta = {
            "mode": "polygon",
            "area_km2": round(area_km2, 6),
            "wkt": shapely_geom.wkt,
        }

    else:
        raise ValueError(
            f"Unknown AOI mode '{mode}'. Valid options: 'point_radius', 'polygon'."
        )

    logger.info(
        "AOI constructed — mode=%s area=%.4f km²", meta["mode"], meta["area_km2"]
    )
    return geometry, meta


def build_tiles(cfg: dict) -> list:
    """Build geometries for a 3×3 grid of same-size tiles around the AOI centre.

    Only valid for ``aoi.mode == "point_radius"``.  Returns tiles for the
    centre AOI plus its eight compass neighbours, all the same size, contiguous
    with no gaps.

    Parameters
    ----------
    cfg:
        Full config dict.  ``aoi.surrounding_boxes`` must be ``true`` and
        ``aoi.mode`` must be ``"point_radius"``.

    Returns
    -------
    list of (label: str, geometry: ee.Geometry)
        Always 9 entries in the order defined by ``_TILE_OFFSETS``:
        center, N, NE, E, SE, S, SW, W, NW.

    Raises
    ------
    ValueError
        If called when ``aoi.mode`` is not ``"point_radius"``.
    """
    aoi_cfg = cfg["aoi"]
    if aoi_cfg.get("mode") != "point_radius":
        raise ValueError(
            "surrounding_boxes is only supported for aoi.mode = 'point_radius'. "
            "Polygon AOIs have no well-defined neighbour grid."
        )

    lat      = float(aoi_cfg["center"]["lat"])
    lon      = float(aoi_cfg["center"]["lon"])
    radius_m = float(aoi_cfg["radius_m"])

    # Each tile is 2*radius_m wide, so adjacent centres are 2*radius_m apart
    step_lat = (2 * radius_m) / 111_320
    step_lon = (2 * radius_m) / (111_320 * math.cos(math.radians(lat)))

    tiles = []
    for label, (dx, dy) in _TILE_OFFSETS.items():
        tile_lat = lat + dy * step_lat
        tile_lon = lon + dx * step_lon
        geom = _point_radius_geometry(tile_lat, tile_lon, radius_m)
        tiles.append((label, geom))
        logger.debug(
            "Tile %s: centre=(%.6f, %.6f) offset=(%+d,%+d)",
            label, tile_lat, tile_lon, dx, dy,
        )

    logger.info(
        "Surrounding tiles built — 3×3 grid, radius=%dm, step_lat=%.6f°, step_lon=%.6f°",
        radius_m, step_lat, step_lon,
    )
    return tiles


def build_ee_tiles_fc(tiles: list) -> "ee.FeatureCollection":
    """Convert a tile list to a GEE FeatureCollection.

    Each feature carries an ``aoi_tile`` string property (e.g. ``"center"``,
    ``"N"``, ``"NE"`` …) used as the grouping key in ``reduceRegions`` output.

    Parameters
    ----------
    tiles:
        Output of ``build_tiles()``.

    Returns
    -------
    ee.FeatureCollection
        One feature per tile with geometry and ``aoi_tile`` property set.
    """
    features = [
        ee.Feature(geom, {"aoi_tile": label})
        for label, geom in tiles
    ]
    return ee.FeatureCollection(features)


def build_bounds(cfg: dict) -> tuple[float, float, float, float]:
    """Return the centre AOI as (west, south, east, north) in WGS84.

    Does not require an EE connection — derived directly from the config.
    Used by the Copernicus STAC search and rasterio window reads.
    """
    aoi_cfg = cfg["aoi"]
    mode = aoi_cfg["mode"]

    if mode == "point_radius":
        lat      = float(aoi_cfg["center"]["lat"])
        lon      = float(aoi_cfg["center"]["lon"])
        radius_m = float(aoi_cfg["radius_m"])
        lat_off  = radius_m / 111_320
        lon_off  = radius_m / (111_320 * math.cos(math.radians(lat)))
        return (lon - lon_off, lat - lat_off, lon + lon_off, lat + lat_off)

    elif mode == "polygon":
        coords = aoi_cfg["coordinates"]
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        return (min(lons), min(lats), max(lons), max(lats))

    else:
        raise ValueError(f"Unknown AOI mode '{mode}'")


def build_full_extent(tiles: list) -> "ee.Geometry":
    """Return the bounding rectangle of all tile geometries.

    Used as the ``filterBounds`` geometry when querying satellite collections
    so that images covering any of the 9 tiles are included.  For small AOIs
    (radius_m < ~10 km) all tiles fit within a single satellite swath, so
    using the centre geometry alone would give the same result — but this
    version is correct for larger AOIs too.

    Parameters
    ----------
    tiles:
        Output of ``build_tiles()``.

    Returns
    -------
    ee.Geometry
        Bounding rectangle of the union of all tile geometries.
    """
    fc = ee.FeatureCollection([ee.Feature(geom) for _, geom in tiles])
    return fc.geometry().bounds()
