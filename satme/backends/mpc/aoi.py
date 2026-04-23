"""MPC AOI construction stub.

MPC / STAC uses GeoJSON bounding boxes and shapely geometries instead of
ee.Geometry objects.  The config format (point_radius, polygon) is identical —
only the output type changes.

Equivalent to: satme/aoi.py  (GEE version returns ee.Geometry)
"""

# PSEUDOCODE — not executable

# import math
# from shapely.geometry import box, Polygon, mapping


# def build(cfg: dict) -> tuple:
#     """Build a shapely geometry from the ``aoi`` config block.
#
#     Returns
#     -------
#     (shapely.Polygon, dict)
#         geometry     : shapely.Polygon (used directly in STAC search as bbox
#                        and for spatial operations with rioxarray)
#         metadata_dict: same structure as the GEE version — area_km2, wkt, etc.
#
#     Key difference vs GEE version
#     ──────────────────────────────
#     GEE aoi.build() returns ee.Geometry.Rectangle / ee.Geometry.Polygon.
#     This returns shapely.Polygon — a pure Python object, no network call.
#     The bounding box is derived via shapely_geom.bounds → (minx,miny,maxx,maxy)
#     and passed to pystac_client.search(bbox=...).
#     """
#     aoi_cfg = cfg["aoi"]
#     mode = aoi_cfg["mode"]
#
#     if mode == "point_radius":
#         lat = float(aoi_cfg["center"]["lat"])
#         lon = float(aoi_cfg["center"]["lon"])
#         radius_m = float(aoi_cfg["radius_m"])
#
#         lat_offset = radius_m / 111_320
#         lon_offset = radius_m / (111_320 * math.cos(math.radians(lat)))
#         geometry = box(lon - lon_offset, lat - lat_offset,
#                        lon + lon_offset, lat + lat_offset)
#
#         area_km2 = ((2 * radius_m) ** 2) / 1_000_000
#         meta = {
#             "mode": "point_radius",
#             "center_lat": lat, "center_lon": lon, "radius_m": radius_m,
#             "area_km2": round(area_km2, 6),
#             "wkt": geometry.wkt,
#             # STAC search uses bbox — expose it for convenience
#             "bbox": list(geometry.bounds),   # [west, south, east, north]
#             "geojson": mapping(geometry),     # GeoJSON dict for rioxarray clip
#         }
#
#     elif mode == "polygon":
#         coordinates = aoi_cfg["coordinates"]
#         geometry = Polygon(coordinates)
#         # ... same area calculation as GEE version ...
#         meta = {
#             "mode": "polygon",
#             "area_km2": ...,
#             "wkt": geometry.wkt,
#             "bbox": list(geometry.bounds),
#             "geojson": mapping(geometry),
#         }
#
#     return geometry, meta
#
#
# # Usage in the MPC pipeline:
# #   geometry, aoi_meta = mpc_aoi.build(cfg)
# #   bbox = aoi_meta["bbox"]          # → passed to catalog.search(bbox=bbox)
# #   geojson = aoi_meta["geojson"]    # → passed to da.rio.clip([geojson])
