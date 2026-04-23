"""MPC Sentinel-2 L2A source stub.

Replaces: satme/sources/sentinel2.py  (GEE version)

The SatelliteSource interface (base.py) is backend-agnostic — its abstract
methods use `object` type hints.  This class implements the same contract
but returns STAC items / xarray DataArrays instead of ee.Image objects.

MPC collection for Sentinel-2 L2A
───────────────────────────────────
Collection ID : "sentinel-2-l2a"
STAC endpoint : https://planetarycomputer.microsoft.com/api/stac/v1
Bands present : same as GEE — B2, B3, B4, B5, B6, B7, B8, B8A, B11, B12, SCL
Coverage      : global, 2015-11-01 to present (same archive as GEE)
Cloud property: "eo:cloud_cover"  (equivalent to CLOUDY_PIXEL_PERCENTAGE)

The key behavioural difference vs the GEE source:
  get_collection() returns list[pystac.Item], not ee.ImageCollection.
  apply_cloud_mask() operates on xarray.DataArray, not ee.Image.
  compute_index() returns xarray.DataArray, not ee.Image.
  image_metadata() reads item.properties dict — no network call needed.
"""

# PSEUDOCODE — not executable

# from satme.sources.base import SatelliteSource

# MPC_COLLECTION_ID = "sentinel-2-l2a"
# MPC_BAND_ASSETS = {
#     "B2": "blue", "B3": "green", "B4": "red",
#     "B5": "rededge", "B8": "nir", "B8A": "nir08",
#     "B11": "swir16", "B12": "swir22", "SCL": "SCL",
# }
# SCL_INVALID = [0, 1, 3, 8, 9, 10]


# class Sentinel2MPCSource(SatelliteSource):
#     """Sentinel-2 L2A source backed by Microsoft Planetary Computer.
#
#     Drop-in replacement for Sentinel2Source (GEE version).
#     The pipeline resolves this class when backend="mpc" is set in config.
#
#     Interface contract (same abstract methods, different return types)
#     ──────────────────────────────────────────────────────────────────
#     get_collection()    → list[pystac.Item]      (was ee.ImageCollection)
#     apply_cloud_mask()  → xarray.DataArray       (was ee.Image)
#     compute_index()     → xarray.DataArray       (was ee.Image)
#     image_metadata()    → dict                   (same — plain Python)
#     check_aoi_coverage()→ bool                   (same)
#     """
#
#     source_name    = "sentinel2"
#     collection_id  = MPC_COLLECTION_ID           # STAC collection ID (not GEE)
#     available_bands = ["B2","B3","B4","B5","B8","B8A","B11","B12"]
#     archive_start  = "2015-11-01"
#
#     def __init__(self, src_cfg: dict):
#         self.cfg = src_cfg
#         self.max_tile_cloud  = src_cfg.get("max_tile_cloud_pct", 100)
#         self.max_aoi_cloud   = src_cfg.get("max_aoi_cloud_pct", 100)
#         self.requested_indices = src_cfg.get("indices", ["NDVI"])
#
#         # Validate indices — same call as GEE version (registry is shared)
#         from satme.indices import validate_indices
#         issues = validate_indices(self.requested_indices, self.available_bands)
#         if issues:
#             raise ValueError(f"Sentinel-2 MPC index errors: {issues}")
#
#     def get_collection(self, aoi: object, date_range: dict) -> list:
#         """Search MPC STAC for Sentinel-2 items matching AOI + date range.
#
#         Parameters
#         ----------
#         aoi:
#             shapely.Polygon (from mpc.aoi.build()).
#             Internally converted to bbox [W, S, E, N] for the STAC query.
#         date_range:
#             Dict {"start": "...", "end": "..."} — same as GEE version.
#
#         Returns
#         -------
#         list[pystac.Item]
#             Signed items (asset URLs include SAS tokens).
#             Each item has all metadata in item.properties — no extra round-trip.
#         """
#         from satme.backends.mpc.image_filter import search_collection
#         # aoi is a shapely.Polygon; convert to bbox for STAC search
#         bbox = list(aoi.bounds)   # (minx, miny, maxx, maxy)
#         return search_collection(
#             catalog=self._catalog,           # injected at construction time
#             collection_id=MPC_COLLECTION_ID,
#             bbox=bbox,
#             date_range=date_range,
#             max_tile_cloud_pct=self.max_tile_cloud,
#         )
#
#     def apply_cloud_mask(self, image: object) -> object:
#         """Apply SCL-based cloud mask to a DataArray slice.
#
#         Parameters
#         ----------
#         image:
#             xarray.DataArray with shape (band, y, x) for one time step.
#             Must include the "SCL" band (asset key = "SCL").
#
#         Returns
#         -------
#         xarray.DataArray
#             Same DataArray with cloud/shadow pixels set to NaN.
#             Equivalent to cloud_mask.sentinel2_scl() for ee.Image.
#         """
#         import numpy as np
#         scl = image.sel(band="SCL").values
#         valid = ~np.isin(scl, SCL_INVALID)                 # True = valid
#         return image.where(image.coords["band"] == "SCL" or valid)
#         # More precisely:
#         # valid_da = xr.DataArray(valid, dims=["y","x"])
#         # return image.where(valid_da)   # sets invalid pixels to NaN
#
#     def get_tile_cloud_pct(self, image: object) -> float | None:
#         """Read tile-level cloud % from the STAC item properties.
#
#         For MPC, image here is a pystac.Item (not yet loaded as DataArray).
#         The tile cloud % is in item.properties["eo:cloud_cover"].
#         No network call needed — it was fetched during the STAC search.
#         """
#         return image.properties.get("eo:cloud_cover")
#
#     def compute_index(self, image: object, index_name: str) -> object:
#         """Compute a spectral index from a DataArray.
#
#         Parameters
#         ----------
#         image:
#             xarray.DataArray (band, y, x) — cloud masked.
#         index_name:
#             e.g. "NDVI"
#
#         Returns
#         -------
#         xarray.DataArray
#             2D DataArray (y, x) with the index values.
#         """
#         from satme.backends.mpc.stats import _compute_index_numpy
#         # For a single DataArray (no time dimension), adapt _compute_index_numpy
#         # to work without the t_idx parameter.
#         raise NotImplementedError("wire _compute_index_numpy for single-image case")
#
#     def image_metadata(self, image: object) -> dict:
#         """Extract metadata from a pystac.Item — pure Python, no network call.
#
#         STAC items contain all metadata as structured JSON properties,
#         fetched upfront during the catalog search.  There is no GEE-style
#         image.toDictionary().getInfo() round-trip.
#
#         Returns a dict with the same keys as the GEE version so the
#         downstream pipeline code (build_csv_row, flags, etc.) is unchanged.
#         """
#         item = image  # for clarity
#         props = item.properties
#         return {
#             "image_id":            item.id,
#             "date":                item.datetime.date().isoformat(),
#             "tile_cloud_pct":      props.get("eo:cloud_cover"),
#             "mgrs_tile":           props.get("s2:mgrs_tile"),
#             "orbit_number":        props.get("s2:sequence"),
#             "processing_baseline": props.get("s2:processing_baseline"),
#             # aoi_cloud_pct and aoi_covered are attached by load_and_aoi_filter()
#             # and stored in item.extra_fields — not in image_metadata for MPC.
#         }
#
#     def check_aoi_coverage(self, image: object, aoi: object) -> bool:
#         """Return True if the item footprint contains the AOI polygon.
#
#         Parameters
#         ----------
#         image:
#             pystac.Item.
#         aoi:
#             shapely.Polygon (from mpc.aoi.build()).
#         """
#         from shapely.geometry import shape
#         footprint = shape(image.geometry)
#         return footprint.contains(aoi)
