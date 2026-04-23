"""MPC statistics computation stub.

Replaces: satme/stats.py  (GEE version)

Key paradigm shift
──────────────────
GEE: computation is described as a lazy graph (collection.map + reduceRegion),
     then all results are pulled in one aggregate_array.getInfo() call.
     Pixels never leave GEE — only scalar stats are returned.

MPC: pixels are downloaded as xarray DataArrays (numpy/dask arrays).
     Stats are computed locally using vectorised numpy/xarray operations.
     This means actual pixel data crosses the network, but for small AOIs
     the data volume is manageable (a 1 km² AOI at 10 m = 10,000 pixels
     per band per image ≈ 40 KB Float32).

     For large AOIs (> ~100 km²), the pixel download volume becomes the
     bottleneck — GEE's server-side computation is preferable at that scale.

Methodology is identical
────────────────────────
  Index formulas (NDVI etc.) remain the same — (B8-B4)/(B8+B4).
  Stats computed:  mean, std, min, max, percentiles [p10, p25, p50, p75, p90]
  Cloud masking:   same SCL invalid classes applied before computing stats.
  Output format:   same flat dict structure → same CSV column names.

Equivalent GEE calls → MPC equivalents
───────────────────────────────────────
  map_stats_over_collection(collection, index_names, aoi, stats_cfg, mask_fn)
    → load_and_compute_stats(items, index_names, geojson, stats_cfg)

  fetch_stats_batch(collection_with_stats, index_names, stats_cfg)
    → (stats are returned directly by load_and_compute_stats — no separate fetch)
"""

# PSEUDOCODE — not executable

# import numpy as np
# import stackstac


# ─── Index band mapping for MPC STAC assets ───────────────────────────────────
# MPC Sentinel-2 STAC uses named assets, not GEE band names.
# Map GEE band names → STAC asset keys:
#
# MPC_BAND_ASSETS = {
#     "B2":  "blue",
#     "B3":  "green",
#     "B4":  "red",
#     "B5":  "rededge",
#     "B8":  "nir",
#     "B8A": "nir08",
#     "B11": "swir16",
#     "B12": "swir22",
#     "SCL": "SCL",
# }


# def load_and_compute_stats(
#     items,          # list[pystac.Item] — clean items only
#     index_names,    # e.g. ["NDVI", "NDWI", "NDMI"]
#     geojson,        # AOI GeoJSON dict
#     stats_cfg,      # same stats config block as GEE version
#     scl_invalid=(0, 1, 3, 8, 9, 10),
#     resolution=10,  # metres; use 20 for NDMI (B8A/B11 native)
# ) -> list[dict]:
#     """Download bands, apply cloud mask, compute stats — one batch for all items.
#
#     Uses stackstac to load all items as a single 4D DataArray
#     (time × band × y × x), then applies operations over the spatial dims.
#     With dask, the actual pixel download is deferred until .compute() is called.
#
#     Returns
#     -------
#     list[dict]
#         One dict per item in the same order as items.
#         Keys: image_id + same stat column names as GEE version
#               (e.g. NDVI_mean, NDVI_std, NDVI_p50, ...).
#     """
#     # ── Determine which bands are needed for the requested indices ──────────
#     from satme.indices import REGISTRY
#     needed_gee_bands = set()
#     needed_gee_bands.add("SCL")  # always needed for cloud mask
#     for idx in index_names:
#         needed_gee_bands.update(REGISTRY[idx]["bands"])
#
#     asset_keys = [MPC_BAND_ASSETS[b] for b in needed_gee_bands]
#
#     # ── Load all items as a single 4D xarray DataArray ─────────────────────
#     # stackstac.stack loads all items at the same resolution and CRS,
#     # aligned to a common grid — equivalent to GEE's collection.map().
#     # With chunksize, computation is deferred (dask lazy).
#     da = stackstac.stack(
#         items,
#         assets=asset_keys,
#         bounds=geojson_to_bounds(geojson),
#         resolution=resolution,
#         dtype="float32",
#         fill_value=np.nan,
#         chunksize=512,
#     )
#     # da.shape = (n_images, n_bands, height, width)
#     # da.coords["time"] = image acquisition datetimes
#     # da.coords["band"] = asset key strings
#
#     # ── Clip to exact AOI polygon ───────────────────────────────────────────
#     da = da.rio.write_crs("EPSG:4326").rio.clip([geojson], crs="EPSG:4326")
#
#     # ── Build SCL-based cloud mask ──────────────────────────────────────────
#     # Same logic as cloud_mask.sentinel2_scl() — just in numpy instead of GEE.
#     scl = da.sel(band="SCL")                        # shape: (n_images, y, x)
#     valid_mask = ~np.isin(scl.values, list(scl_invalid))  # True = valid pixel
#
#     # ── Compute indices and stats per image ────────────────────────────────
#     # This is the local equivalent of GEE's collection.map(_process_image).
#     percentiles = stats_cfg.get("percentiles", [10, 25, 50, 75, 90])
#     rows = []
#
#     for t_idx, item in enumerate(items):
#         mask_2d = valid_mask[t_idx]         # (y, x) boolean array
#         stats_row = {"image_id": item.id}
#
#         for idx_name in index_names:
#             # ── Compute index (same formula as indices.py, but in numpy) ───
#             idx_vals = _compute_index_numpy(da, t_idx, idx_name)
#
#             # ── Apply cloud mask ───────────────────────────────────────────
#             masked_vals = idx_vals[mask_2d]   # 1D array of valid pixels only
#
#             if masked_vals.size == 0:
#                 # No valid pixels — fill all stats with None
#                 for stat in ["mean","std","min","max"] + [f"p{p}" for p in percentiles]:
#                     stats_row[f"{idx_name}_{stat}"] = None
#                 continue
#
#             # ── Reduce to scalars (equivalent to reduceRegion) ─────────────
#             if stats_cfg.get("include_mean", True):
#                 stats_row[f"{idx_name}_mean"] = round(float(np.nanmean(masked_vals)), 6)
#             if stats_cfg.get("include_stddev", True):
#                 stats_row[f"{idx_name}_std"] = round(float(np.nanstd(masked_vals)), 6)
#             if stats_cfg.get("include_min_max", True):
#                 stats_row[f"{idx_name}_min"] = round(float(np.nanmin(masked_vals)), 6)
#                 stats_row[f"{idx_name}_max"] = round(float(np.nanmax(masked_vals)), 6)
#             for p in percentiles:
#                 stats_row[f"{idx_name}_p{p}"] = round(
#                     float(np.nanpercentile(masked_vals, p)), 6
#                 )
#
#         rows.append(stats_row)
#
#     return rows


# ─── Index computation in numpy (mirrors satme/indices.py formulas) ───────────

# def _compute_index_numpy(da, t_idx, index_name) -> np.ndarray:
#     """Compute a spectral index for one time step as a 2D numpy array.
#
#     The formulas are identical to REGISTRY in indices.py — just operating
#     on numpy arrays instead of ee.Image server-side graphs.
#
#     Parameters
#     ----------
#     da:
#         Full 4D DataArray (time, band, y, x).
#     t_idx:
#         Index into the time dimension.
#     index_name:
#         e.g. "NDVI"
#     """
#     def band(name):
#         # Map GEE band name → STAC asset key → DataArray slice
#         asset = MPC_BAND_ASSETS[name]
#         return da.sel(band=asset).isel(time=t_idx).values.astype(np.float32)
#
#     if index_name == "NDVI":
#         nir, red = band("B8"), band("B4")
#         return (nir - red) / (nir + red + 1e-10)       # +eps avoids /0
#
#     elif index_name == "NDWI":
#         green, nir = band("B3"), band("B8")
#         return (green - nir) / (green + nir + 1e-10)
#
#     elif index_name == "NDMI":
#         nir8a, swir = band("B8A"), band("B11")
#         return (nir8a - swir) / (nir8a + swir + 1e-10)
#
#     elif index_name == "EVI":
#         nir, red, blue = band("B8"), band("B4"), band("B2")
#         return 2.5 * (nir - red) / (nir + 6 * red - 7.5 * blue + 1 + 1e-10)
#
#     elif index_name == "SAVI":
#         nir, red = band("B8"), band("B4")
#         return 1.5 * (nir - red) / (nir + red + 0.5 + 1e-10)
#
#     elif index_name == "MNDWI":
#         green, swir = band("B3"), band("B11")
#         return (green - swir) / (green + swir + 1e-10)
#
#     elif index_name == "NDBI":
#         swir, nir = band("B11"), band("B8")
#         return (swir - nir) / (swir + nir + 1e-10)
#
#     elif index_name == "BSI":
#         swir, red, nir, blue = band("B11"), band("B4"), band("B8"), band("B2")
#         return ((swir + red) - (nir + blue)) / ((swir + red) + (nir + blue) + 1e-10)
#
#     elif index_name == "GNDVI":
#         nir, green = band("B8"), band("B3")
#         return (nir - green) / (nir + green + 1e-10)
#
#     else:
#         raise KeyError(f"Index '{index_name}' not implemented in MPC numpy path.")
