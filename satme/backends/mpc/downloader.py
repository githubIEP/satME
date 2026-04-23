"""MPC GeoTIFF download stub.

Replaces: satme/downloader.py  (GEE version uses getDownloadUrl())

Key difference
──────────────
GEE: getDownloadUrl() asks GEE to render the image server-side, then
     returns a short-lived URL to a pre-rendered GeoTIFF zip.

MPC: pixels are already in an xarray.DataArray after load_and_compute_stats().
     Writing to GeoTIFF is a local rioxarray operation — no extra network call.
     The DataArray was already downloaded to compute the stats; saving it
     as GeoTIFF is essentially free (just writing bytes from RAM to disk).

This is a significant advantage of MPC for the GeoTIFF use case:
  GEE → stats = local, GeoTIFF = separate server round-trip per file
  MPC → stats = local from downloaded pixels, GeoTIFF = free (same data)
"""

# PSEUDOCODE — not executable

# import numpy as np
# import rioxarray   # pip install rioxarray
# from pathlib import Path


# def save_index_geotiff(
#     index_da,         # xarray.DataArray (y, x) — already computed index values
#     index_name,       # e.g. "NDVI"
#     image_date,       # ISO date string
#     source_name,      # e.g. "sentinel2"
#     run_name,         # run identifier
#     output_cfg,       # config["output"] block
# ) -> Path:
#     """Write a computed index DataArray to a GeoTIFF file.
#
#     Called after load_and_compute_stats() has already downloaded and
#     computed the index as an xarray.DataArray.  The pixel data is already
#     in memory — this just writes it to disk.
#
#     Equivalent to downloader.download_index_geotiff() in the GEE version,
#     but without the network round-trip (getDownloadUrl was the round-trip).
#
#     Output path matches the GEE version convention:
#       {base_dir}/{run_name}/geotiffs/{source_name}/{index_name}/{date}.tif
#     """
#     base = output_cfg.get("base_dir", "outputs/runs")
#     out_path = (
#         Path(base) / run_name / "geotiffs" / source_name / index_name
#         / f"{image_date}_{index_name}.tif"
#     )
#     out_path.parent.mkdir(parents=True, exist_ok=True)
#
#     if output_cfg.get("skip_existing", True) and out_path.exists():
#         return out_path
#
#     # Ensure the DataArray has CRS and spatial dims set (rioxarray requirement)
#     da = index_da.rio.write_crs("EPSG:4326")
#     da.rio.to_raster(str(out_path), dtype="float32", compress="deflate")
#
#     return out_path
#
#
# def save_rainfall_geotiff(
#     accumulated_da,   # xarray.DataArray (y, x) of summed precipitation
#     image_date,
#     run_name,
#     output_cfg,
#     accumulation_days=30,
# ) -> Path:
#     """Write an accumulated rainfall DataArray to a GeoTIFF file.
#
#     Equivalent to downloader.download_chirps_geotiff() in the GEE version.
#     """
#     base = output_cfg.get("base_dir", "outputs/runs")
#     out_path = (
#         Path(base) / run_name / "geotiffs" / "chirps"
#         / f"{image_date}_chirps_{accumulation_days}d_mm.tif"
#     )
#     out_path.parent.mkdir(parents=True, exist_ok=True)
#
#     if output_cfg.get("skip_existing", True) and out_path.exists():
#         return out_path
#
#     da = accumulated_da.rio.write_crs("EPSG:4326")
#     da.rio.to_raster(str(out_path), dtype="float32", compress="deflate")
#
#     return out_path
