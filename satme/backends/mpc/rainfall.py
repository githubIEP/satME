"""MPC / STAC rainfall source stub.

Replaces: satme/sources/chirps.py  (GEE version)

CHIRPS on MPC
─────────────
CHIRPS daily data IS available on MPC at collection ID "chirps-gpm".
As of 2024 it covers 2000–present at 0.05° (~5.5 km), same as the GEE
version (UCSB-CHG/CHIRPS/DAILY).  Coverage 1981–1999 is GEE-only.

Alternative: download CHIRPS directly from the UCSB FTP / HTTP server
as GeoTIFF without any cloud platform:
  https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05/

For the accumulation logic (sum over N days), both approaches are equivalent
— the difference is just where the data is fetched from.

Equivalent GEE calls → MPC equivalents
───────────────────────────────────────
  chirps_source.batch_rainfall_scalars(dates, geometry)
    → batch_rainfall_mpc(dates, geojson, accumulation_days)

The methodology is identical:
  1. For each image date, select the preceding N daily rasters
  2. Sum them (accumulation)
  3. Compute mean over the AOI polygon
  Result: one float per date in mm
"""

# PSEUDOCODE — not executable

# import numpy as np
# import requests                # for direct UCSB HTTP download
# import rasterio
# from io import BytesIO
# from datetime import timedelta
# from shapely.geometry import mapping


# ─── Option A: via MPC STAC ───────────────────────────────────────────────────

# MPC_CHIRPS_COLLECTION = "chirps-gpm"

# def batch_rainfall_mpc_stac(
#     image_dates,         # list[datetime.date]
#     geojson,             # AOI as GeoJSON dict
#     catalog,             # pystac_client.Client
#     accumulation_days=30,
# ) -> list:               # list[float | None]
#     """Fetch accumulated rainfall for all dates via MPC STAC.
#
#     For each image date:
#       1. Search CHIRPS collection for (date-N) to (date)
#       2. Load precipitation band as DataArray
#       3. Sum over time dimension (accumulation)
#       4. Clip to AOI, compute mean
#
#     Returns one float per date, same order as image_dates.
#     Methodology identical to chirps.batch_rainfall_scalars() in GEE version.
#     """
#     results = []
#     for img_date in image_dates:
#         start_date = img_date - timedelta(days=accumulation_days)
#         items = list(catalog.search(
#             collections=[MPC_CHIRPS_COLLECTION],
#             bbox=list(shapely_from_geojson(geojson).bounds),
#             datetime=f"{start_date}/{img_date}",
#         ).items())
#
#         if not items:
#             results.append(None)
#             continue
#
#         import stackstac
#         da = stackstac.stack(items, assets=["precipitation"],
#                              resolution=0.05, dtype="float32")
#         accumulated = da.sum(dim="time").squeeze()       # sum over N days
#         clipped = accumulated.rio.clip([geojson], crs="EPSG:4326")
#         mean_val = float(np.nanmean(clipped.values))
#         results.append(round(mean_val, 3) if not np.isnan(mean_val) else None)
#
#     return results


# ─── Option B: direct UCSB HTTP download (no cloud platform dependency) ───────

# CHIRPS_BASE_URL = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05"

# def batch_rainfall_ucsb(
#     image_dates,
#     geojson,
#     accumulation_days=30,
# ) -> list:
#     """Fetch accumulated rainfall directly from UCSB CHIRPS HTTP server.
#
#     No MPC account required — CHIRPS data is publicly available.
#     Slower than MPC STAC for many dates (one HTTP request per day),
#     but completely free and independent of any cloud platform.
#
#     URL pattern:
#       {BASE_URL}/{year}/chirps-v2.0.{year}.{month:02d}.{day:02d}.tif.gz
#     """
#     from shapely.geometry import shape
#     aoi_shape = shape(geojson)
#     results = []
#
#     for img_date in image_dates:
#         total_mm = 0.0
#         days_found = 0
#         for delta in range(accumulation_days):
#             d = img_date - timedelta(days=delta)
#             url = f"{CHIRPS_BASE_URL}/{d.year}/chirps-v2.0.{d.strftime('%Y.%m.%d')}.tif.gz"
#             try:
#                 resp = requests.get(url, timeout=30)
#                 resp.raise_for_status()
#                 with rasterio.open(BytesIO(resp.content)) as src:
#                     # Read pixel values within the AOI bounding box
#                     window = src.window(*aoi_shape.bounds)
#                     data = src.read(1, window=window).astype(float)
#                     data[data == src.nodata] = np.nan
#                     mean_for_day = np.nanmean(data)
#                     if not np.isnan(mean_for_day):
#                         total_mm += mean_for_day
#                         days_found += 1
#             except Exception:
#                 pass   # missing day — skip
#
#         results.append(round(total_mm, 3) if days_found > 0 else None)
#
#     return results
