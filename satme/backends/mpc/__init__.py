"""Microsoft Planetary Computer backend — STUB / PSEUDOCODE ONLY.

This package is not yet functional.  It documents the implementation path
for replacing GEE with MPC without changing any existing pipeline code.

Dependencies that would be required (not installed)
────────────────────────────────────────────────────
    pip install planetary-computer pystac-client stackstac odc-stac rioxarray

Key MPC concepts vs GEE equivalents
─────────────────────────────────────
  GEE concept               MPC / STAC equivalent
  ───────────────────────   ───────────────────────────────────────────────
  ee.ImageCollection        list[pystac.Item]  (from pystac_client search)
  ee.Image                  xarray.DataArray   (loaded via stackstac/odc-stac)
  ee.Geometry               shapely.Polygon    (or GeoJSON dict)
  collection.map(fn)        xarray operations  (vectorised numpy/dask)
  reduceRegion(mean)        da.mean(dim=["x","y"]).compute()
  getDownloadUrl()          rioxarray .to_raster() after .compute()
  filterDate / filterBounds  pystac_client.search(datetime=..., bbox=...)
  CLOUDY_PIXEL_PERCENTAGE   STAC item property "eo:cloud_cover"
  system:index              item.id
  system:time_start         item.datetime
  aggregate_array           pandas / list comprehension after .compute()

Authentication
──────────────
MPC uses SAS (Shared Access Signature) tokens — no OAuth flow.
planetary_computer.sign(item) attaches a short-lived token to each asset URL.
Tokens expire in ~1 hour; for long runs, re-sign per batch.
"""
