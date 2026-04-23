"""MPC collection filtering and metadata stub.

Replaces: satme/image_filter.py  (GEE version)

Key paradigm shift
──────────────────
GEE:  collection.map(fn) runs server-side; no pixels leave GEE until
      getDownloadUrl() is called.  Cloud % is computed by reducing the
      SCL band over the AOI — still server-side, no local pixel transfer.

MPC:  STAC search returns item metadata only (no pixels).  Cloud %
      filtering at search time uses the item property "eo:cloud_cover"
      (tile-level, same as CLOUDY_PIXEL_PERCENTAGE in GEE).
      AOI-level cloud % requires loading the SCL band as an xarray
      DataArray and computing locally — a genuine pixel download.

      This is the biggest performance difference: GEE's AOI cloud %
      pre-filter is free (server-side); MPC's equivalent costs a
      small raster download per image for the SCL band.

      Mitigation: use the STAC tile-cloud property as a proxy first
      (same as our max_tile_cloud_pct filter), then load SCL only for
      images that pass the tile-level threshold.  For small AOIs the
      SCL download is tiny (~few KB at 20 m).

Equivalent GEE calls → MPC equivalents
───────────────────────────────────────
  prefilter_by_aoi_cloud(collection, aoi, threshold)
    → search_and_filter(catalog, source, bbox, date_range, tile_threshold)
       then load_and_aoi_filter(items, geojson, aoi_threshold)

  batch_image_metadata(collection)
    → items_to_metadata(items)   — pure Python, no network call
"""

# PSEUDOCODE — not executable

# import numpy as np
# import stackstac          # or: import odc.stac
# import planetary_computer


# ─── Step 1: STAC search (replaces get_collection + tile-cloud filter) ────────

# def search_collection(
#     catalog,           # pystac_client.Client from mpc.auth.get_catalog()
#     collection_id,     # e.g. "sentinel-2-l2a"
#     bbox,              # [west, south, east, north] from aoi_meta["bbox"]
#     date_range,        # {"start": "2016-01-01", "end": "2024-12-31"}
#     max_tile_cloud_pct=100,
# ) -> list:             # list[pystac.Item]
#     """Search MPC STAC catalog — equivalent to GEE get_collection().
#
#     Returns STAC items (metadata only — no pixels loaded yet).
#     Items are pre-signed so asset URLs are ready to use.
#
#     STAC property used for tile-cloud filter: "eo:cloud_cover"
#     (0–100 float, same semantics as CLOUDY_PIXEL_PERCENTAGE in GEE).
#     """
#     search = catalog.search(
#         collections=[collection_id],
#         bbox=bbox,
#         datetime=f"{date_range['start']}/{date_range['end']}",
#         query={"eo:cloud_cover": {"lte": max_tile_cloud_pct}},
#     )
#     items = list(search.items())
#     return items


# ─── Step 2: AOI-level cloud filter (replaces prefilter_by_aoi_cloud) ─────────

# def load_and_aoi_filter(
#     items,                  # list[pystac.Item] from search_collection()
#     geojson,                # AOI as GeoJSON dict (from aoi_meta["geojson"])
#     max_aoi_cloud_pct=100,
#     scl_invalid_classes=(0, 1, 3, 8, 9, 10),
# ) -> tuple:                 # (clean_items, all_items_with_cloud_pct)
#     """Compute AOI-level cloud % for each item and filter.
#
#     Unlike GEE (fully server-side), this loads the SCL band as a small
#     raster for each item.  For a 1 km² AOI at 20 m, the SCL tile is
#     ~50×50 px — roughly 2.5 KB per image, trivial bandwidth.
#
#     Methodology is identical to prefilter_by_aoi_cloud in image_filter.py:
#       valid_mask = SCL not in scl_invalid_classes   (binary 0/1 raster)
#       aoi_cloud_pct = (1 - mean(valid_mask)) * 100
#     The arithmetic is the same; it just runs in numpy instead of GEE.
#
#     Returns
#     -------
#     (clean_items, annotated_items)
#         clean_items     : items with aoi_cloud_pct <= threshold
#         annotated_items : all items with aoi_cloud_pct attached as
#                           a plain Python float in item.extra_fields
#     """
#     annotated = []
#     for item in items:
#         # Load ONLY the SCL band clipped to the AOI bbox
#         # stackstac.stack returns xarray.DataArray (bands × time × y × x)
#         da = stackstac.stack(
#             [item],
#             assets=["SCL"],
#             bounds=geojson_to_bounds(geojson),
#             resolution=20,
#             dtype="uint8",
#         ).squeeze("time")   # → (y, x) after squeezing single time step
#
#         # Clip to exact AOI polygon (not just the bounding box)
#         scl = da.rio.write_crs("EPSG:4326").rio.clip([geojson], crs="EPSG:4326")
#
#         # Binary valid-pixel mask — same logic as sentinel2_scl() + prefilter
#         valid_mask = ~np.isin(scl.values, list(scl_invalid_classes))
#         n_total = valid_mask.size
#         if n_total == 0:
#             aoi_cloud_pct = 100.0
#             aoi_covered = False
#         else:
#             valid_fraction = valid_mask.sum() / n_total
#             aoi_cloud_pct = round((1 - float(valid_fraction)) * 100, 2)
#             # Coverage: True if any valid pixels exist in the AOI
#             # (for exact footprint check, compare item footprint geometry
#             #  to the AOI polygon using shapely.contains())
#             aoi_covered = item_footprint_contains_aoi(item, geojson)
#
#         # Attach as extra_fields so downstream code can read them
#         item.extra_fields["aoi_cloud_pct"] = aoi_cloud_pct
#         item.extra_fields["aoi_covered"] = aoi_covered
#         annotated.append(item)
#
#     clean = [it for it in annotated
#              if it.extra_fields["aoi_cloud_pct"] <= max_aoi_cloud_pct]
#     return clean, annotated


# ─── Step 3: Batch metadata (replaces batch_image_metadata) ───────────────────

# def items_to_metadata(items) -> list[dict]:
#     """Convert STAC items to metadata dicts — pure Python, zero network calls.
#
#     STAC items already contain all metadata (date, cloud %, tile ID, etc.)
#     as JSON properties fetched during the search.  No separate round-trip
#     is needed — this is free compared to GEE's aggregate_array call.
#
#     Equivalent to batch_image_metadata() in image_filter.py.
#     """
#     rows = []
#     for item in items:
#         props = item.properties
#         rows.append({
#             "image_id":            item.id,
#             "date":                item.datetime.date().isoformat(),
#             "tile_cloud_pct":      props.get("eo:cloud_cover"),
#             "aoi_cloud_pct":       item.extra_fields.get("aoi_cloud_pct"),
#             "aoi_covered":         item.extra_fields.get("aoi_covered", False),
#             "mgrs_tile":           props.get("s2:mgrs_tile"),
#             "orbit_number":        props.get("s2:sequence"),
#             "processing_baseline": props.get("s2:processing_baseline"),
#         })
#     return rows


# ─── Helper ───────────────────────────────────────────────────────────────────

# def item_footprint_contains_aoi(item, geojson) -> bool:
#     """Return True if the item's geometry fully contains the AOI polygon."""
#     from shapely.geometry import shape
#     item_shape = shape(item.geometry)
#     aoi_shape  = shape(geojson)
#     return item_shape.contains(aoi_shape)
#
#
# def geojson_to_bounds(geojson) -> tuple:
#     """Convert GeoJSON polygon to (west, south, east, north) tuple."""
#     from shapely.geometry import shape
#     return shape(geojson).bounds   # (minx, miny, maxx, maxy)
