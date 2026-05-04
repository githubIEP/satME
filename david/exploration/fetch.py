"""STAC search and two-pass cache build for per-AOI Sentinel-2 zarr stores."""

from __future__ import annotations

import dask
import numpy as np
import odc.stac
import pandas as pd
import planetary_computer
import pystac_client
import xarray as xr
from dask.diagnostics import ProgressBar

from . import cache, config, indices


def make_bbox(lat: float, lon: float, half_m: float) -> tuple:
    half_lat = half_m / 111_320
    half_lon = half_m / (111_320 * np.cos(np.deg2rad(lat)))
    return (lon - half_lon, lat - half_lat, lon + half_lon, lat + half_lat)


def open_catalog():
    return pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )


def search_per_year(catalog, bbox, year_start: int, year_end: int):
    """Year-chunked STAC search with 3 retries per year."""
    items = []
    for year in range(year_start, year_end + 1):
        for attempt in range(3):
            try:
                yr = list(
                    catalog.search(
                        collections=["sentinel-2-l2a"],
                        bbox=bbox,
                        datetime=f"{year}-01-01/{year}-12-31",
                        query={"eo:cloud_cover": {"lt": config.MAX_SCENE_CLOUD}},
                    ).items()
                )
                items.extend(yr)
                print(f"    {year}: {len(yr)} candidate scenes")
                break
            except Exception as e:
                print(f"    {year} attempt {attempt + 1} failed: {e}")
        else:
            print(f"    {year}: gave up after 3 attempts")
    return items


def build_per_scene_cache(name: str, lat: float, lon: float, catalog) -> xr.Dataset:
    """Two-pass build: SCL filter, then full bands for kept scenes. Writes zarr."""
    print(f"\n--- building cache for {name} ({lat}, {lon}) ---")
    bbox = make_bbox(lat, lon, config.HALF_SIZE_M)
    print(f"  bbox: {bbox}")

    print("  STAC search (year-chunked)...")
    items = search_per_year(catalog, bbox, config.START_YEAR, config.END_YEAR)
    if not items:
        raise SystemExit(f"No scenes found for {name}")
    print(f"  {len(items)} total candidates")

    # Pass 1: SCL only, compute valid_fraction, filter
    print("  pass 1: loading SCL to compute clear-pixel fraction...")
    data_scl = odc.stac.load(
        items,
        bands=["SCL"],
        bbox=bbox,
        resolution=10,
        chunks={"time": 16, "x": 128, "y": 128},
        groupby="solar_day",
    )
    valid_p1 = indices.valid_mask(data_scl["SCL"])
    total_pixels = valid_p1.sizes["x"] * valid_p1.sizes["y"]
    with ProgressBar():
        valid_n_p1 = valid_p1.sum(dim=["x", "y"]).compute()
    valid_frac_p1 = valid_n_p1.values / total_pixels

    keep_times = data_scl.time.values[valid_frac_p1 >= config.MIN_VALID_FRACTION]
    print(
        f"  {len(keep_times)}/{len(data_scl.time)} scenes "
        f">= {config.MIN_VALID_FRACTION * 100:.0f}% clear"
    )
    if len(keep_times) == 0:
        raise SystemExit(f"No scenes passed cloud filter for {name}")

    # Pass 2: full bands for kept items
    keep_dates = {pd.Timestamp(t).date().isoformat() for t in keep_times}
    kept_items = [
        it for it in items
        if pd.Timestamp(it.datetime).date().isoformat() in keep_dates
    ]
    print(f"  pass 2: re-signing {len(kept_items)} items ...")
    kept_items = [planetary_computer.sign(it) for it in kept_items]

    print(f"  pass 2: loading {len(config.BANDS)} bands for {len(kept_items)} scenes...")
    data = odc.stac.load(
        kept_items,
        bands=config.BANDS,
        bbox=bbox,
        resolution=10,
        chunks={"time": 8, "x": 128, "y": 128},
        groupby="solar_day",
    )

    valid = indices.valid_mask(data["SCL"])
    valid_n = valid.sum(dim=["x", "y"])
    valid_frac = valid_n / total_pixels

    green = (data["B03"].where(valid) / 10000.0).astype("float32")
    red = (data["B04"].where(valid) / 10000.0).astype("float32")
    nir = (data["B08"].where(valid) / 10000.0).astype("float32")
    swir = (data["B11"].where(valid) / 10000.0).astype("float32")

    ndvi_da = indices.ndvi(nir, red)
    mndwi_da = indices.mndwi(green, swir)

    times = pd.to_datetime(data.time.values)
    doy = np.asarray(times.dayofyear)
    bin_idx = np.minimum((doy - 1) // config.DOY_BIN_SIZE, config.N_DOY_BINS - 1).astype("int8")

    times_arr = times.to_numpy()
    period = np.where(
        times_arr < np.datetime64(config.DAM_CONSTRUCTION_DATE), "pre",
        np.where(
            (np.asarray(times.year) == config.DAM_CONSTRUCTION_DATE.year)
            & (np.asarray(times.month) == config.DAM_CONSTRUCTION_DATE.month),
            "build", "post",
        ),
    )
    is_drought = (
        (times_arr >= np.datetime64(config.DROUGHT_START))
        & (times_arr <= np.datetime64(config.DROUGHT_END))
    )

    out = xr.Dataset(
        {
            "ndvi": ndvi_da,
            "mndwi": mndwi_da,
            "valid": valid,
            "valid_fraction": ("time", valid_frac.values),
            "doy": ("time", doy),
            "doy_bin": ("time", bin_idx),
            "period": ("time", period.astype("U5")),
            "is_drought": ("time", is_drought),
        },
        attrs={
            "cache_version": config.CACHE_VERSION,
            "aoi_name": name,
            "aoi_center_lat": lat,
            "aoi_center_lon": lon,
            "min_valid_fraction": config.MIN_VALID_FRACTION,
            "ndvi_threshold": config.NDVI_VEG_THRESHOLD,
            "mndwi_threshold": config.MNDWI_WATER_THRESHOLD,
        },
    )

    cache.write_zarr_atomic(out, name)
    return xr.open_zarr(cache.cache_path_for(name), consolidated=False)
