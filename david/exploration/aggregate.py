"""Per-pixel pre/post aggregation, DOY binning, ring-buffer corridor analysis."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from pyproj import Transformer
from rasterio.features import geometry_mask
from shapely.geometry import LineString, MultiLineString
from shapely.ops import transform as shapely_transform

import rioxarray  # noqa: F401  registers .rio accessor

from . import config


# ---------- season filter ----------
def filter_to_season(ds: xr.Dataset, season_months: list[int] | None) -> xr.Dataset:
    """Restrict a cached dataset to scenes whose month is in season_months.

    Returns the dataset unchanged if season_months is None.
    """
    if season_months is None:
        return ds
    times = pd.to_datetime(ds.time.values)
    mask = np.isin(times.month, season_months)
    return ds.isel(time=mask)


# ---------- per-pixel pre/post fractions ----------
def per_pixel_pre_post(
    ds: xr.Dataset,
    season_months: list[int] | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (pre_frac, post_frac, diff) arrays, each (y, x) float."""
    ds = filter_to_season(ds, season_months)
    period = ds["period"].values
    valid = ds["valid"]
    veg = (ds["ndvi"] > config.NDVI_VEG_THRESHOLD) & valid

    pre_sel = (period == "pre")
    post_sel = (period == "post")

    pre_n = valid.isel(time=pre_sel).sum(dim="time").compute()
    pre_v = veg.isel(time=pre_sel).sum(dim="time").compute()
    post_n = valid.isel(time=post_sel).sum(dim="time").compute()
    post_v = veg.isel(time=post_sel).sum(dim="time").compute()

    pre_frac = (pre_v / pre_n).where(pre_n > 0).values
    post_frac = (post_v / post_n).where(post_n > 0).values
    return pre_frac, post_frac, post_frac - pre_frac


def compute_doy_pixel_fraction(
    ds: xr.Dataset,
    period_label: str,
    season_months: list[int] | None = None,
) -> xr.DataArray:
    """Per-pixel-per-DOY-bin fraction of clear scenes where NDVI > threshold."""
    ds = filter_to_season(ds, season_months)
    period = ds["period"].values
    sel = (period == period_label)
    if not sel.any():
        ny, nx = ds.sizes["y"], ds.sizes["x"]
        return xr.DataArray(
            np.full((config.N_DOY_BINS, ny, nx), np.nan, dtype="float32"),
            dims=("doy_bin", "y", "x"),
        )

    sub = ds.isel(time=sel)
    valid = sub["valid"]
    veg = (sub["ndvi"] > config.NDVI_VEG_THRESHOLD) & valid

    doy_bin = sub["doy_bin"].load()
    veg_grp = veg.groupby(doy_bin).sum(dim="time")
    val_grp = valid.groupby(doy_bin).sum(dim="time")

    bin_full = np.arange(config.N_DOY_BINS, dtype="int8")
    veg_grp = veg_grp.reindex(doy_bin=bin_full, fill_value=0)
    val_grp = val_grp.reindex(doy_bin=bin_full, fill_value=0)

    with np.errstate(divide="ignore", invalid="ignore"):
        frac = veg_grp.astype("float32") / val_grp.astype("float32")
    return frac.where(val_grp > 0)


def pixel_diff_map(
    ds: xr.Dataset,
    season_months: list[int] | None = None,
) -> tuple[xr.DataArray, xr.DataArray, np.ndarray]:
    """Per-pixel post-pre averaged across DOY bins, masked where bins<MIN_BINS."""
    pre = compute_doy_pixel_fraction(ds, "pre", season_months=season_months)
    post = compute_doy_pixel_fraction(ds, "post", season_months=season_months)
    diff_per_bin = post - pre
    pixel_diff = diff_per_bin.mean(dim="doy_bin", skipna=True)
    n_bins = diff_per_bin.notnull().sum(dim="doy_bin")

    arr = pixel_diff.values.copy()
    arr[n_bins.values < config.MIN_BINS_REQUIRED] = np.nan
    return pixel_diff, n_bins, arr


# ---------- per-scene aggregate ----------
def aggregate_timeseries(ds: xr.Dataset, name: str) -> pd.DataFrame:
    """One row per kept scene with vegetation/water aggregates."""
    n_total = ds.sizes["y"] * ds.sizes["x"]
    valid_n = ds["valid"].sum(dim=["x", "y"]).values
    veg_count = (
        (ds["ndvi"] > config.NDVI_VEG_THRESHOLD) & ds["valid"]
    ).sum(dim=["x", "y"]).values
    water_count = (
        (ds["mndwi"] > config.MNDWI_WATER_THRESHOLD) & ds["valid"]
    ).sum(dim=["x", "y"]).values
    mean_ndvi = ds["ndvi"].mean(dim=["x", "y"], skipna=True).values

    times = pd.to_datetime(ds.time.values)
    df = pd.DataFrame({
        "aoi": name,
        "date": times,
        "doy": times.dayofyear,
        "month": times.month,
        "year": times.year,
        "period": ds["period"].values,
        "is_drought": ds["is_drought"].values,
        "valid_fraction": valid_n / n_total,
        "vegetation_fraction": np.where(valid_n > 0, veg_count / valid_n, np.nan),
        "vegetation_ha": veg_count * config.PIXEL_AREA_HA,
        "water_fraction": np.where(valid_n > 0, water_count / valid_n, np.nan),
        "water_ha": water_count * config.PIXEL_AREA_HA,
        "mean_ndvi": mean_ndvi,
    })
    return df


# ---------- ring buffers ----------
def parse_kml_linestring(kml_path: Path) -> list[tuple[float, float]]:
    ns = {"kml": "http://www.opengis.net/kml/2.2"}
    coords_elem = ET.parse(kml_path).getroot().find(
        ".//kml:LineString/kml:coordinates", ns)
    if coords_elem is None:
        raise ValueError(f"No LineString in {kml_path}")
    pts = []
    for triple in coords_elem.text.split():
        lon, lat, *_ = (float(v) for v in triple.split(","))
        pts.append((lon, lat))
    return pts


def parse_kml_geometry(kml_paths) -> LineString | MultiLineString:
    """Parse one or more KML files into a single shapely geometry.

    A single path returns a LineString; multiple paths return a MultiLineString
    so that buffer / difference operations correctly union all segments.
    """
    if not isinstance(kml_paths, (list, tuple)):
        kml_paths = [kml_paths]
    lines = [LineString(parse_kml_linestring(Path(p))) for p in kml_paths]
    if len(lines) == 1:
        return lines[0]
    return MultiLineString(lines)


def get_cache_crs(ds: xr.Dataset):
    if hasattr(ds, "rio") and ds.rio.crs is not None:
        return ds.rio.crs
    if "spatial_ref" in ds.coords or "spatial_ref" in ds.data_vars:
        sr = (
            ds.coords["spatial_ref"]
            if "spatial_ref" in ds.coords
            else ds["spatial_ref"]
        )
        wkt = sr.attrs.get("crs_wkt") or sr.attrs.get("spatial_ref")
        if wkt:
            from rasterio.crs import CRS
            return CRS.from_wkt(wkt)
    raise SystemExit("Cannot determine CRS from cache.")


def project_geometry(geom, crs):
    """Reproject a shapely LineString or MultiLineString from WGS84 to crs."""
    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    return shapely_transform(transformer.transform, geom)


def build_ring_masks(geom_utm, transform, out_shape, ring_bounds):
    """Build concentric ring masks around any shapely geometry that supports buffer."""
    rings = []
    for inner_d, outer_d in zip(ring_bounds[:-1], ring_bounds[1:]):
        outer_geom = geom_utm.buffer(outer_d)
        if inner_d > 0:
            ring_geom = outer_geom.difference(geom_utm.buffer(inner_d))
        else:
            ring_geom = outer_geom
        mask = ~geometry_mask(
            [ring_geom], transform=transform, out_shape=out_shape, invert=False
        )
        rings.append((f"{inner_d}-{outer_d}m", mask, ring_geom))
    return rings


def corridor_summary(
    ds: xr.Dataset,
    kml_paths,
    season_months: list[int] | None = None,
) -> list[dict]:
    """Return per-ring rows with pre/post means and delta for the treatment AOI.

    kml_paths may be a single path or a list of paths; multiple paths are
    combined as a MultiLineString so all segments contribute to the buffers.
    """
    geom_wgs = parse_kml_geometry(kml_paths)
    crs = get_cache_crs(ds)
    geom_utm = project_geometry(geom_wgs, crs)
    transform = ds.rio.transform()
    out_shape = (ds.sizes["y"], ds.sizes["x"])

    rings = build_ring_masks(geom_utm, transform, out_shape, config.RING_BOUNDS_M)
    pre_frac, post_frac, diff = per_pixel_pre_post(ds, season_months=season_months)

    rows = []
    for label, mask, _ in rings:
        valid_in_ring = mask & np.isfinite(diff)
        n_px = int(valid_in_ring.sum())
        if n_px == 0:
            rows.append({"ring": label, "n_pixels": 0,
                         "pre_mean": None, "post_mean": None, "delta": None})
            continue
        rows.append({
            "ring": label,
            "n_pixels": n_px,
            "pre_mean": float(np.nanmean(pre_frac[valid_in_ring])),
            "post_mean": float(np.nanmean(post_frac[valid_in_ring])),
            "delta": float(np.nanmean(diff[valid_in_ring])),
        })
    return rows


def yearly_median_ndvi(
    ds: xr.Dataset,
    season_months: list[int] | None = None,
) -> dict[int, np.ndarray]:
    """Return {year: median_ndvi_2d_array} for every year in the cache.

    If season_months is given, only scenes within those calendar months are
    used to compute the per-year median (e.g. dry-season median).
    """
    ds = filter_to_season(ds, season_months)
    times = pd.to_datetime(ds.time.values)
    years = sorted(set(times.year))
    out = {}
    for y in years:
        sel = (times.year == y)
        sub = ds["ndvi"].isel(time=sel)
        if sub.sizes["time"] == 0:
            continue
        out[y] = sub.median(dim="time", skipna=True).compute().values
    return out
