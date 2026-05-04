"""NDVI, MNDWI, valid-pixel mask helpers for Sentinel-2 L2A surface reflectance."""

from __future__ import annotations

import xarray as xr

from . import config


def valid_mask(scl: xr.DataArray) -> xr.DataArray:
    """Boolean mask: True where SCL pixel is not in the bad-class set."""
    return ~scl.isin(config.BAD_SCL_CLASSES)


def ndvi(b08: xr.DataArray, b04: xr.DataArray) -> xr.DataArray:
    """(NIR − Red) / (NIR + Red), float32."""
    return ((b08 - b04) / (b08 + b04)).astype("float32")


def mndwi(b03: xr.DataArray, b11: xr.DataArray) -> xr.DataArray:
    """(Green − SWIR) / (Green + SWIR), float32."""
    return ((b03 - b11) / (b03 + b11)).astype("float32")
