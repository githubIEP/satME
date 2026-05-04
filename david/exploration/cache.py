"""Zarr cache utilities for the per-scene NDVI/MNDWI dataset."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import xarray as xr

from . import config


def cache_path_for(name: str) -> Path:
    return config.CACHE_DIR / f"{name}_v{config.CACHE_VERSION}.zarr"


def find_cache(name_prefix: str) -> Path:
    """Locate the most recent cache for an AOI prefix; raises if none exist."""
    candidates = list(config.CACHE_DIR.glob(f"{name_prefix}_v*.zarr"))
    if not candidates:
        raise SystemExit(
            f"No {name_prefix} cache in {config.CACHE_DIR}. "
            f"Run scripts/step01_build_cache.py first."
        )
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def load_cached(name: str) -> xr.Dataset | None:
    """Open a cached zarr if it exists and version-matches; else None."""
    path = cache_path_for(name)
    if not path.exists():
        return None
    try:
        ds = xr.open_zarr(path, consolidated=False)
        if ds.attrs.get("cache_version") != config.CACHE_VERSION:
            print(f"  cache version mismatch for {name} — rebuild required")
            return None
        time_min = pd.Timestamp(ds.time.min().item())
        time_max = pd.Timestamp(ds.time.max().item())
        if time_min.year > config.START_YEAR or time_max.year < config.END_YEAR:
            print(
                f"  cache time-range insufficient for {name} "
                f"({time_min.year}..{time_max.year}) — rebuild required"
            )
            return None
        print(f"  cache hit: {name} ({len(ds.time)} scenes)")
        return ds
    except Exception as e:
        print(f"  cache read failed for {name}: {e} — rebuild required")
        return None


def write_zarr_atomic(ds: xr.Dataset, name: str) -> Path:
    """Write a dataset to the canonical cache path via tmp + rename."""
    config.CACHE_DIR.mkdir(exist_ok=True)
    final_path = cache_path_for(name)
    tmp_path = final_path.with_suffix(".zarr.tmp")
    if tmp_path.exists():
        shutil.rmtree(tmp_path)
    print(f"  writing zarr to {tmp_path} ...")
    ds.to_zarr(tmp_path, mode="w")
    if final_path.exists():
        shutil.rmtree(final_path)
    tmp_path.rename(final_path)
    print(f"  cache written: {final_path}")
    return final_path
