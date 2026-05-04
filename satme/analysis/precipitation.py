"""Daily precipitation at the AOI centre via CHIRPS, fetched through GEE.

Replaces the Open-Meteo / ERA5 fetch used in the original david/exploration
study so the analysis layer only depends on a single data backend (GEE).

Same public API as david/exploration/precipitation.py: ``fetch_daily_precipitation``,
``to_monthly``, ``load_or_fetch``.
"""

from __future__ import annotations

import logging

import ee
import pandas as pd

from . import _runtime_config as config

logger = logging.getLogger(__name__)

_COLLECTION = "UCSB-CHG/CHIRPS/DAILY"
_BAND = "precipitation"
_NATIVE_SCALE_M = 5566  # ~0.05° at the equator


def fetch_daily_precipitation(
    lat: float | None = None,
    lon: float | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Daily CHIRPS rainfall at the AOI centre point, via GEE.getRegion.

    Defaults pull from ``_runtime_config`` so callers don't need to thread
    the config dict through.
    """
    lat   = config.TREATMENT_LAT if lat is None else lat
    lon   = config.TREATMENT_LON if lon is None else lon
    start = f"{config.START_YEAR}-01-01" if start is None else start
    end   = f"{config.END_YEAR}-12-31"   if end   is None else end

    point = ee.Geometry.Point([lon, lat])
    col = (
        ee.ImageCollection(_COLLECTION)
        .filterBounds(point)
        .filterDate(start, end)
        .select(_BAND)
    )
    print(f"  fetching CHIRPS daily for ({lat}, {lon}) {start} → {end}")
    raw = col.getRegion(point, scale=_NATIVE_SCALE_M).getInfo()
    if not raw or len(raw) < 2:
        raise SystemExit(f"CHIRPS returned no rows for ({lat}, {lon})")

    header, *rows = raw
    df = pd.DataFrame(rows, columns=header)
    df["date"] = pd.to_datetime(df["time"], unit="ms").dt.normalize()
    df = df.rename(columns={_BAND: "precip_mm"})[["date", "precip_mm"]]
    df["precip_mm"] = df["precip_mm"].fillna(0.0).astype("float32")
    return df.sort_values("date").reset_index(drop=True)


def to_monthly(df_daily: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily precipitation to monthly totals."""
    df = df_daily.copy()
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    monthly = (
        df.groupby("month", as_index=False)["precip_mm"]
        .sum()
        .rename(columns={"month": "date"})
    )
    return monthly


def load_or_fetch(force: bool = False) -> pd.DataFrame:
    """Cached daily precipitation; refetch if file missing or force=True."""
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = config.PRECIPITATION_CSV
    if path.exists() and not force:
        df = pd.read_csv(path, parse_dates=["date"])
        print(f"  precipitation cache hit: {path.name} ({len(df)} days)")
        return df
    df = fetch_daily_precipitation()
    df.to_csv(path, index=False)
    print(f"  wrote {path} ({len(df)} days)")
    return df
