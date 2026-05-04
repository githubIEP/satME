"""Daily precipitation at the AOI centre via Open-Meteo Historical Weather API.

Open-Meteo provides ERA5-based daily precipitation totals at any lat/lon, free
and without authentication. CHIRPS is the specialist alternative for sub-regional
spatial detail; ERA5 is sufficient for the report's drought-context overlay
because the relevant signal is the multi-month rainfall regime, not pixel-level
patches of rain.

Source paper for ERA5: Hersbach et al. (2020), "The ERA5 global reanalysis",
QJRMS, DOI 10.1002/qj.3803.
"""

from __future__ import annotations

import urllib.parse
import urllib.request
import json

import pandas as pd

from . import config

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_daily_precipitation(
    lat: float = config.TREATMENT_LAT,
    lon: float = config.TREATMENT_LON,
    start: str = f"{config.START_YEAR}-01-01",
    end: str = f"{config.END_YEAR}-12-31",
) -> pd.DataFrame:
    """Return daily precipitation totals (mm) for the AOI centre."""
    params = {
        "latitude": f"{lat}",
        "longitude": f"{lon}",
        "start_date": start,
        "end_date": end,
        "daily": "precipitation_sum",
        "timezone": "GMT",
    }
    url = f"{OPEN_METEO_URL}?{urllib.parse.urlencode(params)}"
    print(f"  fetching {url}")
    with urllib.request.urlopen(url, timeout=60) as resp:
        payload = json.loads(resp.read())
    daily = payload.get("daily")
    if not daily:
        raise SystemExit(f"No daily precipitation in Open-Meteo response: {payload}")
    df = pd.DataFrame({
        "date": pd.to_datetime(daily["time"]),
        "precip_mm": daily["precipitation_sum"],
    })
    return df


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
    config.CACHE_DIR.mkdir(exist_ok=True)
    path = config.PRECIPITATION_CSV
    if path.exists() and not force:
        df = pd.read_csv(path, parse_dates=["date"])
        print(f"  precipitation cache hit: {path.name} ({len(df)} days)")
        return df
    df = fetch_daily_precipitation()
    df.to_csv(path, index=False)
    print(f"  wrote {path} ({len(df)} days)")
    return df
