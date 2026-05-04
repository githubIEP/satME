"""Fetch daily precipitation at the AOI centre from Open-Meteo (ERA5).

Cached to backend/_cache_fulltimeseries/precipitation_daily.csv.

    python -m backend.exploration.scripts.step02_fetch_precipitation
    python -m backend.exploration.scripts.step02_fetch_precipitation --force
"""

from __future__ import annotations

import argparse

from .. import precipitation


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--force", action="store_true",
                   help="ignore cache and refetch")
    args = p.parse_args()

    df = precipitation.load_or_fetch(force=args.force)
    monthly = precipitation.to_monthly(df)
    print(f"\nDaily rows:   {len(df)}")
    print(f"Monthly rows: {len(monthly)}")
    print(f"\nFirst 5 monthly:\n{monthly.head()}")
    print(f"Last 5 monthly:\n{monthly.tail()}")


if __name__ == "__main__":
    main()
