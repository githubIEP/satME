"""Run all numerical analysis stages from cached data.

Fast (<1 min) when zarr caches are populated and precipitation is cached.

Outputs (to backend/exploration/outputs/):
    full_timeseries.csv
    corridor_summary.csv
    did_summary.json
    chirps_monthly.csv

    python -m backend.exploration.scripts.step03_run_analysis
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import xarray as xr

from .. import aggregate, cache, config, precipitation, stats


def _load_cache(name: str) -> xr.Dataset:
    ds = cache.load_cached(name)
    if ds is None:
        raise SystemExit(
            f"No valid cache for {name}. "
            f"Run step01_build_cache.py first."
        )
    return ds


def main():
    config.OUTPUTS_DIR.mkdir(exist_ok=True)
    print(f"Outputs dir: {config.OUTPUTS_DIR}")

    print("\n=== loading caches ===")
    ds_t = _load_cache(config.TREATMENT_NAME)
    ds_c = _load_cache(config.CONTROL_NAME)

    if ds_t.sizes != ds_c.sizes:
        ny = min(ds_t.sizes["y"], ds_c.sizes["y"])
        nx = min(ds_t.sizes["x"], ds_c.sizes["x"])
        print(f"AOI shape mismatch — cropping both to (y={ny}, x={nx})")
        ds_t = ds_t.isel(y=slice(0, ny), x=slice(0, nx))
        ds_c = ds_c.isel(y=slice(0, ny), x=slice(0, nx))

    print("\n=== per-scene aggregate time series ===")
    df_t = aggregate.aggregate_timeseries(ds_t, config.TREATMENT_NAME)
    df_c = aggregate.aggregate_timeseries(ds_c, config.CONTROL_NAME)
    df_all = pd.concat([df_t, df_c], ignore_index=True)
    out_csv = config.OUTPUTS_DIR / "full_timeseries.csv"
    df_all.to_csv(out_csv, index=False)
    print(f"  wrote {out_csv} ({len(df_all)} rows)")

    season = config.DRY_SEASON_MONTHS
    season_label = (
        f"dry season (months {season})" if season else "all months"
    )
    print(f"\n=== season filter: {season_label} ===")
    df_t_season = df_t[df_t["month"].isin(season)] if season else df_t
    df_c_season = df_c[df_c["month"].isin(season)] if season else df_c
    print(f"  treatment scenes in season: {len(df_t_season)}/{len(df_t)}")
    print(f"  control scenes in season:   {len(df_c_season)}/{len(df_c)}")

    print("\n=== per-pixel pre/post (dry-season only) ===")
    _, _, arr_t = aggregate.pixel_diff_map(ds_t, season_months=season)
    _, _, arr_c = aggregate.pixel_diff_map(ds_c, season_months=season)
    arr_did = arr_t - arr_c

    print("\n=== ring corridor (treatment AOI, dry-season only, combined polylines) ===")
    rows = aggregate.corridor_summary(
        ds_t, config.KML_PATHS, season_months=season,
    )
    out_corridor = config.OUTPUTS_DIR / "corridor_summary.csv"
    pd.DataFrame(rows).to_csv(out_corridor, index=False)
    print(f"  wrote {out_corridor}")
    for r in rows:
        if r["delta"] is None:
            continue
        print(f"    {r['ring']}: n={r['n_pixels']:4d}  delta={r['delta']:+.3f}")

    print("\n=== ring corridor by segment (upstream vs downstream of dam wall) ===")
    # DEM elevations: sanddam.kml (WSW arm) rises 9 m to its far end and is
    # therefore the upstream side of the dam wall. sandam-upstream.kml (NNE
    # arm) is essentially flat from the dam and is the downstream side. The
    # KML file names were assigned before the DEM check and do not match
    # the actual flow direction.
    upstream_kml = next(p for p in config.KML_PATHS if p.name == "sanddam.kml")
    downstream_kml = next(
        p for p in config.KML_PATHS if p.name == "sandam-upstream.kml"
    )
    rows_upstream = aggregate.corridor_summary(
        ds_t, [upstream_kml], season_months=season,
    )
    rows_downstream = aggregate.corridor_summary(
        ds_t, [downstream_kml], season_months=season,
    )
    print(f"  upstream of dam wall (WSW arm, file={upstream_kml.name}):")
    for r in rows_upstream:
        if r["delta"] is None:
            continue
        print(f"    {r['ring']}: n={r['n_pixels']:4d}  delta={r['delta']:+.3f}")
    print(f"  downstream of dam wall (NNE arm, file={downstream_kml.name}):")
    for r in rows_downstream:
        if r["delta"] is None:
            continue
        print(f"    {r['ring']}: n={r['n_pixels']:4d}  delta={r['delta']:+.3f}")
    pd.DataFrame(
        [{"segment": "upstream", **r} for r in rows_upstream]
        + [{"segment": "downstream", **r} for r in rows_downstream]
    ).to_csv(config.OUTPUTS_DIR / "corridor_by_segment.csv", index=False)

    print("\n=== trend tests on annual medians (dry-season scenes) ===")
    trend_t = stats.trend_one("treatment", df_t_season)
    trend_c = stats.trend_one("control", df_c_season)
    pixel_summary = stats.pixel_summary(arr_t, arr_c, arr_did)

    print("\n=== precipitation (CHIRPS via Open-Meteo / ERA5) ===")
    df_precip_daily = precipitation.load_or_fetch()
    df_precip_monthly = precipitation.to_monthly(df_precip_daily)
    out_precip = config.OUTPUTS_DIR / "chirps_monthly.csv"
    df_precip_monthly.to_csv(out_precip, index=False)
    print(f"  wrote {out_precip}")
    drought_total = df_precip_daily.loc[
        (df_precip_daily["date"] >= config.DROUGHT_START)
        & (df_precip_daily["date"] <= config.DROUGHT_END), "precip_mm"
    ].sum()
    pre_drought_total = df_precip_daily.loc[
        df_precip_daily["date"] < config.DROUGHT_START, "precip_mm"
    ].sum()
    pre_drought_years = (
        (config.DROUGHT_START - df_precip_daily["date"].min()).days / 365.25
    )
    drought_years = (
        (config.DROUGHT_END - config.DROUGHT_START).days / 365.25
    )
    print(f"  pre-drought mean rainfall:  {pre_drought_total / pre_drought_years:.1f} mm/year")
    print(f"  during-drought mean rainfall: {drought_total / drought_years:.1f} mm/year")

    summary = {
        "config": {
            "treatment": {
                "name": config.TREATMENT_NAME,
                "lat": config.TREATMENT_LAT, "lon": config.TREATMENT_LON,
            },
            "control": {
                "name": config.CONTROL_NAME,
                "lat": config.CONTROL_LAT, "lon": config.CONTROL_LON,
            },
            "half_size_m": config.HALF_SIZE_M,
            "construction_date": str(config.DAM_CONSTRUCTION_DATE.date()),
            "drought_window": [
                str(config.DROUGHT_START.date()),
                str(config.DROUGHT_END.date()),
            ],
            "ndvi_threshold": config.NDVI_VEG_THRESHOLD,
            "min_valid_fraction": config.MIN_VALID_FRACTION,
            "doy_bin_size": config.DOY_BIN_SIZE,
            "cache_version": config.CACHE_VERSION,
            "dry_season_months": season,
            "kml_paths": [str(p) for p in config.KML_PATHS],
        },
        "n_scenes": {
            "treatment_all": len(df_t),
            "treatment_dry_season": len(df_t_season),
            "control_all": len(df_c),
            "control_dry_season": len(df_c_season),
        },
        "trend": {"treatment": trend_t, "control": trend_c},
        "pixel_did": pixel_summary,
        "corridor_summary": rows,
        "corridor_by_segment": {
            "downstream": rows_downstream,
            "upstream": rows_upstream,
        },
        "precipitation": {
            "pre_drought_mm_per_year": float(pre_drought_total / pre_drought_years),
            "drought_mm_per_year": float(drought_total / drought_years),
        },
    }
    out_json = config.OUTPUTS_DIR / "did_summary.json"
    with open(out_json, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"\n  wrote {out_json}")
    print("\nDone. Run step04_render_outputs.py for figures.")


if __name__ == "__main__":
    main()
