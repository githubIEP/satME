"""Render all figures from cached data and outputs/full_timeseries.csv.

Produces (in backend/exploration/outputs/):
    site_methodology.png
    yearly_ndvi_grid.png
    timeseries_with_precip.png
    per_pixel_change_treatment.png
    per_pixel_change_control.png
    per_pixel_did.png
    corridor_decay.png

    python -m backend.exploration.scripts.step04_render_outputs
"""

from __future__ import annotations

import pandas as pd

from .. import aggregate, cache, config, precipitation, visualisation


def _load_cache(name: str):
    ds = cache.load_cached(name)
    if ds is None:
        raise SystemExit(
            f"No valid cache for {name}. Run step01_build_cache.py first."
        )
    return ds


def main():
    config.OUTPUTS_DIR.mkdir(exist_ok=True)
    out = config.OUTPUTS_DIR
    print(f"Rendering to {out}")

    print("\n=== loading caches ===")
    ds_t = _load_cache(config.TREATMENT_NAME)
    ds_c = _load_cache(config.CONTROL_NAME)

    if ds_t.sizes != ds_c.sizes:
        ny = min(ds_t.sizes["y"], ds_c.sizes["y"])
        nx = min(ds_t.sizes["x"], ds_c.sizes["x"])
        print(f"AOI shape mismatch — cropping both to (y={ny}, x={nx})")
        ds_t = ds_t.isel(y=slice(0, ny), x=slice(0, nx))
        ds_c = ds_c.isel(y=slice(0, ny), x=slice(0, nx))

    season = config.DRY_SEASON_MONTHS
    season_label = (
        f"dry season, months {season}" if season else "all months"
    )
    print(f"\nSeason filter: {season_label}")

    print("\n=== site / methodology figure ===")
    visualisation.render_site_methodology(ds_t, out / "site_methodology.png")

    print("\n=== yearly NDVI grid (treatment AOI, dry-season median) ===")
    visualisation.render_yearly_ndvi_grid(
        ds_t, out / "yearly_ndvi_grid.png",
        season_months=season, season_label=season_label,
    )

    print("\n=== per-pixel change maps (dry-season only) ===")
    _, _, arr_t = aggregate.pixel_diff_map(ds_t, season_months=season)
    _, _, arr_c = aggregate.pixel_diff_map(ds_c, season_months=season)
    arr_did = arr_t - arr_c
    visualisation.render_per_pixel_change_simplified(
        arr_t, ds_t, out / "per_pixel_change_treatment.png",
        aoi_label="Treatment",
    )
    visualisation.render_per_pixel_change(
        arr_t, "Treatment (technical)", out / "per_pixel_change_treatment_technical.png",
    )
    visualisation.render_per_pixel_change(
        arr_c, "Control (technical)", out / "per_pixel_change_control.png",
    )
    visualisation.render_did_map(arr_did, out / "per_pixel_did.png")

    print("\n=== ring corridor decay (dry-season only, combined polylines) ===")
    rows = aggregate.corridor_summary(
        ds_t, config.KML_PATHS, season_months=season,
    )
    visualisation.render_corridor_decay(rows, out / "corridor_decay.png")

    print("\n=== corridor decay by segment (upstream vs downstream of dam wall) ===")
    # DEM elevations confirm: sanddam.kml (WSW arm, rises 9 m) is the
    # upstream side of the dam wall; sandam-upstream.kml (NNE arm, flat)
    # is the downstream side. The KML file names were assigned before the
    # DEM check and do not match the actual flow direction.
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
    visualisation.render_corridor_decay_comparison(
        rows_upstream, rows_downstream,
        label_left="Upstream of dam wall (WSW arm)",
        label_right="Downstream of dam wall (NNE arm)",
        out_path=out / "corridor_decay_by_segment.png",
    )

    print("\n=== time series with precipitation ===")
    ts_csv = out / "full_timeseries.csv"
    if ts_csv.exists():
        df_veg = pd.read_csv(ts_csv, parse_dates=["date"])
    else:
        from .. import aggregate as agg
        df_veg = pd.concat([
            agg.aggregate_timeseries(ds_t, config.TREATMENT_NAME),
            agg.aggregate_timeseries(ds_c, config.CONTROL_NAME),
        ], ignore_index=True)
        df_veg.to_csv(ts_csv, index=False)
    df_precip = precipitation.to_monthly(precipitation.load_or_fetch())
    visualisation.render_timeseries_with_precip(
        df_veg, df_precip, out / "timeseries_with_precip.png",
    )

    print("\nDone. Outputs in", out)


if __name__ == "__main__":
    main()
