"""Visualise a satME stats.csv run.

Usage
-----
    python visualize.py outputs/runs/makaveti_2016_2024/stats.csv

Produces three files next to the CSV:
  • <run>_run_summary.txt        — human-readable record of all inputs & filter steps
  • <run>_index_timeseries.png  — median ± std for each spectral index (center tile)
  • <run>_tile_comparison.png   — NDVI across all 9 tiles
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
INDICES = ["NDVI", "NDWI", "NDMI"]

INDEX_LABELS = {
    "NDVI":  "NDVI — vegetation health",
    "NDWI":  "NDWI — surface water",
    "NDMI":  "NDMI — soil/vegetation moisture",
}

INDEX_COLORS = {
    "NDVI":  "#2d7a2d",
    "NDWI":  "#1a6fa3",
    "NDMI":  "#a0522d",
}

TILE_ORDER = ["center", "N", "NE", "E", "SE", "S", "SW", "W", "NW"]
CHIRPS_COLOR = "#5b9bd5"
MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_stats(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["date"])
    return df.sort_values("date")


def load_flag_report(run_dir: Path) -> pd.DataFrame | None:
    p = run_dir / "flag_report.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, parse_dates=["date"])
    return df.sort_values("date")


def load_metadata(run_dir: Path) -> dict:
    p = run_dir / "run_metadata.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text())


def reference_date(df: pd.DataFrame):
    pre  = df[df["pre_post"] == "PRE"]["date"].max()
    post = df[df["pre_post"] == "POST"]["date"].min()
    if pd.notna(pre) and pd.notna(post):
        return pre + (post - pre) / 2
    return None


def _fmt_date(d) -> str:
    if pd.isna(d):
        return "—"
    return pd.Timestamp(d).strftime("%Y-%m-%d")


def _month_list(months: list[int]) -> str:
    return ", ".join(MONTH_NAMES[m - 1] for m in months)


# ── Run summary text file ─────────────────────────────────────────────────────

def write_run_summary(run_dir: Path) -> Path:
    meta  = load_metadata(run_dir)
    flags = load_flag_report(run_dir)
    out   = run_dir / f"{run_dir.name}_run_summary.txt"

    cfg      = meta.get("config", {})
    aoi_meta = meta.get("aoi", {})
    gee      = meta.get("gee_counts", {})
    run_cfg  = cfg.get("run", {})
    aoi_cfg  = cfg.get("aoi", {})
    season   = cfg.get("season", {})
    sources  = cfg.get("sources", {})
    s2_cfg   = sources.get("sentinel2", {})

    W = 65  # line width

    def rule(char="─"):
        return char * W

    lines = [
        rule("═"),
        f"  satME Run Summary",
        f"  Run:       {meta.get('run_name', run_dir.name)}",
        f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"  Pipeline:  satME v{meta.get('satme_version', '?')}",
        rule("═"),
        "",
        "RUN CONFIGURATION",
        rule(),
        f"  Reference date:  {run_cfg.get('reference_date', '?')}",
        f"                   (images before = PRE, on/after = POST)",
        f"  Date range:      {cfg.get('date_range', {}).get('start', '?')} "
            f"→ {cfg.get('date_range', {}).get('end', '?')}",
        f"  Season filter:   months {season.get('target_months', [])} "
            f"({_month_list(season.get('target_months', []))})",
        f"  Season mode:     {'flag only (all images kept)' if season.get('flag_only') else 'exclude out-of-season images'}",
        "",
        "AREA OF INTEREST",
        rule(),
    ]

    mode = aoi_cfg.get("mode", aoi_meta.get("mode", "?"))
    if mode == "point_radius":
        lines += [
            f"  Mode:    point + radius",
            f"  Centre:  {aoi_meta.get('center_lat', aoi_cfg.get('center', {}).get('lat', '?'))}°,  "
                f"{aoi_meta.get('center_lon', aoi_cfg.get('center', {}).get('lon', '?'))}°",
            f"  Radius:  {aoi_meta.get('radius_m', aoi_cfg.get('radius_m', '?'))} m",
            f"  Area:    {aoi_meta.get('area_km2', '?')} km²  "
                f"({aoi_meta.get('radius_m', aoi_cfg.get('radius_m', 315)) * 2} × "
                f"{aoi_meta.get('radius_m', aoi_cfg.get('radius_m', 315)) * 2} m square)",
        ]
    else:
        lines += [f"  Mode:    polygon", f"  Area:    {aoi_meta.get('area_km2', '?')} km²"]

    tiles = aoi_meta.get("tile_labels", [])
    if tiles:
        lines.append(f"  Tiles:   {len(tiles)} — {', '.join(tiles)}")
    lines.append(f"  WKT:     {aoi_meta.get('wkt', '?')}")

    # ── Per-source filter pipeline ────────────────────────────────────────────
    for src_name, src_cfg in sources.items():
        if not src_cfg.get("enabled", False):
            continue

        lines += ["", f"SENTINEL-2 FILTER PIPELINE" if src_name == "sentinel2" else f"{src_name.upper()} FILTER PIPELINE", rule()]

        if src_name == "sentinel2":
            lines += [
                f"  Collection:            {s2_cfg.get('collection', '?')}",
                f"  Tile cloud ceiling:    {s2_cfg.get('max_tile_cloud_pct', '?')}%",
                f"  AOI cloud ceiling:     {s2_cfg.get('max_aoi_cloud_pct', '?')}%",
                f"  Copernicus fallback:   {'yes, pre-' + s2_cfg.get('gee_cutoff_date', '?') if s2_cfg.get('copernicus_fallback') else 'no'}",
                f"  Indices computed:      {', '.join(s2_cfg.get('indices', []))}",
                "",
            ]

            s2_gee = gee.get("sentinel2", {})
            n_tile  = s2_gee.get("n_tile_filtered", "?")
            n_clean = s2_gee.get("n_clean", "?")
            n_final = s2_gee.get("n_in_season", "?")

            if flags is not None:
                src_flags = flags[flags["source"] == "sentinel2"] if "source" in flags.columns else flags
                n_total = len(src_flags)
                n_cloudy_tile = n_total - (n_tile if isinstance(n_tile, int) else 0)
                n_cloudy_aoi  = (n_tile if isinstance(n_tile, int) else 0) - (n_clean if isinstance(n_clean, int) else 0)
                n_season_drop = (n_clean if isinstance(n_clean, int) else 0) - (n_final if isinstance(n_final, int) else 0)
                lines += [
                    f"  {'Step':<38} {'In':>6}  {'Out':>6}",
                    f"  {rule('-')}",
                    f"  {'All images in date range':<38} {n_total:>6}",
                    f"  {'After tile cloud filter (≤' + str(s2_cfg.get('max_tile_cloud_pct','?')) + '%)':<38} {n_tile if isinstance(n_tile,int) else '?':>6}  {n_cloudy_tile if isinstance(n_tile,int) else '?':>6} rejected",
                    f"  {'After AOI cloud filter (≤' + str(s2_cfg.get('max_aoi_cloud_pct','?')) + '%)':<38} {n_clean if isinstance(n_clean,int) else '?':>6}  {n_cloudy_aoi if isinstance(n_clean,int) else '?':>6} rejected",
                    f"  {'After season filter':<38} {n_final if isinstance(n_final,int) else '?':>6}  {n_season_drop if isinstance(n_final,int) else '?':>6} rejected",
                    f"  {rule('-')}",
                    f"  {'FINAL clean images used':<38} {n_final if isinstance(n_final,int) else '?':>6}",
                ]
            else:
                lines += [
                    f"  After tile cloud filter:   {n_tile}",
                    f"  After AOI cloud filter:    {n_clean}",
                    f"  After season filter:       {n_final}",
                ]

    # ── Included / excluded images ────────────────────────────────────────────
    # "Included" = date appears in stats.csv (passed cloud + season filters).
    # "in_clean_set" in the flag report only records cloud-filter pass/fail;
    # out-of-season images can be cloud-clean but still excluded from stats.
    if flags is not None:
        stats_csv = run_dir / "stats.csv"
        if stats_csv.exists():
            stats_dates = set(
                pd.read_csv(stats_csv, usecols=["date"])["date"].unique()
            )
        else:
            # Fallback: treat cloud-clean images as included
            stats_dates = None

        src_flags = flags[flags["source"] == "sentinel2"] if "source" in flags.columns else flags

        if stats_dates is not None:
            included = src_flags[src_flags["date"].astype(str).isin(stats_dates)].sort_values("date")
            excluded = src_flags[~src_flags["date"].astype(str).isin(stats_dates)].sort_values("date")
        else:
            included = src_flags[src_flags["in_clean_set"] == True].sort_values("date")
            excluded = src_flags[src_flags["in_clean_set"] == False].sort_values("date")

        # ── Included ──────────────────────────────────────────────────────────
        lines += ["", f"INCLUDED IN STATS  ({len(included)})  — passed all filters", rule()]
        lines.append(f"  {'Date':<14} {'Pre/Post':<10} {'AOI cloud':>10}  {'Tile cloud':>11}  Flags")
        lines.append(f"  {rule('-')}")
        for _, row in included.iterrows():
            aoi_c  = f"{float(row['aoi_cloud_pct']):.1f}%" if pd.notna(row.get('aoi_cloud_pct')) else "?"
            tile_c = f"{float(row['tile_cloud_pct']):.1f}%" if pd.notna(row.get('tile_cloud_pct')) else "?"
            flagstr = str(row.get("flags", "")).replace("|", " | ")
            lines.append(f"  {_fmt_date(row['date']):<14} {str(row.get('pre_post','')):<10} {aoi_c:>10}  {tile_c:>11}  {flagstr}")

        # ── Excluded — break down by reason ───────────────────────────────────
        excl_cloudy_tile = excluded[excluded["in_clean_set"] == False]
        excl_passed_cloud = excluded[excluded["in_clean_set"] == True]
        excl_season = excl_passed_cloud[
            excl_passed_cloud["flags"].astype(str).str.contains("OUT_OF_SEASON", na=False)
        ]
        excl_other = excl_passed_cloud[
            ~excl_passed_cloud["flags"].astype(str).str.contains("OUT_OF_SEASON", na=False)
        ]

        lines += [
            "",
            f"EXCLUDED IMAGES  ({len(excluded)})  — did not reach stats output",
            rule(),
            f"  Note: 'in_clean_set' = passed cloud filters only.",
            f"        Season filter is applied after; out-of-season images",
            f"        can be cloud-clean yet still excluded from stats.",
            "",
            f"  By reason:",
            f"    Tile cloud > {s2_cfg.get('max_tile_cloud_pct','?')}% (or AOI cloud > {s2_cfg.get('max_aoi_cloud_pct','?')}%):  {len(excl_cloudy_tile)}",
            f"    Cloud-clean but out of season:                  {len(excl_season)}",
            f"    Other:                                          {len(excl_other)}",
            "",
        ]

        for label, subset in [
            (f"  Cloud-rejected  ({len(excl_cloudy_tile)})", excl_cloudy_tile),
            (f"  Out-of-season, cloud-clean  ({len(excl_season)})", excl_season),
        ]:
            if len(subset) == 0:
                continue
            lines += [label, f"  {rule('-')}"]
            lines.append(f"  {'Date':<14} {'Pre/Post':<10} {'AOI cloud':>10}  {'Tile cloud':>11}  Flags")
            for _, row in subset.iterrows():
                aoi_c  = f"{float(row['aoi_cloud_pct']):.1f}%" if pd.notna(row.get('aoi_cloud_pct')) else "?"
                tile_c = f"{float(row['tile_cloud_pct']):.1f}%" if pd.notna(row.get('tile_cloud_pct')) else "?"
                flagstr = str(row.get("flags", "")).replace("|", " | ")
                lines.append(f"  {_fmt_date(row['date']):<14} {str(row.get('pre_post','')):<10} {aoi_c:>10}  {tile_c:>11}  {flagstr}")

    # ── Stats config ──────────────────────────────────────────────────────────
    stats_cfg = cfg.get("stats", {})
    if stats_cfg:
        lines += ["", "STATISTICS COMPUTED", rule()]
        lines += [
            f"  Percentiles:  {stats_cfg.get('percentiles', [])}",
            f"  Mean:         {'yes' if stats_cfg.get('include_mean') else 'no'}",
            f"  Std dev:      {'yes' if stats_cfg.get('include_stddev') else 'no'}",
            f"  Min/max:      {'yes' if stats_cfg.get('include_min_max') else 'no'}",
        ]

    lines += ["", rule("═"), ""]

    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {out}")
    return out


# ── Plot helpers ──────────────────────────────────────────────────────────────

def _apply_date_xaxis(ax, dates: pd.Series) -> None:
    """Month-level x-axis: major ticks = Jun/Sep each year, minor = other months."""
    span_years = (dates.max() - dates.min()).days / 365
    if span_years <= 3:
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[6, 9]))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[6]))
        ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[9]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=7.5)


def _annotate_dates(ax, dates: pd.Series, values: pd.Series) -> None:
    """Small rotated date labels above each data point."""
    for d, v in zip(dates, values):
        ax.annotate(
            pd.Timestamp(d).strftime("%d %b"),
            xy=(d, v), xytext=(0, 6),
            textcoords="offset points",
            fontsize=5.5, ha="center", va="bottom",
            rotation=60, color="#444444",
        )


# ── Plot 1 — index time series (center tile) ─────────────────────────────────

def plot_index_timeseries(df: pd.DataFrame, out_path: Path) -> None:
    center = df[df["aoi_tile"] == "center"].copy().sort_values("date")

    has_chirps = "chirps_30d_mm" in center.columns and center["chirps_30d_mm"].notna().any()
    n_rows     = len(INDICES) + (1 if has_chirps else 0)

    fig, axes = plt.subplots(
        n_rows, 1,
        figsize=(15, 3.8 * n_rows),
        sharex=True,
        gridspec_kw={"height_ratios": [3] * len(INDICES) + ([1.5] if has_chirps else [])},
    )
    if n_rows == 1:
        axes = [axes]

    ref = reference_date(df)

    for ax, idx in zip(axes[:len(INDICES)], INDICES):
        col_med = f"{idx}_p50"
        col_std = f"{idx}_std"
        col_p25 = f"{idx}_p25"
        col_p75 = f"{idx}_p75"

        if any(c not in center.columns for c in [col_med, col_std]):
            ax.text(0.5, 0.5, f"{idx}: columns not found", ha="center", transform=ax.transAxes)
            continue

        dates = center["date"]
        med   = center[col_med]
        std   = center[col_std]
        color = INDEX_COLORS[idx]

        # ± 1 std band
        ax.fill_between(dates, med - std, med + std,
                        alpha=0.18, color=color, label="± 1 std dev")

        # p25–p75 band
        if col_p25 in center.columns and col_p75 in center.columns:
            ax.fill_between(dates, center[col_p25], center[col_p75],
                            alpha=0.30, color=color, label="p25 – p75")

        # Median line + points
        ax.plot(dates, med, color=color, linewidth=1.8, label="median (p50)")
        ax.scatter(dates, med, color=color, s=25, zorder=5)

        # Date labels on each point
        _annotate_dates(ax, dates, med)

        # Reference date
        if ref is not None:
            ax.axvline(ref, color="black", linewidth=1.2, linestyle="--", alpha=0.6)
            ymax = ax.get_ylim()[1]
            ax.text(ref, ymax, "  ref\n  date", fontsize=7, va="top",
                    ha="left", color="black", alpha=0.7)

        # PRE/POST shading
        if ref is not None:
            ax.axvspan(dates.min(), ref, alpha=0.04, color="grey")
            ax.axvspan(ref, dates.max(), alpha=0.04, color="steelblue")

        ax.set_ylabel(INDEX_LABELS.get(idx, idx), fontsize=9)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.legend(fontsize=7.5, loc="upper left", framealpha=0.7)
        ax.grid(axis="y", linewidth=0.5, alpha=0.4)
        ax.grid(axis="x", linewidth=0.3, alpha=0.3, which="minor")
        ax.spines[["top", "right"]].set_visible(False)

    # CHIRPS panel
    if has_chirps:
        ax_rain = axes[len(INDICES)]
        ax_rain.bar(center["date"], center["chirps_30d_mm"],
                    width=10, color=CHIRPS_COLOR, alpha=0.75, label="30-day rainfall (mm)")
        for d, v in zip(center["date"], center["chirps_30d_mm"]):
            if pd.notna(v):
                ax_rain.annotate(f"{v:.0f}", xy=(d, v), xytext=(0, 3),
                                 textcoords="offset points", fontsize=5.5,
                                 ha="center", va="bottom", color="#333333")
        if ref is not None:
            ax_rain.axvline(ref, color="black", linewidth=1.2, linestyle="--", alpha=0.6)
        ax_rain.set_ylabel("CHIRPS 30d (mm)", fontsize=9)
        ax_rain.legend(fontsize=7.5, framealpha=0.7)
        ax_rain.grid(axis="y", linewidth=0.5, alpha=0.4)
        ax_rain.spines[["top", "right"]].set_visible(False)

    # X-axis on the bottom panel
    _apply_date_xaxis(axes[-1], center["date"])

    fig.suptitle(
        f"Spectral index time series — center tile\n{out_path.parent.name}",
        fontsize=11, y=1.005,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {out_path}")
    plt.close(fig)


# ── Plot 2 — tile comparison (NDVI median across all 9 tiles) ────────────────

def plot_tile_comparison(df: pd.DataFrame, out_path: Path) -> None:
    idx     = "NDVI"
    col_med = f"{idx}_p50"
    col_std = f"{idx}_std"

    if col_med not in df.columns:
        print(f"Skipping tile comparison — {col_med} not found")
        return

    tiles_present = [t for t in TILE_ORDER if t in df["aoi_tile"].unique()]
    n     = len(tiles_present)
    ncols = 3
    nrows = int(np.ceil(n / ncols))

    fig, axes = plt.subplots(nrows, ncols, figsize=(15, 4 * nrows),
                             sharex=True, sharey=True)
    axes_flat = axes.flatten() if hasattr(axes, "flatten") else [axes]

    ref   = reference_date(df)
    color = INDEX_COLORS[idx]

    for ax, tile in zip(axes_flat, tiles_present):
        sub   = df[df["aoi_tile"] == tile].sort_values("date")
        dates = sub["date"]
        med   = sub[col_med]
        std   = sub[col_std]

        ax.fill_between(dates, med - std, med + std, alpha=0.20, color=color)
        ax.plot(dates, med, color=color, linewidth=1.6)
        ax.scatter(dates, med, color=color, s=18, zorder=5)
        _annotate_dates(ax, dates, med)

        if ref is not None:
            ax.axvline(ref, color="black", linewidth=1.0, linestyle="--", alpha=0.5)

        ax.set_title(tile, fontsize=9, fontweight="bold")
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.grid(axis="y", linewidth=0.4, alpha=0.4)
        ax.grid(axis="x", linewidth=0.3, alpha=0.25, which="minor")
        ax.spines[["top", "right"]].set_visible(False)
        _apply_date_xaxis(ax, dates)

    for ax in axes_flat[n:]:
        ax.set_visible(False)

    fig.suptitle(
        f"NDVI median ± std — all 9 tiles\n{out_path.parent.name}",
        fontsize=11, y=1.005,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {out_path}")
    plt.close(fig)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        csv_path = Path("outputs/runs/makaveti_2016_2024/stats.csv")
    else:
        csv_path = Path(sys.argv[1])

    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found")
        sys.exit(1)

    df      = load_stats(csv_path)
    run_dir = csv_path.parent

    print(f"Loaded {len(df)} rows | {df['date'].nunique()} dates | "
          f"tiles: {sorted(df['aoi_tile'].dropna().unique())}")

    write_run_summary(run_dir)
    plot_index_timeseries(df, run_dir / f"{run_dir.name}_index_timeseries.png")
    plot_tile_comparison(df,  run_dir / f"{run_dir.name}_tile_comparison.png")


if __name__ == "__main__":
    main()
