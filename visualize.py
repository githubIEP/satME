"""Visualise a satME stats.csv run.

Usage
-----
    python visualize.py outputs/runs/makaveti_2016_2024/stats.csv

Produces files next to the CSV depending on which sources are present:
  • <run>_run_summary.txt              — human-readable record of all inputs & filter steps
  • <run>_index_timeseries.png         — Sentinel-2 median ± std per spectral index (center tile)
  • <run>_tile_comparison.png          — Sentinel-2 NDVI across all 9 tiles
  • <run>_sar_timeseries.png           — Sentinel-1 SAR index time series (center tile)
  • <run>_sar_tile_comparison.png      — Sentinel-1 RVI across all 9 tiles
  • <run>_viirs_timeseries.png         — VIIRS avg_rad nighttime lights over time
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

# ── Sentinel-2 config ─────────────────────────────────────────────────────────
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

# ── Sentinel-1 SAR config ─────────────────────────────────────────────────────
SAR_INDEX_LABELS = {
    "RVI":   "RVI — Radar Vegetation Index (0–1)",
    "VH_VV": "VH/VV ratio (dB) — depolarisation / vegetation proxy",
    "DPSVI": "DPSVI — Dual-Pol SAR Vegetation Index (lower = more vegetation)",
}

SAR_INDEX_COLORS = {
    "RVI":   "#8b1a1a",   # dark red
    "VH_VV": "#4a4a4a",   # dark grey
    "DPSVI": "#6b5a1e",   # dark olive
}

# ── VIIRS config ──────────────────────────────────────────────────────────────
VIIRS_SIGNALS = ["avg_rad"]

VIIRS_THRESHOLDS = [
    (0.5,  "rural / uninhabited",   "#cccccc"),
    (2.0,  "villages / small towns","#aaaaaa"),
    (10.0, "suburban / small city", "#888888"),
    (50.0, "urban / commercial",    "#555555"),
]

# ── Shared ────────────────────────────────────────────────────────────────────
TILE_ORDER = ["center", "N", "NE", "E", "SE", "S", "SW", "W", "NW"]
CHIRPS_COLOR = "#5b9bd5"
VIIRS_COLOR  = "#f5a623"
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


# ── Shared helpers ────────────────────────────────────────────────────────────

def _detect_indices(df: pd.DataFrame, source: str, candidates: list[str]) -> list[str]:
    """Return which candidates actually have a _p50 column in df for this source."""
    sub = df[df["source"] == source] if "source" in df.columns else df
    return [idx for idx in candidates if f"{idx}_p50" in sub.columns and sub[f"{idx}_p50"].notna().any()]


# ── Plot 3 — Sentinel-1 SAR time series (center tile) ────────────────────────

def plot_sar_timeseries(df: pd.DataFrame, out_path: Path) -> None:
    """Plot SAR index time series for the center tile (or whole df if no tiles)."""
    sar_df = df[df["source"] == "sentinel1"].copy() if "source" in df.columns else df.copy()

    if sar_df.empty:
        print("Skipping SAR time series — no sentinel1 rows found")
        return

    # Filter to center tile if tile column exists
    if "aoi_tile" in sar_df.columns and "center" in sar_df["aoi_tile"].values:
        center = sar_df[sar_df["aoi_tile"] == "center"].sort_values("date")
    else:
        center = sar_df.sort_values("date")

    if center.empty:
        print("Skipping SAR time series — no center-tile sentinel1 rows")
        return

    sar_indices = _detect_indices(sar_df, "sentinel1", list(SAR_INDEX_LABELS))
    if not sar_indices:
        print("Skipping SAR time series — no SAR index columns found (RVI, VH_VV, DPSVI)")
        return

    fig, axes = plt.subplots(
        len(sar_indices), 1,
        figsize=(15, 3.8 * len(sar_indices)),
        sharex=True,
    )
    if len(sar_indices) == 1:
        axes = [axes]

    ref = reference_date(df)

    for ax, idx in zip(axes, sar_indices):
        col_med = f"{idx}_p50"
        col_std = f"{idx}_std"
        col_p25 = f"{idx}_p25"
        col_p75 = f"{idx}_p75"
        color   = SAR_INDEX_COLORS.get(idx, "#333333")

        if col_med not in center.columns:
            ax.text(0.5, 0.5, f"{idx}: columns not found", ha="center", transform=ax.transAxes)
            continue

        dates = center["date"]
        med   = center[col_med]
        std   = center.get(col_std, pd.Series(dtype=float))

        if col_std in center.columns:
            ax.fill_between(dates, med - center[col_std], med + center[col_std],
                            alpha=0.18, color=color, label="± 1 std dev")

        if col_p25 in center.columns and col_p75 in center.columns:
            ax.fill_between(dates, center[col_p25], center[col_p75],
                            alpha=0.30, color=color, label="p25 – p75")

        ax.plot(dates, med, color=color, linewidth=1.8, label="median (p50)")
        ax.scatter(dates, med, color=color, s=25, zorder=5)
        _annotate_dates(ax, dates, med)

        if ref is not None:
            ax.axvline(ref, color="black", linewidth=1.2, linestyle="--", alpha=0.6)
            ymax = ax.get_ylim()[1]
            ax.text(ref, ymax, "  ref\n  date", fontsize=7, va="top",
                    ha="left", color="black", alpha=0.7)

        if ref is not None:
            ax.axvspan(dates.min(), ref, alpha=0.04, color="grey")
            ax.axvspan(ref, dates.max(), alpha=0.04, color="steelblue")

        # RVI is 0–1 by definition
        if idx == "RVI":
            lo, hi = ax.get_ylim()
            ax.set_ylim(max(lo, -0.05), min(hi, 1.05))

        ax.set_ylabel(SAR_INDEX_LABELS.get(idx, idx), fontsize=9)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.3f"))
        ax.legend(fontsize=7.5, loc="upper left", framealpha=0.7)
        ax.grid(axis="y", linewidth=0.5, alpha=0.4)
        ax.grid(axis="x", linewidth=0.3, alpha=0.3, which="minor")
        ax.spines[["top", "right"]].set_visible(False)

        # SAR note: no cloud masking needed
        ax.text(0.01, 0.97, "SAR — cloud-penetrating", transform=ax.transAxes,
                fontsize=6.5, va="top", ha="left", color="#666666", style="italic")

    _apply_date_xaxis(axes[-1], center["date"])

    fig.suptitle(
        f"Sentinel-1 SAR index time series — center tile\n{out_path.parent.name}",
        fontsize=11, y=1.005,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {out_path}")
    plt.close(fig)


# ── Plot 4 — Sentinel-1 tile comparison (RVI) ────────────────────────────────

def plot_sar_tile_comparison(df: pd.DataFrame, out_path: Path) -> None:
    """Plot RVI median ± std across all 9 tiles (mirrors the S2 tile comparison)."""
    sar_df = df[df["source"] == "sentinel1"].copy() if "source" in df.columns else df.copy()

    idx     = "RVI"
    col_med = f"{idx}_p50"
    col_std = f"{idx}_std"

    if col_med not in sar_df.columns or sar_df[col_med].isna().all():
        print(f"Skipping SAR tile comparison — {col_med} not found in sentinel1 rows")
        return

    if "aoi_tile" not in sar_df.columns:
        print("Skipping SAR tile comparison — no aoi_tile column (single-tile run)")
        return

    tiles_present = [t for t in TILE_ORDER if t in sar_df["aoi_tile"].unique()]
    if len(tiles_present) <= 1:
        print("Skipping SAR tile comparison — only one tile present")
        return

    n     = len(tiles_present)
    ncols = 3
    nrows = int(np.ceil(n / ncols))

    fig, axes = plt.subplots(nrows, ncols, figsize=(15, 4 * nrows),
                             sharex=True, sharey=True)
    axes_flat = axes.flatten() if hasattr(axes, "flatten") else [axes]

    ref   = reference_date(df)
    color = SAR_INDEX_COLORS[idx]

    for ax, tile in zip(axes_flat, tiles_present):
        sub   = sar_df[sar_df["aoi_tile"] == tile].sort_values("date")
        dates = sub["date"]
        med   = sub[col_med]

        if col_std in sub.columns:
            ax.fill_between(dates, med - sub[col_std], med + sub[col_std],
                            alpha=0.20, color=color)

        ax.plot(dates, med, color=color, linewidth=1.6)
        ax.scatter(dates, med, color=color, s=18, zorder=5)
        _annotate_dates(ax, dates, med)

        if ref is not None:
            ax.axvline(ref, color="black", linewidth=1.0, linestyle="--", alpha=0.5)

        ax.set_title(tile, fontsize=9, fontweight="bold")
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.set_ylim(-0.05, 1.05)
        ax.grid(axis="y", linewidth=0.4, alpha=0.4)
        ax.grid(axis="x", linewidth=0.3, alpha=0.25, which="minor")
        ax.spines[["top", "right"]].set_visible(False)
        _apply_date_xaxis(ax, dates)

    for ax in axes_flat[n:]:
        ax.set_visible(False)

    fig.suptitle(
        f"Sentinel-1 RVI median ± std — all {n} tiles\n{out_path.parent.name}",
        fontsize=11, y=1.005,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {out_path}")
    plt.close(fig)


# ── Plot 5 — VIIRS nighttime lights ──────────────────────────────────────────

def plot_viirs_timeseries(df: pd.DataFrame, out_path: Path) -> None:
    """Plot VIIRS avg_rad nighttime radiance over time with threshold annotations."""
    viirs_df = df[df["source"] == "viirs"].copy() if "source" in df.columns else df.copy()

    if viirs_df.empty:
        print("Skipping VIIRS time series — no viirs rows found")
        return

    # Use center tile if available, otherwise all rows
    if "aoi_tile" in viirs_df.columns and "center" in viirs_df["aoi_tile"].values:
        plot_df = viirs_df[viirs_df["aoi_tile"] == "center"].sort_values("date")
    else:
        plot_df = viirs_df.sort_values("date")

    if plot_df.empty:
        print("Skipping VIIRS time series — no usable rows")
        return

    # Prefer median (p50); fall back to mean
    if "avg_rad_p50" in plot_df.columns and plot_df["avg_rad_p50"].notna().any():
        rad_col   = "avg_rad_p50"
        rad_label = "avg_rad median (p50) — nW/cm²/sr"
    elif "avg_rad_mean" in plot_df.columns and plot_df["avg_rad_mean"].notna().any():
        rad_col   = "avg_rad_mean"
        rad_label = "avg_rad mean — nW/cm²/sr"
    else:
        print("Skipping VIIRS time series — no avg_rad columns found")
        return

    has_std = "avg_rad_std" in plot_df.columns and plot_df["avg_rad_std"].notna().any()

    fig, ax = plt.subplots(figsize=(15, 5))

    dates = plot_df["date"]
    vals  = plot_df[rad_col]

    # Determine bar width: monthly data → ~28 days, leave a small gap
    bar_width = pd.Timedelta(days=25)

    ax.bar(dates, vals, width=bar_width, color=VIIRS_COLOR, alpha=0.80,
           label=rad_label, align="center")

    # ± 1 std error bars if available
    if has_std:
        ax.errorbar(dates, vals, yerr=plot_df["avg_rad_std"],
                    fmt="none", color="#b37a00", linewidth=0.8, capsize=3, alpha=0.7)

    # Value labels above bars
    for d, v in zip(dates, vals):
        if pd.notna(v):
            ax.annotate(f"{v:.2f}", xy=(d, v), xytext=(0, 4),
                        textcoords="offset points", fontsize=5.5,
                        ha="center", va="bottom", color="#333333")

    # Interpretation threshold lines
    threshold_vals = [t[0] for t in VIIRS_THRESHOLDS]
    ymax = max(vals.max() * 1.25 if vals.notna().any() else 1.0,
               max(threshold_vals) * 0.6)
    for thresh, label, tcolor in VIIRS_THRESHOLDS:
        if thresh < ymax * 1.4:
            ax.axhline(thresh, color=tcolor, linewidth=0.8, linestyle="--", alpha=0.7)
            ax.text(dates.max(), thresh, f"  {label}", va="bottom",
                    fontsize=6, color=tcolor, alpha=0.85)

    # Reference date
    ref = reference_date(df)
    if ref is not None:
        ax.axvline(ref, color="black", linewidth=1.2, linestyle="--", alpha=0.6)
        ax.text(ref, ax.get_ylim()[1], "  ref\n  date", fontsize=7, va="top",
                ha="left", color="black", alpha=0.7)

        ax.axvspan(dates.min(), ref, alpha=0.04, color="grey")
        ax.axvspan(ref, dates.max(), alpha=0.04, color="steelblue")

    _apply_date_xaxis(ax, dates)
    ax.set_ylabel("avg_rad (nW/cm²/sr)", fontsize=9)
    ax.legend(fontsize=8, framealpha=0.7)
    ax.grid(axis="y", linewidth=0.5, alpha=0.4)
    ax.spines[["top", "right"]].set_visible(False)

    fig.suptitle(
        f"VIIRS nighttime radiance — center tile\n{out_path.parent.name}",
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

    sources_present = df["source"].dropna().unique().tolist() if "source" in df.columns else []
    print(f"Loaded {len(df)} rows | {df['date'].nunique()} dates | "
          f"sources: {sorted(sources_present)} | "
          f"tiles: {sorted(df['aoi_tile'].dropna().unique()) if 'aoi_tile' in df.columns else '(single tile)'}")

    write_run_summary(run_dir)

    # Produce per-source dataframes; keep pre_post / reference_date derivation
    # from the full df so all plots share the same reference line position.
    def _src(name):
        """Return rows for a single source, preserving pre_post column."""
        if "source" not in df.columns:
            return df
        return df[df["source"] == name].copy()

    # ── Sentinel-2 plots ──────────────────────────────────────────────────────
    if not sources_present or "sentinel2" in sources_present:
        s2_df = _src("sentinel2") if sources_present else df
        if not s2_df.empty:
            # Pass reference_date context from full df by injecting a helper
            plot_index_timeseries(s2_df, run_dir / f"{run_dir.name}_index_timeseries.png")
            plot_tile_comparison(s2_df,  run_dir / f"{run_dir.name}_tile_comparison.png")
        else:
            print("Skipping Sentinel-2 plots — no sentinel2 rows in stats.csv")

    # ── Sentinel-1 SAR plots ──────────────────────────────────────────────────
    if "sentinel1" in sources_present:
        plot_sar_timeseries(df, run_dir / f"{run_dir.name}_sar_timeseries.png")
        plot_sar_tile_comparison(df, run_dir / f"{run_dir.name}_sar_tile_comparison.png")

    # ── VIIRS nighttime lights plot ───────────────────────────────────────────
    if "viirs" in sources_present:
        plot_viirs_timeseries(df, run_dir / f"{run_dir.name}_viirs_timeseries.png")


if __name__ == "__main__":
    main()
