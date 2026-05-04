"""All rendering: site map, yearly NDVI grid, time-series with rainfall,
per-pixel change maps, ring decay chart.

Vendored from david/exploration/visualisation.py — only the config import
path was changed. A few site-specific strings (e.g. "Machakos, Kenya")
remain hardcoded; parameterising them is a follow-up.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib.path import Path as MplPath
from pyproj import Transformer
from shapely.ops import transform as shapely_transform

import rioxarray  # noqa: F401  registers .rio accessor

from . import aggregate, _runtime_config as config


# ---------- shared helpers ----------
def _shapely_to_mpl_path(geom):
    if geom.is_empty:
        return None
    if geom.geom_type == "Polygon":
        polys = [geom]
    elif geom.geom_type == "MultiPolygon":
        polys = list(geom.geoms)
    else:
        return None
    vertices, codes = [], []
    for poly in polys:
        ext = list(poly.exterior.coords)
        n = len(ext)
        vertices.extend(ext)
        codes.append(MplPath.MOVETO)
        codes.extend([MplPath.LINETO] * (n - 2))
        codes.append(MplPath.CLOSEPOLY)
        for hole in poly.interiors:
            h = list(hole.coords)
            m = len(h)
            vertices.extend(h)
            codes.append(MplPath.MOVETO)
            codes.extend([MplPath.LINETO] * (m - 2))
            codes.append(MplPath.CLOSEPOLY)
    return MplPath(np.asarray(vertices), codes)


def _iter_lines(geom):
    """Yield each LineString in a LineString or MultiLineString geometry."""
    if geom.geom_type == "LineString":
        yield geom
    elif geom.geom_type == "MultiLineString":
        for sub in geom.geoms:
            yield sub


# ---------- site / methodology figure ----------
def render_site_methodology(ds: xr.Dataset, out_path: Path) -> None:
    """Two-panel figure: channel polyline on NDVI; same with ring buffers."""
    crs = aggregate.get_cache_crs(ds)
    period = ds["period"].values
    post_ndvi = (
        ds["ndvi"].isel(time=(period == "post"))
        .mean(dim="time", skipna=True)
        .compute()
    )

    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    geom_wgs = aggregate.parse_kml_geometry(config.KML_PATHS)
    geom_utm = shapely_transform(transformer.transform, geom_wgs)
    dam_x, dam_y = transformer.transform(config.TREATMENT_LON, config.TREATMENT_LAT)

    ymin, ymax = float(ds.y.min()), float(ds.y.max())
    xmin, xmax = float(ds.x.min()), float(ds.x.max())
    extent = [xmin, xmax, ymin, ymax]

    rings = []
    for inner_d, outer_d in zip(config.RING_BOUNDS_M[:-1], config.RING_BOUNDS_M[1:]):
        outer = geom_utm.buffer(outer_d)
        ring = outer.difference(geom_utm.buffer(inner_d)) if inner_d > 0 else outer
        rings.append(ring)
    ring_colors = ["#08306b", "#2171b5", "#6baed6", "#bdd7e7", "#deebf7"]
    ring_labels = [
        f"{a}–{b} m" for a, b in zip(config.RING_BOUNDS_M[:-1], config.RING_BOUNDS_M[1:])
    ]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6.5), dpi=130)

    im1 = ax1.imshow(post_ndvi.values, extent=extent, origin="upper",
                     vmin=0, vmax=0.7, cmap="RdYlGn")
    first_label = True
    for line in _iter_lines(geom_utm):
        lx, ly = line.xy
        ax1.plot(lx, ly, color="cyan", linewidth=2.6,
                 label="Hand-traced channel" if first_label else None)
        first_label = False
    ax1.plot(dam_x, dam_y, marker="*", markersize=22, color="white",
             markeredgecolor="black", markeredgewidth=1.5, linestyle="",
             label="Sand dam location")
    ax1.set_title("Site: channel polyline on the treatment AOI\n"
                  "Background: mean NDVI post-construction",
                  fontsize=11)
    ax1.set_xlabel("UTM easting (m)")
    ax1.set_ylabel("UTM northing (m)")
    ax1.legend(loc="upper right", fontsize=9, framealpha=0.92)
    ax1.set_aspect("equal")
    plt.colorbar(im1, ax=ax1, fraction=0.046, pad=0.04, label="NDVI")

    ax2.imshow(post_ndvi.values, extent=extent, origin="upper",
               vmin=0, vmax=0.7, cmap="RdYlGn", alpha=0.55)
    for ring, color in zip(rings, ring_colors):
        path = _shapely_to_mpl_path(ring)
        if path is None:
            continue
        patch = mpatches.PathPatch(path, facecolor=color, alpha=0.55,
                                   edgecolor=color, linewidth=0.7)
        ax2.add_patch(patch)
    for line in _iter_lines(geom_utm):
        lx, ly = line.xy
        ax2.plot(lx, ly, color="black", linewidth=2.0)
    ax2.plot(dam_x, dam_y, marker="*", markersize=20, color="white",
             markeredgecolor="black", markeredgewidth=1.5, linestyle="")
    ax2.set_title("Methodology: distance bands around the channel",
                  fontsize=11)
    ax2.set_xlabel("UTM easting (m)")
    ax2.set_ylabel("UTM northing (m)")
    handles = [
        mpatches.Patch(color=c, alpha=0.55, label=lbl)
        for c, lbl in zip(ring_colors, ring_labels)
    ]
    handles.append(plt.Line2D([0], [0], color="black", linewidth=2.0,
                              label="Channel"))
    ax2.legend(handles=handles, loc="upper right", fontsize=8,
               framealpha=0.92, title="Distance from channel")
    ax2.set_aspect("equal")
    ax2.set_xlim(xmin, xmax)
    ax2.set_ylim(ymin, ymax)

    total_length = sum(line.length for line in _iter_lines(geom_utm))
    fig.suptitle(
        f"Sand dam at {config.TREATMENT_LAT}, {config.TREATMENT_LON}. "
        f"Treatment AOI: {2*config.HALF_SIZE_M:.0f} m × {2*config.HALF_SIZE_M:.0f} m. "
        f"Channel polyline total length: {total_length:.0f} m "
        f"({sum(1 for _ in _iter_lines(geom_utm))} segments).",
        y=1.01, fontsize=11,
    )
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


# ---------- annual NDVI grid ----------
def render_yearly_ndvi_grid(
    ds: xr.Dataset,
    out_path: Path,
    season_months: list[int] | None = None,
    season_label: str = "all months",
) -> None:
    """One panel per year showing the median NDVI for that year, optionally
    restricted to a calendar season."""
    medians = aggregate.yearly_median_ndvi(ds, season_months=season_months)
    if not medians:
        print("  no annual data; skipping yearly NDVI grid")
        return
    years = sorted(medians.keys())
    ncols = 5
    nrows = int(np.ceil(len(years) / ncols))

    fig, axes = plt.subplots(
        nrows, ncols, figsize=(ncols * 2.6, nrows * 2.9), dpi=130
    )
    axes_flat = np.atleast_2d(axes).flatten()
    last_im = None
    dam_year = config.DAM_CONSTRUCTION_DATE.year
    drought_lo, drought_hi = config.DROUGHT_START.year, config.DROUGHT_END.year
    for ax, year in zip(axes_flat, years):
        last_im = ax.imshow(
            medians[year], vmin=config.NDVI_VMIN, vmax=config.NDVI_VMAX,
            cmap="RdYlGn",
        )
        period = "pre-construction" if year < dam_year else "post-construction"
        drought_flag = " (drought)" if drought_lo <= year <= drought_hi else ""
        ax.set_title(f"{year} – {period}{drought_flag}", fontsize=9)
        ax.set_xticks([]); ax.set_yticks([])
    for ax in axes_flat[len(years):]:
        ax.axis("off")
    fig.suptitle(
        f"Annual median NDVI in the treatment AOI ({season_label})",
        fontsize=12, y=1.0,
    )
    fig.tight_layout()
    if last_im is not None:
        fig.colorbar(last_im, ax=axes_flat.tolist(), fraction=0.025, pad=0.02,
                     label="NDVI (median across all clear scenes for the year)")
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


# ---------- time series with precipitation ----------
def render_timeseries_with_precip(
    df_veg: pd.DataFrame,
    df_precip_monthly: pd.DataFrame,
    out_path: Path,
    aois: list[tuple[str, str]] | None = None,
) -> None:
    """Two-panel chart: vegetation fraction (top); monthly precipitation (bottom).

    aois: list of (aoi_name, colour) pairs to plot. Defaults to treatment only.
    """
    if aois is None:
        aois = [(config.TREATMENT_NAME, "tab:orange")]
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11, 7), dpi=130, sharex=True,
        gridspec_kw={"height_ratios": [3, 1]},
    )

    for name, color in aois:
        sub = df_veg[df_veg["aoi"] == name].sort_values("date")
        ax1.scatter(sub["date"], sub["vegetation_fraction"],
                    s=8, alpha=0.4, color=color, label=f"{name} (per scene)")
        rolled = (
            sub.set_index("date")["vegetation_fraction"]
            .rolling("90D", min_periods=3).mean()
        )
        ax1.plot(rolled.index, rolled.values, color=color, linewidth=1.5,
                 label=f"{name} (90-day rolling)")
    ax1.axvline(config.DAM_CONSTRUCTION_DATE, color="black", linestyle="--",
                linewidth=1, label="Dam constructed")
    ax1.axvspan(config.DROUGHT_START, config.DROUGHT_END, color="red", alpha=0.08,
                label="Drought window")
    ax1.set_ylabel(f"Vegetation fraction (NDVI > {config.NDVI_VEG_THRESHOLD})")
    ax1.set_title("Vegetation cover and rainfall", fontsize=12)
    ax1.legend(loc="upper right", fontsize=8, framealpha=0.92)
    ax1.grid(alpha=0.3)
    ax1.set_ylim(-0.02, 1.02)

    df_p = df_precip_monthly.copy()
    df_p["date"] = pd.to_datetime(df_p["date"])
    ax2.bar(df_p["date"], df_p["precip_mm"], width=25, color="tab:blue",
            edgecolor="none", alpha=0.85)
    ax2.axvline(config.DAM_CONSTRUCTION_DATE, color="black", linestyle="--",
                linewidth=1)
    ax2.axvspan(config.DROUGHT_START, config.DROUGHT_END, color="red", alpha=0.08)
    ax2.set_ylabel("Monthly rainfall (mm)")
    ax2.set_xlabel("Date")
    ax2.grid(alpha=0.3, axis="y")

    ax2.xaxis.set_major_locator(mdates.YearLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


# ---------- per-pixel change maps ----------
def render_per_pixel_change(
    arr: np.ndarray,
    aoi_label: str,
    out_path: Path,
) -> None:
    """Per-pixel post-pre vegetation-frequency change map."""
    fig, ax = plt.subplots(figsize=(6, 6), dpi=130)
    im = ax.imshow(
        arr, vmin=config.NDVI_DIFF_VMIN, vmax=config.NDVI_DIFF_VMAX, cmap="RdBu",
    )
    ax.set_title(
        f"{aoi_label} AOI: per-pixel vegetation-frequency change\n"
        f"Post-construction (treatment) minus pre-construction (control)",
        fontsize=10,
    )
    ax.set_xticks([]); ax.set_yticks([])
    plt.colorbar(
        im, ax=ax, fraction=0.046, pad=0.04,
        label=f"Frequency (NDVI > {config.NDVI_VEG_THRESHOLD}) post − pre",
    )
    fig.text(
        0.5, 0.02,
        f"Pre-construction = images before {config.DAM_CONSTRUCTION_DATE.date()} "
        f"(used as the temporal control). "
        f"Post-construction = images after that date (treatment). "
        f"Grey pixels lacked data in fewer than {config.MIN_BINS_REQUIRED} day-of-year bins.",
        ha="center", fontsize=8, style="italic", wrap=True,
    )
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


def render_per_pixel_change_simplified(
    arr: np.ndarray,
    ds: xr.Dataset,
    out_path: Path,
    aoi_label: str = "Treatment",
) -> None:
    """Simplified plain-language version of the per-pixel change map."""
    from scipy.ndimage import gaussian_filter
    from matplotlib.colors import BoundaryNorm, ListedColormap

    crs = aggregate.get_cache_crs(ds)
    ymin, ymax = float(ds.y.min()), float(ds.y.max())
    xmin, xmax = float(ds.x.min()), float(ds.x.max())
    extent = [xmin, xmax, ymin, ymax]

    arr_smooth = arr.copy()
    nan_mask = np.isnan(arr_smooth)
    arr_smooth[nan_mask] = 0.0
    arr_smooth = gaussian_filter(arr_smooth, sigma=2.0)
    weight = gaussian_filter((~nan_mask).astype("float32"), sigma=2.0)
    with np.errstate(invalid="ignore", divide="ignore"):
        arr_smooth = np.where(weight > 0.05, arr_smooth / weight, np.nan)

    bounds = [-1.0, -0.30, -0.10, 0.10, 0.30, 1.0]
    band_colors = ["#762a83", "#e7a99a", "#f5f5f5", "#a6cee3", "#1b6ca8"]
    band_labels = [
        "Large loss (more than 30 pp drop)",
        "Some loss (10 to 30 pp drop)",
        "Little change (within 10 pp)",
        "Some gain (10 to 30 pp rise)",
        "Large gain (more than 30 pp rise)",
    ]
    cmap = ListedColormap(band_colors)
    cmap.set_bad(color="#dddddd")
    norm = BoundaryNorm(bounds, ncolors=len(band_colors))

    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    geom_wgs = aggregate.parse_kml_geometry(config.KML_PATHS)
    geom_utm = shapely_transform(transformer.transform, geom_wgs)
    dam_x, dam_y = transformer.transform(config.TREATMENT_LON, config.TREATMENT_LAT)

    fig, ax = plt.subplots(figsize=(8, 7.5), dpi=130)
    ax.imshow(
        arr_smooth, extent=extent, origin="upper",
        cmap=cmap, norm=norm, interpolation="nearest",
    )

    for line in _iter_lines(geom_utm):
        lx, ly = line.xy
        ax.plot(lx, ly, color="cyan", linewidth=2.6, label="River channel")
    ax.plot(dam_x, dam_y, marker="*", markersize=24, color="white",
            markeredgecolor="black", markeredgewidth=1.6, linestyle="",
            label="Sand dam")

    bar_len_m = 200
    bar_x0 = xmax - 280
    bar_y0 = ymin + 60
    ax.plot([bar_x0, bar_x0 + bar_len_m], [bar_y0, bar_y0],
            color="black", linewidth=3)
    ax.text(bar_x0 + bar_len_m / 2, bar_y0 + 25, f"{bar_len_m} m",
            ha="center", va="bottom", fontsize=9,
            bbox=dict(facecolor="white", edgecolor="none", pad=1.5))

    arrow_x = xmin + 60
    arrow_y = ymax - 110
    ax.annotate(
        "N", xy=(arrow_x, arrow_y), xytext=(arrow_x, arrow_y - 110),
        ha="center", fontsize=11, fontweight="bold",
        arrowprops=dict(arrowstyle="->", linewidth=2.0, color="black"),
    )

    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.set_xticks([]); ax.set_yticks([])
    ax.set_aspect("equal")
    ax.set_title(
        f"Where dry-season plant cover changed at the {aoi_label.lower()} site\n"
        f"(post-construction vs pre-construction relative to "
        f"{config.DAM_CONSTRUCTION_DATE.date()})",
        fontsize=11,
    )

    handles = [mpatches.Patch(color=c, label=lbl)
               for c, lbl in zip(band_colors, band_labels)]
    handles.append(plt.Line2D([0], [0], color="cyan", linewidth=2.6,
                              label="River channel"))
    handles.append(plt.Line2D([0], [0], marker="*", markersize=12,
                              color="white", markeredgecolor="black",
                              markeredgewidth=1.2, linestyle="",
                              label="Sand dam"))
    leg = ax.legend(
        handles=handles, loc="center left", bbox_to_anchor=(1.02, 0.5),
        fontsize=9, framealpha=1.0, title="Change in dry-season green cover",
    )
    leg.get_title().set_fontweight("bold")

    fig.text(
        0.5, 0.02,
        "Each square of land is shaded by how much its dry-season green cover changed after the dam was built. "
        "'pp' means percentage points (a 10 pp drop is roughly: from 60 per cent of the time green to 50 per cent). "
        "Light spatial smoothing has been applied to reduce single-pixel noise.",
        ha="center", fontsize=8, style="italic", wrap=True,
    )
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


def render_did_map(arr_did: np.ndarray, out_path: Path) -> None:
    """DiD per-pixel map: treatment_change − control_change."""
    fig, ax = plt.subplots(figsize=(6, 6), dpi=130)
    im = ax.imshow(
        arr_did, vmin=config.NDVI_DIFF_VMIN, vmax=config.NDVI_DIFF_VMAX, cmap="RdBu",
    )
    ax.set_title(
        "Difference-in-differences (treatment − control)\n"
        "Blue: dam AOI gained vegetation more (or lost less) than control",
        fontsize=10,
    )
    ax.set_xticks([]); ax.set_yticks([])
    plt.colorbar(
        im, ax=ax, fraction=0.046, pad=0.04,
        label="Per-pixel DiD (treatment Δ − control Δ)",
    )
    fig.text(
        0.5, 0.02,
        "Pixel (i,j) of treatment is compared to pixel (i,j) of control. "
        "These are NOT the same geographic location.",
        ha="center", fontsize=8, style="italic",
    )
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


# ---------- ring decay chart ----------
def render_corridor_decay(rows: list[dict], out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, 5), dpi=130)
    midpoints = [
        (config.RING_BOUNDS_M[i] + config.RING_BOUNDS_M[i + 1]) / 2
        for i in range(len(rows))
    ]
    widths = [
        (config.RING_BOUNDS_M[i + 1] - config.RING_BOUNDS_M[i]) * 0.85
        for i in range(len(rows))
    ]
    deltas = [r["delta"] if r["delta"] is not None else 0.0 for r in rows]
    n_pixels = [r["n_pixels"] for r in rows]
    colors = ["tab:blue" if d > 0 else "tab:red" for d in deltas]
    bars = ax.bar(midpoints, deltas, width=widths, color=colors,
                  edgecolor="black", linewidth=0.5)
    for bar, d, n in zip(bars, deltas, n_pixels):
        h = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            h + (0.005 if h >= 0 else -0.01),
            f"{d:+.3f}\nn={n}",
            ha="center", va="bottom" if h >= 0 else "top", fontsize=8,
        )
    ax.axhline(0, color="black", linewidth=0.7)
    ax.set_xlabel("Distance from channel (m)")
    ax.set_ylabel(f"Post − pre fraction (NDVI > {config.NDVI_VEG_THRESHOLD})")
    ax.set_title(
        "Distance-decay of vegetation change from the sand dam channel\n"
        "Treatment AOI; positive values indicate higher post-construction vegetation",
        fontsize=11,
    )
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


def render_corridor_decay_comparison(
    rows_left: list[dict],
    rows_right: list[dict],
    label_left: str,
    label_right: str,
    out_path: Path,
) -> None:
    """Grouped-bar comparison of two corridor-decay results across the same rings."""
    n_rings = len(rows_left)
    x = np.arange(n_rings)
    w = 0.4

    deltas_l = [r["delta"] if r["delta"] is not None else 0.0 for r in rows_left]
    deltas_r = [r["delta"] if r["delta"] is not None else 0.0 for r in rows_right]
    n_l = [r["n_pixels"] for r in rows_left]
    n_r = [r["n_pixels"] for r in rows_right]
    ring_labels = [r["ring"] for r in rows_left]

    fig, ax = plt.subplots(figsize=(11, 5.5), dpi=130)
    bars_l = ax.bar(
        x - w / 2, deltas_l, w, color="tab:orange", edgecolor="black",
        linewidth=0.4, label=label_left,
    )
    bars_r = ax.bar(
        x + w / 2, deltas_r, w, color="tab:purple", edgecolor="black",
        linewidth=0.4, label=label_right,
    )
    for bar, d, n in zip(bars_l, deltas_l, n_l):
        h = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            h + (0.005 if h >= 0 else -0.01),
            f"{d:+.3f}\nn={n}",
            ha="center", va="bottom" if h >= 0 else "top", fontsize=7,
        )
    for bar, d, n in zip(bars_r, deltas_r, n_r):
        h = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            h + (0.005 if h >= 0 else -0.01),
            f"{d:+.3f}\nn={n}",
            ha="center", va="bottom" if h >= 0 else "top", fontsize=7,
        )
    ax.axhline(0, color="black", linewidth=0.7)
    ax.set_xticks(x)
    ax.set_xticklabels(ring_labels)
    ax.set_xlabel("Distance from channel segment (m)")
    ax.set_ylabel(f"Post − pre fraction (NDVI > {config.NDVI_VEG_THRESHOLD})")
    ax.set_title(
        "Distance-decay by channel segment: upstream vs downstream of the dam wall\n"
        "Within-treatment heterogeneity comparison (not a treatment-control study)",
        fontsize=11,
    )
    ax.legend(loc="lower right", fontsize=9)
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")
