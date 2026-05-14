"""
analyze_wv2.py — WorldView-2 vs Sentinel-2 comparison for the Busia AOI.

Processes all MUL (multispectral) TIF files found in data/wv2/ and compares
them against the busia_4km_full Sentinel-2 GEE time series.

Usage:
    python analyze_wv2.py

Outputs (written to outputs/runs/busia_4km_full/):
    busia_wv2_index_maps.png       — per-image NDVI / NDWI / NDRE maps
    busia_wv2_season_comparison.png — side-by-side dry vs wet index maps
    busia_wv2_vs_s2_timeseries.png  — all WV2 points overlaid on S2 series
    busia_wv2_summary.txt           — printed stats table

BAND ORDER (WorldView-2, from IMD):
    Band 1  C   Coastal Blue  ~425 nm
    Band 2  B   Blue          ~480 nm
    Band 3  G   Green         ~545 nm   <- NDWI numerator
    Band 4  Y   Yellow        ~605 nm
    Band 5  R   Red           ~660 nm   <- NDVI denominator
    Band 6  RE  Red Edge      ~725 nm   <- NDRE denominator
    Band 7  N   NIR1          ~833 nm   <- NDVI numerator, NDWI denominator
    Band 8  N2  NIR2          ~950 nm

INDICES:
    NDVI = (NIR1 - Red)    / (NIR1 + Red)    — vegetation health
    NDWI = (Green - NIR1)  / (Green + NIR1)  — surface water / canopy moisture
    NDRE = (NIR1 - RedEdge)/ (NIR1 + RedEdge)— chlorophyll / stress (WV2-exclusive)

WHY RAW DN VALUES ARE FINE FOR RATIO INDICES:
    All bands share the same gain/offset, so any scaling constant k cancels:
        (k·NIR - k·Red) / (k·NIR + k·Red)  =  (NIR - Red) / (NIR + Red)
    No reflectance conversion is needed for NDVI, NDWI, or NDRE.

COMPARISON NOTE:
    WV2 images are from Feb and May 2025 — beyond the S2 window (2019-2024).
    We compare each WV2 point against same-month S2 observations as the
    closest seasonal analogue.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import rasterio
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
WV2_DIR = Path("data/wv2")
S2_CSV  = Path("outputs/runs/busia_4km_full/stats.csv")
OUT_DIR = Path("outputs/runs/busia_4km_full")

# Shared colormap scales — fixed so images are visually comparable
SCALES = {
    "NDVI": ("RdYlGn", -0.2, 0.9),
    "NDWI": ("RdYlBu", -0.7, 0.2),
    "NDRE": ("PiYG",   -0.1, 0.7),
}

EPS = 1e-6   # prevents division by zero


# ── Helper: load one WV2 image and compute indices ────────────────────────────
def load_wv2(tif_path: Path) -> dict:
    """
    Open a WV2 MUL TIF and return index arrays + stats.
    Returns a dict with keys: label, date, arrays (NDVI/NDWI/NDRE), stats.
    """
    with rasterio.open(tif_path) as src:
        G  = src.read(3).astype(np.float32)   # Green
        R  = src.read(5).astype(np.float32)   # Red
        RE = src.read(6).astype(np.float32)   # Red Edge
        N  = src.read(7).astype(np.float32)   # NIR1
        res = src.res[0]
        shape = (src.height, src.width)

    # Mask edge pixels where any band is zero (no-data)
    valid = (G > 0) & (R > 0) & (N > 0) & (RE > 0)

    NDVI = np.where(valid, (N - R)  / (N + R  + EPS), np.nan)
    NDWI = np.where(valid, (G - N)  / (G + N  + EPS), np.nan)
    NDRE = np.where(valid, (N - RE) / (N + RE + EPS), np.nan)

    def _stats(arr):
        v = arr[~np.isnan(arr)]
        return {
            "mean": float(np.mean(v)),
            "std":  float(np.std(v)),
            "p10":  float(np.percentile(v, 10)),
            "p25":  float(np.percentile(v, 25)),
            "p50":  float(np.percentile(v, 50)),
            "p75":  float(np.percentile(v, 75)),
            "p90":  float(np.percentile(v, 90)),
        }

    # Parse acquisition date from filename: 25FEB11... or 25MAY27...
    stem = tif_path.stem          # e.g. 25FEB11082219-M2AS-...
    date_str = stem[:7]           # e.g. 25FEB11 or 25MAY27
    date = pd.to_datetime(date_str, format="%y%b%d")

    return {
        "path":   tif_path,
        "date":   date,
        "label":  date.strftime("%d %b %Y"),
        "res_m":  res,
        "shape":  shape,
        "valid_pct": 100 * valid.sum() / valid.size,
        "arrays": {"NDVI": NDVI, "NDWI": NDWI, "NDRE": NDRE},
        "stats":  {"NDVI": _stats(NDVI), "NDWI": _stats(NDWI), "NDRE": _stats(NDRE)},
    }


# ── Step 1: Discover and load all MUL images ──────────────────────────────────
print("Step 1 — Discovering WV2 MUL images in data/wv2/ ...")
tifs = sorted(WV2_DIR.glob("*-M2AS-*.TIF"))
if not tifs:
    raise FileNotFoundError("No M2AS TIF files found in data/wv2/")

images = []
for tif in tifs:
    print(f"  Loading {tif.name} ...")
    img = load_wv2(tif)
    images.append(img)
    print(f"    Date: {img['label']}  |  {img['shape'][0]}×{img['shape'][1]} px  "
          f"@ {img['res_m']:.2f} m  |  Valid: {img['valid_pct']:.1f}%")
    print(f"    NDVI median={img['stats']['NDVI']['p50']:.3f}  "
          f"NDWI median={img['stats']['NDWI']['p50']:.3f}  "
          f"NDRE median={img['stats']['NDRE']['p50']:.3f}")

# Sort chronologically
images.sort(key=lambda x: x["date"])

# ── Step 2: Load Sentinel-2 time series ───────────────────────────────────────
print("\nStep 2 — Loading Sentinel-2 time series ...")
s2 = pd.read_csv(S2_CSV, parse_dates=["date"])
s2 = s2[s2["source"] == "sentinel2"].copy()
print(f"  {len(s2)} Sentinel-2 images, {s2['date'].min().date()} → {s2['date'].max().date()}")


# ── Step 3: Per-image index maps ──────────────────────────────────────────────
print("\nStep 3 — Saving per-image index maps ...")
for img in images:
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle(
        f"WorldView-2 — {img['label']} — Busia AOI ({img['res_m']:.1f} m resolution)",
        fontsize=13
    )
    for ax, (idx, (cmap, vmin, vmax)) in zip(axes, SCALES.items()):
        arr = img["arrays"][idx]
        s   = img["stats"][idx]
        im  = ax.imshow(arr, cmap=cmap, vmin=vmin, vmax=vmax, interpolation="nearest")
        titles = {
            "NDVI": "NDVI  (NIR1−Red)/(NIR1+Red)\nVegetation health",
            "NDWI": "NDWI  (Green−NIR1)/(Green+NIR1)\nSurface water / canopy moisture",
            "NDRE": "NDRE  (NIR1−RedEdge)/(NIR1+RedEdge)\nChlorophyll / vegetation stress",
        }
        ax.set_title(titles[idx], fontsize=9)
        ax.axis("off")
        cb = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        cb.ax.tick_params(labelsize=8)
        ax.text(0.02, 0.02,
                f"median={s['p50']:.3f}\nmean={s['mean']:.3f}\n"
                f"p25={s['p25']:.3f}  p75={s['p75']:.3f}",
                transform=ax.transAxes, fontsize=7.5, color="white", va="bottom",
                bbox=dict(boxstyle="round,pad=0.3", fc="black", alpha=0.5))
    plt.tight_layout()
    slug = img["date"].strftime("%Y%m%d")
    out = OUT_DIR / f"busia_wv2_{slug}_index_maps.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out}")


# ── Step 4: Side-by-side season comparison (all images × all indices) ─────────
print("\nStep 4 — Saving season comparison grid ...")
n_img = len(images)
n_idx = len(SCALES)
fig, axes = plt.subplots(n_idx, n_img, figsize=(7 * n_img, 5 * n_idx))
# Ensure axes is always 2-D
if n_img == 1:
    axes = axes[:, np.newaxis]

fig.suptitle("WorldView-2 — Dry vs Wet Season — Busia AOI", fontsize=14, y=1.01)

for col, img in enumerate(images):
    for row, (idx, (cmap, vmin, vmax)) in enumerate(SCALES.items()):
        ax  = axes[row, col]
        arr = img["arrays"][idx]
        s   = img["stats"][idx]
        im  = ax.imshow(arr, cmap=cmap, vmin=vmin, vmax=vmax, interpolation="nearest")
        ax.axis("off")
        if row == 0:
            ax.set_title(img["label"], fontsize=11, fontweight="bold")
        if col == 0:
            ax.set_ylabel(idx, fontsize=10)
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04).ax.tick_params(labelsize=7)
        ax.text(0.02, 0.02,
                f"median={s['p50']:.3f}",
                transform=ax.transAxes, fontsize=8, color="white", va="bottom",
                bbox=dict(boxstyle="round,pad=0.2", fc="black", alpha=0.55))

plt.tight_layout()
out_comp = OUT_DIR / "busia_wv2_season_comparison.png"
plt.savefig(out_comp, dpi=150, bbox_inches="tight")
plt.close()
print(f"  Saved: {out_comp}")


# ── Step 5: All WV2 points overlaid on S2 time series ────────────────────────
print("\nStep 5 — Saving WV2 vs S2 time series ...")

# Colours and markers per WV2 image
POINT_STYLES = [
    {"color": "darkorange", "marker": "D"},
    {"color": "crimson",    "marker": "s"},
    {"color": "purple",     "marker": "^"},
    {"color": "teal",       "marker": "P"},
]

indices_to_plot = [
    ("NDVI", "NDVI_p50", "NDVI_p25", "NDVI_p75", "#2d6a2d"),
    ("NDWI", "NDWI_p50", "NDWI_p25", "NDWI_p75", "#1a6bb5"),
    ("NDMI", "NDMI_p50", "NDMI_p25", "NDMI_p75", "#8B4513"),
]

# CHIRPS — drop duplicate dates, keep rows that have rainfall data
chirps = s2[s2["chirps_30d_mm"].notna()].drop_duplicates("date").sort_values("date")

n_panels = len(indices_to_plot) + 1   # +1 for CHIRPS
fig, axes = plt.subplots(n_panels, 1,
                         figsize=(16, 4 * len(indices_to_plot) + 2.5),
                         sharex=True,
                         gridspec_kw={"height_ratios": [4] * len(indices_to_plot) + [2]})
fig.suptitle("WorldView-2 vs Sentinel-2 time series — Busia AOI", fontsize=13)

for ax, (idx, med_col, lo_col, hi_col, s2_color) in zip(axes, indices_to_plot):
    # S2 band
    ax.fill_between(s2["date"], s2[lo_col], s2[hi_col],
                    alpha=0.2, color=s2_color, label="S2 p25–p75")
    ax.plot(s2["date"], s2[med_col],
            color=s2_color, lw=1.5, label="S2 median (p50)")

    # WV2 points
    for img, style in zip(images, POINT_STYLES):
        if idx not in img["stats"]:
            continue
        s = img["stats"][idx]
        ax.errorbar(
            img["date"], s["p50"],
            yerr=[[s["p50"] - s["p25"]], [s["p75"] - s["p50"]]],
            fmt=style["marker"], color=style["color"],
            markersize=10, capsize=5, lw=2,
            label=f"WV2 {img['label']}  (median={s['p50']:.3f})",
        )

    ax.set_ylabel(idx, fontsize=10)
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(axis="y", alpha=0.3)
    ax.axhline(0, color="black", lw=0.5, ls="--")

# CHIRPS bar chart
ax_chirps = axes[-1]
ax_chirps.bar(chirps["date"], chirps["chirps_30d_mm"],
              width=4, color="#4393c3", alpha=0.8, label="30-day rainfall (mm)")
ax_chirps.set_ylabel("CHIRPS\n30d mm", fontsize=9)
ax_chirps.legend(fontsize=8, loc="upper left")
ax_chirps.grid(axis="y", alpha=0.3)

# Add mm labels on bars above a threshold to avoid clutter
for _, row in chirps[chirps["chirps_30d_mm"] > chirps["chirps_30d_mm"].quantile(0.75)].iterrows():
    ax_chirps.text(row["date"], row["chirps_30d_mm"] + 2,
                   f"{row['chirps_30d_mm']:.0f}",
                   ha="center", va="bottom", fontsize=5.5, color="#08519c")

axes[-1].xaxis.set_major_locator(mdates.YearLocator())
axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
axes[-1].set_xlabel("Date", fontsize=10)

plt.tight_layout()
out_ts = OUT_DIR / "busia_wv2_vs_s2_timeseries.png"
plt.savefig(out_ts, dpi=150, bbox_inches="tight")
plt.close()
print(f"  Saved: {out_ts}")


# ── Step 6: Summary table ─────────────────────────────────────────────────────
print("\nStep 6 — Summary")
lines = []
lines.append("=" * 65)
lines.append("  WorldView-2 vs Sentinel-2 — Busia AOI")
lines.append("=" * 65)

for img in images:
    month = img["date"].month
    s2_month = s2[s2["date"].dt.month == month]
    lines.append(f"\n  WV2 {img['label']}  ({img['res_m']:.1f} m/px)")
    lines.append(f"  {'Index':<8}  {'WV2 median':>12}  {'S2 same-month median':>20}  {'Diff':>8}")
    lines.append("  " + "-" * 55)
    for idx, s2_col in [("NDVI", "NDVI_p50"), ("NDWI", "NDWI_p50")]:
        wv2_val  = img["stats"][idx]["p50"]
        s2_val   = s2_month[s2_col].median() if len(s2_month) else float("nan")
        diff     = wv2_val - s2_val
        lines.append(f"  {idx:<8}  {wv2_val:>12.3f}  {s2_val:>20.3f}  {diff:>+8.3f}")

lines.append("\n  Notes:")
lines.append("  - WV2 resolves 2 m field-scale variation; S2 averages at 10 m.")
lines.append("  - Some WV2–S2 difference is a resolution/scale effect, not land change.")
lines.append("  - NDRE is WV2-exclusive (no equivalent in standard S2 10 m bands).")
lines.append("=" * 65)

summary = "\n".join(lines)
print(summary)
out_txt = OUT_DIR / "busia_wv2_summary.txt"
out_txt.write_text(summary)
print(f"\n  Saved: {out_txt}")
