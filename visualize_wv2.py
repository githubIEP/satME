"""
visualize_wv2.py — WorldView-2 multi-index comparison: Feb vs May 2025

Computes all spectral indices available from WV2's 8 bands and produces
a side-by-side grid: Feb 2025 (dry) | May 2025 (wet) | Difference (May − Feb).

Usage:
    python visualize_wv2.py

Output:
    outputs/runs/busia_4km_full/busia_wv2_full_comparison.png
    outputs/runs/busia_4km_full/busia_wv2_index_stats.csv

═══════════════════════════════════════════════════════════════════
WORLDVIEW-2 BAND REFERENCE
═══════════════════════════════════════════════════════════════════
  Band 1  CB   Coastal Blue  ~425 nm   aerosol / shallow water
  Band 2  B    Blue          ~480 nm   true colour, bathymetry
  Band 3  G    Green         ~545 nm   vegetation peak, water
  Band 4  Y    Yellow        ~605 nm   carotenoids, yellowing
  Band 5  R    Red           ~660 nm   chlorophyll absorption
  Band 6  RE   Red Edge      ~725 nm   chlorophyll concentration
  Band 7  N    NIR1          ~833 nm   biomass, moisture
  Band 8  N2   NIR2          ~950 nm   atmospheric penetration

═══════════════════════════════════════════════════════════════════
INDICES COMPUTED
═══════════════════════════════════════════════════════════════════
Ratio indices (DN used directly — scale factor cancels in division):
  NDVI  = (N − R) / (N + R)
          Vegetation health. Range −1 to +1.
          >0.6 dense crops | 0.2–0.5 sparse | <0.2 bare/built

  GNDVI = (N − G) / (N + G)
          Green NDVI. Less chlorophyll-saturated than NDVI at high
          biomass — better differentiator between healthy canopies.

  NDWI  = (G − N) / (G + N)
          Surface water and canopy moisture. Positive = open water.
          More negative = drier vegetation / bare soil.

  NDRE  = (N − RE) / (N + RE)
          Red-Edge NDVI. WV2-exclusive at this resolution.
          Detects chlorophyll stress earlier than NDVI.

  CIre  = (N / RE) − 1
          Red Edge Chlorophyll Index. Linearly proportional to
          leaf chlorophyll content (μg/cm²). Higher = more chlorophyll.

  NDYI  = (Y − CB) / (Y + CB)
          Yellowing Index. WV2-exclusive. Elevated values indicate
          crop canopy yellowing (nutrient stress, senescence, ripening).

  PSRI  = (R − B) / N
          Plant Senescence Reflectance Index. Tracks carotenoid-to-
          chlorophyll ratio. Rises as vegetation senesces / dies back.
          Useful for timing harvest windows.

  WVI   = (N − N2) / (N + N2)
          WV2 Water Vapour Index. Uses the NIR1/NIR2 ratio to sense
          atmospheric water vapour and canopy liquid water.

Normalised indices (DN divided by 2047 → ~reflectance [0,1]):
  EVI   = 2.5 × (N − R) / (N + 6R − 7.5B + 1)
          Enhanced Vegetation Index. Reduces soil and atmospheric
          background effects. Doesn't saturate over dense canopy.

  SAVI  = ((N − R) / (N + R + 0.5)) × 1.5
          Soil-Adjusted Vegetation Index (L=0.5). Corrects NDVI for
          soil brightness. Useful where bare soil is partially exposed
          (dry season, early season, low plant cover).

  BSI   = ((R + Y) − (N + CB)) / ((R + Y) + (N + CB))
          Bare Soil Index adapted for WV2 (uses Yellow instead of SWIR).
          High = exposed bare soil. Low/negative = vegetated.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import rasterio
from rasterio.warp import reproject, Resampling
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
WV2_DIR = Path("data/wv2")
OUT_DIR = Path("outputs/runs/busia_4km_full")
DN_MAX  = 2047.0   # WV2 11-bit maximum — used to normalise to [0,1] for EVI/SAVI

EPS = 1e-6

# Each entry: (display name, colormap, vmin, vmax, unit label)
INDEX_DISPLAY = {
    "NDVI":  ("NDVI\n(NIR1−Red)/(NIR1+Red)",           "RdYlGn",  -0.2,  0.9,  ""),
    "GNDVI": ("GNDVI\n(NIR1−Green)/(NIR1+Green)",       "RdYlGn",  -0.1,  0.8,  ""),
    "NDWI":  ("NDWI\n(Green−NIR1)/(Green+NIR1)",        "RdYlBu_r", -0.7,  0.2, ""),
    "NDRE":  ("NDRE\n(NIR1−RedEdge)/(NIR1+RedEdge)",    "PiYG",    -0.1,  0.7,  ""),
    "CIre":  ("CIre\n(NIR1/RedEdge)−1",                 "YlGn",     0.0,  3.0,  ""),
    "NDYI":  ("NDYI\n(Yellow−CoastalBlue)/(Y+CB)",      "RdYlGn",  -0.2,  0.6,  ""),
    "PSRI":  ("PSRI\n(Red−Blue)/NIR1",                  "RdBu_r",  -0.1,  0.4,  ""),
    "WVI":   ("WVI\n(NIR1−NIR2)/(NIR1+NIR2)",           "BrBG",    -0.3,  0.3,  ""),
    "EVI":   ("EVI\n2.5×(N−R)/(N+6R−7.5B+1)",          "RdYlGn",  -0.2,  0.9,  ""),
    "SAVI":  ("SAVI\n((N−R)/(N+R+0.5))×1.5",            "RdYlGn",  -0.3,  1.0,  ""),
    "BSI":   ("BSI\n((R+Y)−(N+CB))/((R+Y)+(N+CB))",    "RdYlBu",  -0.5,  0.5,  ""),
}

# Difference plot uses a diverging palette centred on zero
DIFF_CMAP  = "RdBu"
DIFF_RANGE = 0.3   # ± range for the difference column


# ── Load one image and compute all indices ────────────────────────────────────
def compute_indices(tif_path: Path, ref_profile=None) -> tuple[dict, dict, dict]:
    """
    Returns (arrays_dict, stats_dict, profile).
    If ref_profile is provided, reprojects all bands onto that grid first
    (used to align images that have slightly different pixel grids).
    """
    with rasterio.open(tif_path) as src:
        profile = src.profile
        if ref_profile is None or (src.height == ref_profile["height"] and
                                    src.width  == ref_profile["width"]):
            CB = src.read(1).astype(np.float32)
            B  = src.read(2).astype(np.float32)
            G  = src.read(3).astype(np.float32)
            Y  = src.read(4).astype(np.float32)
            R  = src.read(5).astype(np.float32)
            RE = src.read(6).astype(np.float32)
            N  = src.read(7).astype(np.float32)
            N2 = src.read(8).astype(np.float32)
        else:
            # Reproject onto reference grid
            def _reproj(band_idx):
                dst = np.zeros((ref_profile["height"], ref_profile["width"]), dtype=np.float32)
                reproject(
                    source=src.read(band_idx).astype(np.float32),
                    destination=dst,
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=ref_profile["transform"],
                    dst_crs=ref_profile["crs"],
                    resampling=Resampling.bilinear,
                )
                return dst
            CB, B, G, Y, R, RE, N, N2 = [_reproj(i) for i in range(1, 9)]

    # Valid pixel mask (DN=0 means no data)
    valid = (CB > 0) & (B > 0) & (G > 0) & (Y > 0) & (R > 0) & (RE > 0) & (N > 0) & (N2 > 0)

    # Normalised bands for indices that require reflectance scale
    cb = CB / DN_MAX;  b = B / DN_MAX;  g = G / DN_MAX
    y  = Y  / DN_MAX;  r = R / DN_MAX;  re = RE / DN_MAX
    n  = N  / DN_MAX;  n2 = N2 / DN_MAX

    def masked(expr):
        return np.where(valid, expr, np.nan)

    arrays = {
        # ── Ratio indices (raw DN — scale cancels) ──────────────────────────
        "NDVI":  masked((N  - R)  / (N  + R  + EPS)),
        "GNDVI": masked((N  - G)  / (N  + G  + EPS)),
        "NDWI":  masked((G  - N)  / (G  + N  + EPS)),
        "NDRE":  masked((N  - RE) / (N  + RE + EPS)),
        "CIre":  masked((N  / (RE + EPS)) - 1),
        "NDYI":  masked((Y  - CB) / (Y  + CB + EPS)),
        "PSRI":  masked((R  - B)  / (N  + EPS)),
        "WVI":   masked((N  - N2) / (N  + N2 + EPS)),
        # ── Normalised indices (use reflectance-scale bands) ─────────────────
        "EVI":   masked(2.5 * (n - r) / (n + 6*r - 7.5*b + 1 + EPS)),
        "SAVI":  masked(((n - r) / (n + r + 0.5 + EPS)) * 1.5),
        "BSI":   masked(((r + y) - (n + cb)) / ((r + y) + (n + cb) + EPS)),
    }

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

    stats = {k: _stats(v) for k, v in arrays.items()}
    return arrays, stats, profile


# ── Discover and load images ──────────────────────────────────────────────────
tifs = sorted(WV2_DIR.glob("*-M2AS-*.TIF"))
if len(tifs) < 2:
    raise FileNotFoundError(f"Need at least 2 M2AS TIF files in {WV2_DIR}, found {len(tifs)}")

print(f"Found {len(tifs)} WV2 images:")
loaded = []
ref_profile = None
for tif in tifs:
    stem = tif.stem
    date = pd.to_datetime(stem[:7], format="%y%b%d")
    label = date.strftime("%d %b %Y")
    print(f"  {label}  —  {tif.name}")
    arrays, stats, profile = compute_indices(tif, ref_profile)
    if ref_profile is None:
        ref_profile = profile   # first image sets the reference grid
    loaded.append({"date": date, "label": label, "arrays": arrays, "stats": stats})

loaded.sort(key=lambda x: x["date"])
img_a, img_b = loaded[0], loaded[1]   # chronological: Feb = dry, May = wet

print(f"\nComparing:  [{img_a['label']}]  vs  [{img_b['label']}]")

# ── Build stats CSV ───────────────────────────────────────────────────────────
rows = []
for img in loaded:
    for idx, s in img["stats"].items():
        rows.append({"date": img["label"], "index": idx, **s})
stats_df = pd.DataFrame(rows)
out_csv = OUT_DIR / "busia_wv2_index_stats.csv"
stats_df.to_csv(out_csv, index=False)
print(f"Stats saved: {out_csv}")

# ── Plot: rows = indices, cols = [Feb | May | Diff] ──────────────────────────
n_idx = len(INDEX_DISPLAY)
fig, axes = plt.subplots(
    n_idx, 3,
    figsize=(15, 4.2 * n_idx),
    gridspec_kw={"wspace": 0.05, "hspace": 0.35},
)

col_titles = [img_a["label"] + "  (dry)", img_b["label"] + "  (wet)", "Difference\n(May − Feb)"]
for col, title in enumerate(col_titles):
    axes[0, col].set_title(title, fontsize=12, fontweight="bold", pad=8)

for row, (idx, (display_name, cmap, vmin, vmax, _)) in enumerate(INDEX_DISPLAY.items()):
    arr_a   = img_a["arrays"][idx]
    arr_b   = img_b["arrays"][idx]
    diff    = np.where(~np.isnan(arr_a) & ~np.isnan(arr_b), arr_b - arr_a, np.nan)

    s_a = img_a["stats"][idx]
    s_b = img_b["stats"][idx]
    d50 = s_b["p50"] - s_a["p50"]

    for col, (arr, cm, lo, hi) in enumerate([
        (arr_a, cmap,      vmin,       vmax),
        (arr_b, cmap,      vmin,       vmax),
        (diff,  DIFF_CMAP, -DIFF_RANGE, DIFF_RANGE),
    ]):
        ax = axes[row, col]
        im = ax.imshow(arr, cmap=cm, vmin=lo, vmax=hi, interpolation="nearest")
        ax.axis("off")
        fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02,
                     format=ticker.FormatStrFormatter("%.2f")).ax.tick_params(labelsize=6)

        if col == 0:
            # Index label on left edge
            ax.text(-0.04, 0.5, display_name, transform=ax.transAxes,
                    fontsize=7.5, va="center", ha="right", rotation=0,
                    multialignment="right",
                    bbox=dict(boxstyle="round,pad=0.3", fc="#f0f0f0", alpha=0.8))
            stat_txt = f"med={s_a['p50']:.3f}\np25={s_a['p25']:.3f}\np75={s_a['p75']:.3f}"
        elif col == 1:
            stat_txt = f"med={s_b['p50']:.3f}\np25={s_b['p25']:.3f}\np75={s_b['p75']:.3f}"
        else:
            sign = "+" if d50 >= 0 else ""
            stat_txt = f"Δmed={sign}{d50:.3f}"

        ax.text(0.02, 0.03, stat_txt, transform=ax.transAxes,
                fontsize=6.5, color="white", va="bottom",
                bbox=dict(boxstyle="round,pad=0.25", fc="black", alpha=0.55))

fig.suptitle(
    f"WorldView-2 Full Index Comparison — Busia AOI\n"
    f"{img_a['label']} (dry)  vs  {img_b['label']} (wet)",
    fontsize=14, y=1.002
)

out_fig = OUT_DIR / "busia_wv2_full_comparison.png"
plt.savefig(out_fig, dpi=150, bbox_inches="tight")
plt.close()
print(f"Figure saved: {out_fig}")

# ── Console summary ───────────────────────────────────────────────────────────
print("\n" + "=" * 62)
print(f"  {'Index':<8}  {img_a['label']:>14}  {img_b['label']:>14}  {'Δ (wet−dry)':>12}")
print("  " + "-" * 58)
for idx in INDEX_DISPLAY:
    a = img_a["stats"][idx]["p50"]
    b = img_b["stats"][idx]["p50"]
    d = b - a
    sign = "+" if d >= 0 else ""
    print(f"  {idx:<8}  {a:>14.3f}  {b:>14.3f}  {sign}{d:>11.3f}")
print("=" * 62)
print("\nInterpretation guide:")
print("  NDVI / GNDVI / NDRE / CIre / EVI / SAVI — higher = more/healthier vegetation")
print("  NDWI — less negative = wetter / more surface water")
print("  NDYI / PSRI — higher = more yellowing / senescence")
print("  WVI  — positive = NIR1 > NIR2 (more leaf water absorption at NIR2)")
print("  BSI  — higher = more bare soil exposed")
