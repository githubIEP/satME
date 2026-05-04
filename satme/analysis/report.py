"""REPORT.md renderer.

Takes the structured analysis output (did_summary dict) and the directory
of rendered figures, and writes a markdown report that mirrors the
sections of the original david/REPORT.md but is auto-generated and so
suitable for any (treatment, control, dam) site.

The narrative is intentionally lean — this is the concept-proof renderer,
not the polished IEP house-style write-up. A human author can take this
and rewrite the prose; the pipeline guarantees the numbers and figures.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _fmt(x: Any, dp: int = 3, suffix: str = "") -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):.{dp}f}{suffix}"
    except (TypeError, ValueError):
        return str(x)


def _fmt_pct(x: Any, dp: int = 1) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x) * 100:.{dp}f}%"
    except (TypeError, ValueError):
        return str(x)


def _fmt_signed(x: Any, dp: int = 3) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):+.{dp}f}"
    except (TypeError, ValueError):
        return str(x)


def _trend_block(t: dict) -> str:
    if "warning" in t:
        return f"- {t['label']}: {t['warning']}"
    sig = "**significant**" if t.get("significant") else "not significant"
    return (
        f"- **{t['label']}**: Theil-Sen slope "
        f"{_fmt_signed(t['slope_per_year'])} / year "
        f"(95% CI [{_fmt(t['ci_lo'])}, {_fmt(t['ci_hi'])}]); "
        f"Mann-Kendall p = {_fmt(t['mann_kendall_p'])} ({sig}). "
        f"Direction: {t['direction']}; n_years = {t['n_years']}."
    )


def _corridor_table(rows: list[dict]) -> str:
    lines = ["| Ring | n pixels | Pre mean | Post mean | Δ |",
             "|---|---|---|---|---|"]
    for r in rows:
        lines.append(
            f"| {r['ring']} | {r['n_pixels']} | "
            f"{_fmt(r.get('pre_mean'))} | {_fmt(r.get('post_mean'))} | "
            f"{_fmt_signed(r.get('delta'))} |"
        )
    return "\n".join(lines)


def _mw_block(mw: dict) -> str:
    out = []
    for name, b in mw.items():
        if "warning" in b:
            out.append(f"- {name}: {b['warning']}")
            continue
        sig = "**significant**" if b.get("significant") else "not significant"
        out.append(
            f"- **{name}**: pre mean = {_fmt(b['pre_mean'])}, "
            f"post mean = {_fmt(b['post_mean'])}, "
            f"Δ = {_fmt_signed(b['delta'])}, "
            f"Mann-Whitney U p = {_fmt(b['p_value'])} ({sig})."
        )
    return "\n".join(out)


def render(summary: dict, figs_dir: Path, out_path: Path) -> None:
    """Write a markdown report from a did_summary dict + rendered figures."""
    cfg = summary["config"]
    n   = summary["n_scenes"]
    tr  = summary["trend"]
    px  = summary["pixel_did"]
    rows = summary["corridor_summary"]
    seg = summary.get("corridor_by_segment") or {}
    mw  = summary.get("mannwhitney") or {}
    pp  = summary["precipitation"]

    treat = cfg["treatment"]
    ctrl  = cfg["control"]

    # Use POSIX paths in markdown so they render on any platform.
    def _img(name: str) -> str:
        path = figs_dir / name
        if not path.exists():
            return f"_(figure missing: {name})_"
        return f"![{name}]({path.name})"

    md = f"""# Sand Dam Vegetation Impact Assessment

**Treatment AOI:** {treat['name']} ({treat['lat']}, {treat['lon']})
**Control AOI:** {ctrl['name']} ({ctrl['lat']}, {ctrl['lon']})
**AOI half-size:** {cfg['half_size_m']} m
**Dam construction date:** {cfg['construction_date']}
**Drought window:** {cfg['drought_window'][0]} → {cfg['drought_window'][1]}
**Dry-season months:** {cfg.get('dry_season_months') or 'all months'}
**NDVI vegetation threshold:** {cfg['ndvi_threshold']}

---

## Executive summary

Auto-generated assessment of vegetation change inside a sand-dam treatment AOI
relative to a paired control AOI, using Sentinel-2 surface reflectance from
Google Earth Engine and CHIRPS daily precipitation. Pre/post comparison is
defined relative to the dam construction date above.

### Headline numbers

- **Treatment AOI mean per-pixel change in dry-season vegetation frequency:** {_fmt_signed(px.get('treatment_mean_diff'))}
- **Control AOI mean per-pixel change:** {_fmt_signed(px.get('control_mean_diff'))}
- **Difference-in-differences (DiD) mean:** {_fmt_signed(px.get('did_mean'))}
- **DiD pixel directionality:** {px.get('did_pixels_positive', 0)} positive vs {px.get('did_pixels_negative', 0)} negative (of {px.get('did_pixels_total_usable', 0)} usable pixels)

### Trend tests on annual median dry-season vegetation fraction

{_trend_block(tr['treatment'])}
{_trend_block(tr['control'])}

### Mann-Whitney U on annual medians (pre vs post)

{_mw_block(mw) if mw else '_(not computed — insufficient annual data)_'}

---

## 1. Site and methodology

A square AOI ({2*cfg['half_size_m']:.0f} m × {2*cfg['half_size_m']:.0f} m) was placed over each
site. Sentinel-2 L2A scenes for both AOIs were filtered for tile-level cloud
cover (≤ {summary['config'].get('max_scene_cloud', 80)}%) and for AOI-level
clear-pixel fraction (≥ {summary['config']['min_valid_fraction']}, using the
Scene Classification Layer). For each kept scene, NDVI and MNDWI were computed
per 10 m pixel and a vegetation indicator (NDVI > {cfg['ndvi_threshold']})
was tabulated.

To control for seasonality, scenes were grouped into {cfg['doy_bin_size']}-day
day-of-year bins. The pre/post comparison averages each pixel's vegetation
indicator within bins, then averages across bins.

{_img('site_methodology.png')}

---

## 2. Annual dry-season NDVI

{_img('yearly_ndvi_grid.png')}

---

## 3. Per-pixel change

### 3.1 Treatment AOI

{_img('per_pixel_change_treatment.png')}

### 3.2 Control AOI (technical view)

{_img('per_pixel_change_control.png')}

### 3.3 Difference-in-differences (treatment − control)

{_img('per_pixel_did.png')}

Per-pixel DiD compares pixel (i, j) of the treatment AOI to pixel (i, j) of
the control AOI; these are not the same geographic location. The aggregate
DiD scalar is the mean across all usable pixels:
**{_fmt_signed(px.get('did_mean'))}** ({px.get('did_pixels_total_usable', 0)} usable pixels).

---

## 4. Distance-decay corridor analysis

### 4.1 All channel segments combined

{_img('corridor_decay.png')}

{_corridor_table(rows)}

"""

    if seg.get("upstream") and seg.get("downstream"):
        md += f"""### 4.2 Upstream vs downstream of dam wall

{_img('corridor_decay_by_segment.png')}

#### Upstream segment

{_corridor_table(seg['upstream'])}

#### Downstream segment

{_corridor_table(seg['downstream'])}

"""

    md += f"""---

## 5. Time series and rainfall context

{_img('timeseries_with_precip.png')}

- Pre-drought mean rainfall: {_fmt(pp.get('pre_drought_mm_per_year'), dp=1, suffix=' mm/year')}
- Drought-window mean rainfall: {_fmt(pp.get('drought_mm_per_year'), dp=1, suffix=' mm/year')}
- Source: {pp.get('source', 'CHIRPS via GEE')}

---

## 6. Scene counts

| AOI | All scenes | Dry-season scenes |
|---|---|---|
| {treat['name']} | {n['treatment_all']} | {n['treatment_dry_season']} |
| {ctrl['name']} | {n['control_all']} | {n['control_dry_season']} |

---

## 7. Limitations

- The control AOI is a single nearby site; pre-trend equivalence has not been
  validated against a pool of candidate controls.
- NDVI saturates at high vegetation density and is sensitive to soil moisture
  in semi-arid landscapes.
- Pre/post scene counts are unequal; trend tests on annual medians have low
  statistical power with few years.
- No ground-truth campaign has verified the remote-sensing inferences.

---

## Configuration

```json
{__import__('json').dumps(cfg, indent=2)}
```
"""

    out_path.write_text(md)
