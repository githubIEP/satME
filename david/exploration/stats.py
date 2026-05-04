"""Statistical tests: Mann-Kendall, Theil-Sen, Mann-Whitney, DiD scalars."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import kendalltau, mannwhitneyu, theilslopes


def did_block(label: str, df_t: pd.DataFrame, df_c: pd.DataFrame, row_filter=None) -> dict:
    """DiD for vegetation_fraction, vegetation_ha, water_fraction, water_ha."""
    if row_filter is not None:
        df_t = df_t[row_filter(df_t)]
        df_c = df_c[row_filter(df_c)]

    out = {
        "label": label,
        "treatment_n_pre": int((df_t["period"] == "pre").sum()),
        "treatment_n_post": int((df_t["period"] == "post").sum()),
        "control_n_pre": int((df_c["period"] == "pre").sum()),
        "control_n_post": int((df_c["period"] == "post").sum()),
        "metrics": {},
    }
    for col in ["vegetation_fraction", "vegetation_ha", "water_fraction", "water_ha"]:
        pre_t = df_t.loc[df_t["period"] == "pre", col].mean()
        post_t = df_t.loc[df_t["period"] == "post", col].mean()
        pre_c = df_c.loc[df_c["period"] == "pre", col].mean()
        post_c = df_c.loc[df_c["period"] == "post", col].mean()
        delta_t = post_t - pre_t
        delta_c = post_c - pre_c
        did = delta_t - delta_c
        out["metrics"][col] = {
            "treatment_pre": _safe_float(pre_t),
            "treatment_post": _safe_float(post_t),
            "treatment_delta": _safe_float(delta_t),
            "control_pre": _safe_float(pre_c),
            "control_post": _safe_float(post_c),
            "control_delta": _safe_float(delta_c),
            "did": _safe_float(did),
        }
    return out


def _safe_float(x) -> float | None:
    return float(x) if pd.notna(x) else None


def trend_one(label: str, df: pd.DataFrame) -> dict:
    """Mann-Kendall + Theil-Sen on annual median vegetation_fraction."""
    annual = (
        df.groupby("year")
        .agg(median_veg_fraction=("vegetation_fraction", "median"),
             n_scenes=("vegetation_fraction", "count"))
        .dropna()
        .reset_index()
    )
    if len(annual) < 3:
        return {"label": label, "n_years": len(annual),
                "warning": "n<3 annual medians; trend test skipped"}

    xs = annual["year"].to_numpy().astype(float)
    ys = annual["median_veg_fraction"].to_numpy()
    _, p = kendalltau(xs, ys)
    slope, _, lo, hi = theilslopes(ys, xs)
    return {
        "label": label,
        "n_years": int(len(annual)),
        "annual_medians": {int(k): float(v) for k, v in
                           annual.set_index("year")["median_veg_fraction"].to_dict().items()},
        "slope_per_year": float(slope),
        "ci_lo": float(lo),
        "ci_hi": float(hi),
        "mann_kendall_p": float(p),
        "direction": "increased" if slope > 0 else "decreased",
        "significant": bool(p < 0.05),
    }


def mannwhitney_block(df_t: pd.DataFrame, df_c: pd.DataFrame) -> dict:
    """Pre/post Mann-Whitney U on annual median vegetation_fraction, per AOI."""
    out = {}
    for name, df in [("treatment", df_t), ("control", df_c)]:
        annual = (
            df.groupby(["year", "period"])
            .agg(med=("vegetation_fraction", "median"))
            .reset_index()
        )
        pre = annual.loc[annual["period"] == "pre", "med"].to_numpy()
        post = annual.loc[annual["period"] == "post", "med"].to_numpy()
        if len(pre) < 2 or len(post) < 2:
            out[name] = {"n_pre": len(pre), "n_post": len(post),
                         "warning": "n<2 in pre or post"}
            continue
        _, p = mannwhitneyu(post, pre, alternative="two-sided")
        out[name] = {
            "n_pre": int(len(pre)),
            "n_post": int(len(post)),
            "pre_mean": float(pre.mean()),
            "post_mean": float(post.mean()),
            "delta": float(post.mean() - pre.mean()),
            "p_value": float(p),
            "significant": bool(p < 0.05),
        }
    return out


def pixel_summary(arr_t, arr_c, arr_did) -> dict:
    return {
        "treatment_mean_diff": _safe_float(np.nanmean(arr_t)) if np.isfinite(arr_t).any() else None,
        "control_mean_diff": _safe_float(np.nanmean(arr_c)) if np.isfinite(arr_c).any() else None,
        "did_mean": _safe_float(np.nanmean(arr_did)) if np.isfinite(arr_did).any() else None,
        "did_pixels_positive": int(np.nansum(arr_did > 0)),
        "did_pixels_negative": int(np.nansum(arr_did < 0)),
        "did_pixels_total_usable": int(np.isfinite(arr_did).sum()),
    }
