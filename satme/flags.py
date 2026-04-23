"""Image-level flagging logic.

Every image that passes the NO_DATA check gets a flags list in the output.
Flags are additive — an image can carry multiple flags simultaneously.
No image is silently excluded except those with the NO_DATA flag.

Flag codes
----------
OUT_OF_SEASON       Image month not in config season.target_months
HIGH_TILE_CLOUD     Tile-level cloud % > sources.<src>.max_tile_cloud_pct
HIGH_AOI_CLOUD      AOI-level cloud % > sources.<src>.max_aoi_cloud_pct
PRE_INTERVENTION    Image date before run.reference_date
POST_INTERVENTION   Image date on or after run.reference_date
NEAR_INTERVENTION   Within 60 days of run.reference_date — treat with caution
PARTIAL_AOI_COVERAGE Image footprint does not fully cover the AOI
NO_DATA             All AOI pixels masked after cloud removal — image excluded
"""

from datetime import date, timedelta

NEAR_INTERVENTION_DAYS = 60


def assign_flags(
    image_date: date,
    tile_cloud_pct: float | None,
    aoi_cloud_pct: float | None,
    aoi_fully_covered: bool,
    cfg: dict,
) -> list[str]:
    """Assign all applicable flag codes to an image.

    Parameters
    ----------
    image_date:
        The sensing date of the image.
    tile_cloud_pct:
        Tile-level cloud percentage from image metadata (0–100).  None if unknown.
    aoi_cloud_pct:
        AOI-level cloud percentage computed from pixel counts (0–100).  None = NO_DATA.
    aoi_fully_covered:
        Whether the image footprint fully covers the AOI.
    cfg:
        The full parsed config dict.

    Returns
    -------
    list[str]
        Ordered list of flag codes.  Empty list means a clean image.
    """
    flags = []
    src_cfg = cfg["sources"]
    season_cfg = cfg.get("season", {})
    run_cfg = cfg["run"]

    # --- NO_DATA: all AOI pixels masked ---
    if aoi_cloud_pct is None:
        flags.append("NO_DATA")
        return flags  # nothing else is meaningful

    # --- Season check ---
    target_months = season_cfg.get("target_months", [])
    if target_months and image_date.month not in target_months:
        flags.append("OUT_OF_SEASON")

    # --- Tile-level cloud ---
    for src_name, src in src_cfg.items():
        if not src.get("enabled", False):
            continue
        threshold = src.get("max_tile_cloud_pct")
        if threshold is not None and tile_cloud_pct is not None:
            if tile_cloud_pct > threshold:
                flags.append("HIGH_TILE_CLOUD")
                break  # only one source is active per image

    # --- AOI-level cloud ---
    for src_name, src in src_cfg.items():
        if not src.get("enabled", False):
            continue
        threshold = src.get("max_aoi_cloud_pct")
        if threshold is not None:
            if aoi_cloud_pct > threshold:
                flags.append("HIGH_AOI_CLOUD")
                break

    # --- Intervention timing ---
    reference_date = date.fromisoformat(run_cfg["reference_date"])
    delta = abs((image_date - reference_date).days)

    if image_date < reference_date:
        flags.append("PRE_INTERVENTION")
    else:
        flags.append("POST_INTERVENTION")

    if delta <= NEAR_INTERVENTION_DAYS:
        flags.append("NEAR_INTERVENTION")

    # --- Spatial coverage ---
    if not aoi_fully_covered:
        flags.append("PARTIAL_AOI_COVERAGE")

    return flags


def is_excluded(flags: list[str], cfg: dict) -> bool:
    """Determine whether an image should be excluded from download and stats.

    Rules:
      - NO_DATA images are always excluded.
      - OUT_OF_SEASON images are excluded only if season.flag_only is False.
      - HIGH_TILE_CLOUD / HIGH_AOI_CLOUD are always processed (flagged, not excluded).

    Parameters
    ----------
    flags:
        Flag list from assign_flags().
    cfg:
        Full config dict.

    Returns
    -------
    bool
        True if the image should be skipped entirely.
    """
    if "NO_DATA" in flags:
        return True

    flag_only = cfg.get("season", {}).get("flag_only", True)
    if not flag_only and "OUT_OF_SEASON" in flags:
        return True

    return False


def flags_to_string(flags: list[str]) -> str:
    """Serialise flag list to a pipe-separated string for CSV output."""
    return "|".join(flags) if flags else ""
