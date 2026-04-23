"""Pipeline orchestrator — runs the full satME data pipeline in three phases.

Phase 1 — Filter (query + cloud-filter every source, collect counts)
---------------------------------------------------------------------
For each enabled source the pipeline queries GEE, applies the tile-level and
AOI-level cloud filters, and fetches batch metadata.  Results are printed as
a coverage table so the user can verify image counts before committing to the
full compute.  Confirmation is requested here, after the filter summary.

Phase 2 — Compute (statistics + CSV outputs)
--------------------------------------------
Uses the already-filtered collections from Phase 1.  Computes spectral index
statistics and CHIRPS rainfall in batch GEE calls, builds CSV rows in Python,
and writes stats.csv / flag_report.csv / run_metadata.json.  CSV outputs are
written before any GeoTIFF downloads so tabular data is safe even if downloads
fail or are cancelled mid-run.

Phase 3 — Download (GeoTIFF exports, optional)
-----------------------------------------------
Triggered only when export_geotiff: true for at least one source.  Each file
is one HTTP request; failures are logged individually and do not stop the run.

Round-trip count (N_total=300, N_clean=135, 3 indices)
-------------------------------------------------------
Stage                        Round-trips
─────────────────────────────────────────
Auth + AOI                         ~3
Phase 1 metadata (per source)       2   (full + clean batch)
Phase 2 stats                       1
Phase 2 CHIRPS                      1
Phase 3 GeoTIFF downloads         405   (per file, unavoidable)
─────────────────────────────────────────
Total (no GeoTIFFs)               ~9
Total (with GeoTIFFs)            ~414
"""

import calendar
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import ee
import pandas as pd
from tqdm import tqdm

from satme import auth, aoi as aoi_module, estimator, copernicus_auth
from satme.sources import copernicus_s2 as cdse
from satme.flags import assign_flags, is_excluded, flags_to_string
from satme.image_filter import (
    prefilter_by_aoi_cloud,
    batch_image_metadata,
    is_in_season,
    collection_size,
)
from satme.stats import (
    map_stats_over_collection,
    fetch_stats_batch,
    map_stats_multi_tile,
    fetch_stats_multi_tile_batch,
    build_csv_row,
    rows_to_dataframe,
)
from satme import downloader

logger = logging.getLogger(__name__)

# Approximate revisit interval used for the offline "est. available" column in
# the filter summary.  These are conservative single-location estimates:
#   Sentinel-2  : 5 days  (S2A + S2B, equatorial overlap)
#   Sentinel-1  : 6 days  (S1A + S1B, same orbit direction)
#   VIIRS       : 30 days (monthly composites, one per month)
#   Landsat     : 8 days  (L8 + L9 combined; 16 days each)
#   Planet      : 1 day   (daily if licensed)
_REVISIT_DAYS: dict[str, int] = {
    "sentinel2": 5,
    "sentinel1": 6,
    "viirs":     30,
    "landsat":   8,
    "planet":    1,
}


def _resolve_source(source_name: str, src_cfg: dict):
    if source_name == "sentinel2":
        from satme.sources.sentinel2 import Sentinel2Source
        return Sentinel2Source(src_cfg)
    if source_name == "landsat":
        from satme.sources.landsat import LandsatSource
        return LandsatSource(src_cfg)
    if source_name == "sentinel1":
        from satme.sources.sentinel1 import Sentinel1Source
        return Sentinel1Source(src_cfg)
    if source_name == "viirs":
        from satme.sources.viirs import VIIRSSource
        return VIIRSSource(src_cfg)
    if source_name == "planet":
        from satme.sources.planet import PlanetSource
        return PlanetSource(src_cfg)
    raise ValueError(f"Unknown source: {source_name}")


def _output_dir(cfg: dict) -> Path:
    base = cfg["output"].get("base_dir", "outputs/runs")
    return Path(base) / cfg["run"]["name"]


def run(cfg: dict, skip_confirm: bool = False) -> Path:
    """Execute the full pipeline in three phases.

    Phase 1  Filter   — query + cloud-filter every source, print coverage table
    Phase 2  Compute  — statistics, CHIRPS, CSV outputs
    Phase 3  Download — GeoTIFF exports (only if export_geotiff: true)
    """
    run_name = cfg["run"]["name"]
    out_dir  = _output_dir(cfg)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Auth + AOI ────────────────────────────────────────────────────────
    gee_project = cfg.get("auth", {}).get("gee_project")
    service_key = cfg.get("auth", {}).get("service_account_key")
    auth.initialise(project_id=gee_project, service_account_key=service_key)
    conn = auth.verify_connection()
    logger.info("GEE connection OK — %d algorithms available", conn["algorithm_count"])

    geometry, aoi_meta = aoi_module.build(cfg)
    logger.info("AOI built — area=%.4f km²", aoi_meta["area_km2"])

    # ── Surrounding tiles (point_radius only) ─────────────────────────────
    aoi_cfg = cfg["aoi"]
    surrounding_boxes = (
        aoi_cfg.get("surrounding_boxes", False)
        and aoi_cfg.get("mode") == "point_radius"
    )
    if surrounding_boxes:
        tiles       = aoi_module.build_tiles(cfg)
        ee_tiles_fc = aoi_module.build_ee_tiles_fc(tiles)
        filter_geom = aoi_module.build_full_extent(tiles)
        aoi_meta["surrounding_boxes"] = True
        aoi_meta["tile_labels"] = [label for label, _ in tiles]
        logger.info("Surrounding boxes enabled — 3×3 grid (%d tiles)", len(tiles))
    else:
        tiles       = [("center", geometry)]
        ee_tiles_fc = None
        filter_geom = geometry

    # ── Shared config values ───────────────────────────────────────────────
    date_range     = cfg["date_range"]
    season_cfg     = cfg.get("season", {})
    stats_cfg      = cfg.get("stats", {})
    output_cfg     = cfg.get("output", {})
    reference_date = date.fromisoformat(cfg["run"]["reference_date"])
    target_months  = season_cfg.get("target_months", [])
    flag_only      = season_cfg.get("flag_only", True)

    # ── Copernicus fallback token + session (built once, shared across sources)
    _cdse_token_mgr, _cdse_session = copernicus_auth.from_cfg(cfg)
    _aoi_bounds = aoi_module.build_bounds(cfg)  # (W, S, E, N) — no EE call

    # ── CHIRPS source ─────────────────────────────────────────────────────
    chirps_source = None
    chirps_days   = 30
    if cfg["sources"].get("chirps", {}).get("enabled", False):
        from satme.sources.chirps import ChirpsSource
        chirps_source = ChirpsSource(cfg["sources"]["chirps"])
        chirps_days   = chirps_source.accumulation_days
        logger.info("CHIRPS enabled — %d-day accumulation", chirps_days)

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 1 — Filter: query + cloud-filter each source, collect counts
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'─' * 65}")
    print(f"  Phase 1/3 — Querying and filtering collections …")
    print(f"{'─' * 65}")

    phase1: dict       = {}   # src_name → filter results + stored collections
    flag_rows: list[dict] = []

    for src_name in ["sentinel2", "landsat", "sentinel1", "viirs", "planet"]:
        src_cfg = cfg["sources"].get(src_name, {})
        if not src_cfg.get("enabled", False):
            continue

        logger.info("Phase 1 — %s: filtering…", src_name)
        source = _resolve_source(src_name, src_cfg)
        max_aoi_cloud = src_cfg.get("max_aoi_cloud_pct", 100)

        # ── Copernicus fallback: split date range at GEE archive cutoff ───────
        # For sentinel2, GEE's S2_SR_HARMONIZED archive is incomplete before
        # gee_cutoff_date (default 2019-01-01) for many tiles.  When
        # copernicus_fallback: true is set, we search CDSE STAC for the
        # pre-cutoff portion and use local rasterio reads for stats.
        use_cdse = (
            src_name == "sentinel2"
            and src_cfg.get("copernicus_fallback", False)
            and date_range["start"] < src_cfg.get("gee_cutoff_date", "2019-01-01")
        )
        gee_cutoff   = src_cfg.get("gee_cutoff_date", "2019-01-01") if use_cdse else None
        gee_dr       = {"start": max(date_range["start"], gee_cutoff) if gee_cutoff else date_range["start"],
                        "end":   date_range["end"]}
        cdse_dr      = {"start": date_range["start"],
                        "end":   min(date_range["end"], gee_cutoff)} if use_cdse else None

        # ── (a) GEE: date + tile-cloud filter (post-cutoff range only) ────────
        raw_collection = source.get_collection(filter_geom, gee_dr)
        n_tile_gee     = collection_size(raw_collection)
        logger.info("%s: %d GEE images after tile-cloud filter", src_name, n_tile_gee)

        # ── (b+c) GEE: AOI quality map + filter ───────────────────────────────
        clean_col, full_col = prefilter_by_aoi_cloud(
            collection=raw_collection,
            aoi=geometry,
            max_aoi_cloud_pct=max_aoi_cloud,
            quality_fn=source.aoi_quality_fn(),
            scale=source.default_scale,
        )

        # ── (d) GEE: batch metadata ────────────────────────────────────────────
        all_meta   = batch_image_metadata(full_col, source=source)
        clean_meta = batch_image_metadata(clean_col, source=source)
        n_clean_gee = len(clean_meta)
        logger.info(
            "%s GEE: %d tile-filtered | %d clean | %d rejected by AOI cloud",
            src_name, n_tile_gee, n_clean_gee, n_tile_gee - n_clean_gee,
        )

        # ── (e) CDSE: pre-cutoff Sentinel-2 L2A from Copernicus Data Space ────
        n_tile_cdse = 0
        if use_cdse:
            if not _cdse_session:
                logger.warning(
                    "copernicus_fallback is enabled for sentinel2 but no HTTP "
                    "session is available.  Skipping pre-%s data.",
                    gee_cutoff,
                )
            else:
                print(f"  Searching Copernicus STAC for pre-{gee_cutoff} L2A data…")
                try:
                    cdse_items_all = cdse.search_products(
                        bounds_wgs84=_aoi_bounds,
                        start_date=cdse_dr["start"],
                        end_date=cdse_dr["end"],
                        max_tile_cloud_pct=src_cfg.get("max_tile_cloud_pct", 100),
                        session=_cdse_session,
                    )
                    n_tile_cdse = len(cdse_items_all)

                    token = _cdse_token_mgr.get_token() if _cdse_token_mgr else ""
                    print(
                        f"  CDSE: {n_tile_cdse} candidates found — checking AOI cloud cover…"
                    )
                    for item in tqdm(cdse_items_all, desc="CDSE cloud check", unit="img"):
                        # Derive granule dir from OData attributes (no auth needed).
                        # Falls back to Nodes API if attributes are missing.
                        granule_dir = (
                            cdse._granule_dir_from_product(item)
                            or cdse._get_granule_dir(
                                item["Id"], item["Name"], token, _cdse_session
                            )
                        )
                        item["_granule_dir"] = granule_dir

                        aoi_cloud = cdse.compute_aoi_cloud_pct(
                            item, _aoi_bounds, token, _cdse_session
                        )
                        meta = cdse.item_to_meta(
                            item, aoi_cloud_pct=aoi_cloud, granule_dir=granule_dir
                        )

                        # Add to flag report
                        if meta.get("date"):
                            img_date = date.fromisoformat(meta["date"])
                            pre_post = "POST" if img_date >= reference_date else "PRE"
                            is_clean = (aoi_cloud is not None and aoi_cloud <= max_aoi_cloud)
                            cdse_flags = assign_flags(
                                image_date=img_date,
                                tile_cloud_pct=meta.get("tile_cloud_pct"),
                                aoi_cloud_pct=aoi_cloud,
                                aoi_fully_covered=True,
                                cfg=cfg,
                            )
                            flag_rows.append({
                                "date":           meta["date"],
                                "source":         src_name,
                                "image_id":       meta["image_id"],
                                "pre_post":       pre_post,
                                "aoi_cloud_pct":  aoi_cloud,
                                "tile_cloud_pct": meta.get("tile_cloud_pct"),
                                "aoi_covered":    True,
                                "in_clean_set":   is_clean,
                                "flags":          flags_to_string(cdse_flags),
                            })

                        if aoi_cloud is not None and aoi_cloud <= max_aoi_cloud:
                            all_meta.append(meta)
                            clean_meta.append(meta)

                    n_cdse_clean = sum(1 for m in clean_meta if m.get("_cdse"))
                    logger.info(
                        "CDSE: %d tile-filtered | %d clean (AOI cloud ≤ %.0f%%)",
                        n_tile_cdse, n_cdse_clean, max_aoi_cloud,
                    )
                except Exception as exc:
                    logger.warning("CDSE search failed: %s — continuing with GEE only", exc)

        # Combined totals for the filter summary
        n_tile  = n_tile_gee + n_tile_cdse
        n_clean = len(clean_meta)

        # ── Build GEE flag report rows (GEE path) ─────────────────────────────
        clean_ids = {m["image_id"] for m in clean_meta if not m.get("_cdse")}
        for meta in all_meta:
            if meta.get("_cdse") or not meta.get("date"):
                continue  # CDSE rows were added above
            img_date = date.fromisoformat(meta["date"])
            flags    = assign_flags(
                image_date=img_date,
                tile_cloud_pct=meta.get("tile_cloud_pct"),
                aoi_cloud_pct=meta.get("aoi_cloud_pct"),
                aoi_fully_covered=meta.get("aoi_covered", True),
                cfg=cfg,
            )
            pre_post = "POST" if img_date >= reference_date else "PRE"
            flag_rows.append({
                "date":           meta["date"],
                "source":         src_name,
                "image_id":       meta["image_id"],
                "pre_post":       pre_post,
                "aoi_cloud_pct":  meta.get("aoi_cloud_pct"),
                "tile_cloud_pct": meta.get("tile_cloud_pct"),
                "aoi_covered":    meta.get("aoi_covered"),
                "in_clean_set":   meta["image_id"] in clean_ids,
                "flags":          flags_to_string(flags),
            })

        # ── Season filter ──────────────────────────────────────────────────────
        if not flag_only and target_months:
            clean_meta = [
                m for m in clean_meta
                if m.get("date") and is_in_season(date.fromisoformat(m["date"]), target_months)
            ]
            logger.info("%s: %d images remain after season filter", src_name, len(clean_meta))

        if target_months:
            n_in_season = sum(
                1 for m in clean_meta
                if m.get("date") and is_in_season(date.fromisoformat(m["date"]), target_months)
            )
        else:
            n_in_season = len(clean_meta)

        phase1[src_name] = {
            "source":      source,
            "src_cfg":     src_cfg,
            "clean_col":   clean_col,
            "full_col":    full_col,
            "clean_meta":  clean_meta,
            "all_meta":    all_meta,
            "n_tile":      n_tile,
            "n_clean":     n_clean,
            "n_in_season": n_in_season,
            "indices":     src_cfg.get("indices", []),
        }

    # ── Print filter summary ───────────────────────────────────────────────
    _print_filter_summary(phase1, cfg)

    if not phase1:
        print("  No sources enabled — enable at least one source in the config.")
        return out_dir

    # ── Confirm before computing stats ────────────────────────────────────
    if not skip_confirm:
        if not estimator.confirm_proceed():
            logger.info("Run cancelled by user.")
            print("  Run cancelled.")
            return out_dir

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 2 — Compute: stats + CHIRPS + CSV row assembly
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'─' * 65}")
    print(f"  Phase 2/3 — Computing statistics …")
    print(f"{'─' * 65}")

    all_rows:   list[dict] = []
    tile_labels = [label for label, _ in tiles]

    for src_name, p in phase1.items():
        n_clean    = p["n_clean"]
        clean_col  = p["clean_col"]
        clean_meta = p["clean_meta"]
        source     = p["source"]
        src_cfg    = p["src_cfg"]
        indices    = p["indices"]

        if len(clean_meta) == 0:
            logger.info("%s: no clean images — skipping stats", src_name)
            continue

        # Split GEE vs CDSE images
        gee_meta  = [m for m in clean_meta if not m.get("_cdse")]
        cdse_meta = [m for m in clean_meta if m.get("_cdse")]

        logger.info(
            "%s: computing stats — %d GEE images, %d Copernicus images",
            src_name, len(gee_meta), len(cdse_meta),
        )

        # ── (e-gee) GEE batch stats — single or multi-tile ───────────────────
        stats_by_id      = {}
        stats_by_id_tile = {}

        if gee_meta:
            if surrounding_boxes:
                # Batch to avoid GEE "Too many concurrent aggregations" quota.
                # Each batch = N_images × 9 tiles reduceRegions calls.
                # Free-tier limit is roughly 50 concurrent aggregations;
                # batches of 4 images × 9 tiles = 36 — safely under the limit.
                _GEE_BATCH = 4
                logger.info(
                    "%s: reduceRegions over 9 tiles (GEE) — batching %d image(s) at a time",
                    src_name, _GEE_BATCH,
                )
                gee_image_ids = [m["image_id"] for m in gee_meta]
                stats_list: list[dict] = []
                for _i in range(0, len(gee_image_ids), _GEE_BATCH):
                    _batch_ids = gee_image_ids[_i : _i + _GEE_BATCH]
                    _batch_col = clean_col.filter(
                        ee.Filter.inList("system:index", _batch_ids)
                    )
                    _batch_fc = map_stats_multi_tile(
                        collection=_batch_col, ee_tiles_fc=ee_tiles_fc,
                        signal_names=indices, stats_cfg=stats_cfg,
                        preprocess_fn=source.apply_cloud_mask,
                        compute_fn=source.compute_index,
                        scale=source.default_scale,
                    )
                    stats_list.extend(
                        fetch_stats_multi_tile_batch(_batch_fc, indices, stats_cfg)
                    )
                    logger.debug(
                        "%s: batch %d/%d done (%d images)",
                        src_name,
                        _i // _GEE_BATCH + 1,
                        (len(gee_image_ids) + _GEE_BATCH - 1) // _GEE_BATCH,
                        len(_batch_ids),
                    )
                stats_by_id_tile = {
                    (r["image_id"], r["aoi_tile"]): {
                        k: v for k, v in r.items() if k not in ("image_id", "aoi_tile")
                    }
                    for r in stats_list
                }
            else:
                stats_col  = map_stats_over_collection(
                    collection=clean_col, signal_names=indices,
                    aoi=geometry, stats_cfg=stats_cfg,
                    preprocess_fn=source.apply_cloud_mask,
                    compute_fn=source.compute_index,
                    scale=source.default_scale,
                )
                stats_list = fetch_stats_batch(stats_col, indices, stats_cfg)
                stats_by_id = {row["image_id"]: row for row in stats_list}

        # ── (e-cdse) Copernicus local stats — rasterio windowed reads ─────────
        cdse_stats_by_id: dict[str, dict] = {}
        if cdse_meta and _cdse_token_mgr:
            print(f"  Computing stats for {len(cdse_meta)} Copernicus image(s)…")
            token = _cdse_token_mgr.get_token()
            for meta in tqdm(cdse_meta, desc="CDSE stats", unit="img"):
                item = meta.get("_cdse_item")
                if not item:
                    continue
                s = cdse.compute_stats_for_item(
                    item=item,
                    bounds_wgs84=_aoi_bounds,
                    indices=indices,
                    stats_cfg=stats_cfg,
                    token=token,
                    session=_cdse_session,
                )
                if s:
                    cdse_stats_by_id[meta["image_id"]] = s

        # (f) Batch CHIRPS — once for centre geometry (shared across tiles)
        if chirps_source:
            logger.info("%s: fetching CHIRPS for %d dates…", src_name, len(clean_meta))
            image_dates   = [date.fromisoformat(m["date"]) for m in clean_meta if m.get("date")]
            chirps_values = chirps_source.batch_rainfall_scalars(image_dates, geometry)
        else:
            chirps_values = [None] * len(clean_meta)

        # (g) Build CSV rows — zero GEE calls
        for i, meta in enumerate(clean_meta):
            if not meta.get("date"):
                continue
            img_date   = date.fromisoformat(meta["date"])
            pre_post   = "POST" if img_date >= reference_date else "PRE"
            chirps_val = chirps_values[i] if i < len(chirps_values) else None

            flags = assign_flags(
                image_date=img_date,
                tile_cloud_pct=meta.get("tile_cloud_pct"),
                aoi_cloud_pct=meta.get("aoi_cloud_pct"),
                aoi_fully_covered=meta.get("aoi_covered", True),
                cfg=cfg,
            )
            flag_meta = {
                "flags":          flags,
                "pre_post":       pre_post,
                "aoi_cloud_pct":  meta.get("aoi_cloud_pct"),
                "tile_cloud_pct": meta.get("tile_cloud_pct"),
            }

            for tile_label in tile_labels:
                if meta.get("_cdse"):
                    # Copernicus image — use locally computed stats
                    index_stats = cdse_stats_by_id.get(meta["image_id"], {})
                    emit_tile   = None  # CDSE path does not support surrounding_boxes
                elif surrounding_boxes:
                    index_stats = stats_by_id_tile.get((meta["image_id"], tile_label), {})
                    emit_tile   = tile_label
                else:
                    index_stats = stats_by_id.get(meta["image_id"], {})
                    index_stats = {k: v for k, v in index_stats.items() if k != "image_id"}
                    emit_tile   = None

                row = build_csv_row(
                    meta, flag_meta, index_stats,
                    chirps_val, chirps_days, src_name,
                    aoi_tile=emit_tile,
                )
                all_rows.append(row)

    # ── Write CSV outputs NOW — before any GeoTIFF downloads ──────────────
    gee_counts = {
        src_name: {
            "n_tile_filtered": p["n_tile"],
            "n_clean":         p["n_clean"],
            "n_in_season":     p["n_in_season"],
        }
        for src_name, p in phase1.items()
    }
    _write_outputs(cfg, all_rows, flag_rows, aoi_meta, gee_counts, out_dir)
    print(f"\n  stats.csv written → {out_dir / 'stats.csv'}  ({len(all_rows)} rows)")
    print(f"  flag_report.csv  → {out_dir / 'flag_report.csv'}  ({len(flag_rows)} rows)")

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 3 — Download: GeoTIFF exports (optional, per-file HTTP requests)
    # ══════════════════════════════════════════════════════════════════════
    needs_geotiff = any(p["src_cfg"].get("export_geotiff", False) for p in phase1.values())
    needs_chirps_geotiff = chirps_source and getattr(chirps_source, "export_geotiff", False)

    if needs_geotiff or needs_chirps_geotiff:
        print(f"\n{'─' * 65}")
        print(f"  Phase 3/3 — Downloading GeoTIFFs …")
        print(f"  (stats.csv is already saved — downloads can be cancelled safely)")
        print(f"{'─' * 65}")
    else:
        print(f"\n  No GeoTIFF downloads requested (export_geotiff: false).")

    for src_name, p in phase1.items():
        src_cfg    = p["src_cfg"]
        clean_meta = p["clean_meta"]
        clean_col  = p["clean_col"]
        source     = p["source"]
        indices    = p["indices"]
        n_clean    = p["n_clean"]

        if not src_cfg.get("export_geotiff", False):
            continue

        logger.info("%s: downloading GeoTIFFs (%d images × %d indices)…",
                    src_name, n_clean, len(indices))
        clean_list = clean_col.toList(n_clean)

        for i, meta in enumerate(tqdm(clean_meta, desc=f"{src_name} GeoTIFFs", unit="img")):
            img    = ee.Image(clean_list.get(i))
            masked = source.apply_cloud_mask(img)
            for idx_name in indices:
                try:
                    idx_img = source.compute_index(masked, idx_name)
                    downloader.download_index_geotiff(
                        index_image=idx_img,
                        index_name=idx_name,
                        image_date=meta["date"],
                        source_name=src_name,
                        run_name=run_name,
                        aoi=geometry,
                        aoi_area_km2=aoi_meta["area_km2"],
                        scale=source.export_scale(idx_name),
                        output_cfg=output_cfg,
                    )
                except Exception as exc:
                    logger.warning("GeoTIFF failed %s %s: %s", idx_name, meta["date"], exc)

        if needs_chirps_geotiff:
            for meta in tqdm(clean_meta, desc=f"{src_name} CHIRPS GeoTIFFs", unit="img"):
                chirps_img = chirps_source.get_download_image(
                    date.fromisoformat(meta["date"]), geometry
                )
                downloader.download_chirps_geotiff(
                    chirps_image=chirps_img,
                    image_date=meta["date"],
                    run_name=run_name,
                    aoi=geometry,
                    accumulation_days=chirps_days,
                    output_cfg=output_cfg,
                )

    # ── Completion summary ────────────────────────────────────────────────
    print(f"\n{'═' * 65}")
    print(f"  Run complete: {run_name}")
    print(f"  {'Output directory':<40}: {out_dir}")
    for src_name, p in phase1.items():
        n = p["n_clean"]
        idxs = len(p["indices"])
        print(f"  {src_name:<20}  {n:>4} clean images  ×  {idxs} indices  =  {n * idxs} pairs")
    print(f"  {'Total rows in stats.csv':<40}: {len(all_rows)}")
    print(f"{'═' * 65}\n")
    return out_dir


def _print_filter_summary(phase1: dict, cfg: dict) -> None:
    """Print a per-source table of image counts at each filter stage.

    The "Est. available" column is computed purely from date-range length ÷
    revisit interval — no GEE call required.  All other counts are actual GEE
    results from Phase 1.
    """
    date_range    = cfg["date_range"]
    season_cfg    = cfg.get("season", {})
    target_months = season_cfg.get("target_months", [])
    flag_only     = season_cfg.get("flag_only", True)
    aoi_cfg       = cfg["aoi"]

    month_str = (
        ", ".join(calendar.month_abbr[m] for m in target_months)
        if target_months else None
    )

    # Offline estimate: days in range ÷ revisit interval (no GEE call)
    start_d   = date.fromisoformat(date_range["start"])
    end_d     = date.fromisoformat(date_range["end"])
    total_days = (end_d - start_d).days

    W = 65
    print(f"\n{'═' * W}")
    print(f"  Coverage summary")
    print(f"  Date range : {date_range['start']} → {date_range['end']}  ({total_days} days)")
    if aoi_cfg.get("mode") == "point_radius":
        c = aoi_cfg.get("center", {})
        print(f"  AOI        : ({c.get('lat')}, {c.get('lon')})  r = {aoi_cfg.get('radius_m')} m")
    print(f"{'═' * W}")

    if not phase1:
        print("  (no enabled sources)")
        print(f"{'═' * W}\n")
        return

    col_w = 13
    hdr   = (
        f"  {'Source':<14}  {'Est. available':>{col_w}}"
        f"  {'Tile-filtered':>{col_w}}  {'AOI-clean':>{col_w}}"
    )
    if target_months:
        hdr += f"  {'In-season':>{col_w}}"
    print(f"\n{hdr}")
    divider = f"  {'─' * 14}  {'─' * col_w}  {'─' * col_w}  {'─' * col_w}"
    if target_months:
        divider += f"  {'─' * col_w}"
    print(divider)

    for src_name, p in phase1.items():
        src_cfg  = p["src_cfg"]
        n_tile   = p["n_tile"]
        n_clean  = p["n_clean"]
        n_season = p["n_in_season"]
        max_tile = src_cfg.get("max_tile_cloud_pct", "—")
        max_aoi  = src_cfg.get("max_aoi_cloud_pct", 100)

        revisit      = _REVISIT_DAYS.get(src_name, 5)
        n_est        = total_days // revisit
        est_label    = f"~{n_est} ({revisit}d)"
        tile_label   = f"{n_tile} (≤{max_tile}%)"
        clean_label  = f"{n_clean} (≤{max_aoi}%)"

        row = (
            f"  {src_name:<14}  {est_label:>{col_w}}"
            f"  {tile_label:>{col_w}}  {clean_label:>{col_w}}"
        )
        if target_months:
            row += f"  {n_season:>{col_w}}"
        print(row)

    print(f"\n  Column guide:")
    print(f"    Est. available  — date-range days ÷ revisit interval (offline, no GEE call)")
    print(f"    Tile-filtered   — images GEE found after tile-level cloud % filter")
    print(f"    AOI-clean       — images passing the per-pixel AOI cloud % filter")
    if target_months:
        mode_note = "all kept, out-of-season flagged" if flag_only else "out-of-season excluded"
        print(f"    In-season       — clean images in {month_str}  [{mode_note}]")

    reference_date = date.fromisoformat(cfg["run"]["reference_date"])

    print(f"\n  Filter breakdown:")
    for src_name, p in phase1.items():
        src_cfg  = p["src_cfg"]
        max_tile = src_cfg.get("max_tile_cloud_pct")
        max_aoi  = src_cfg.get("max_aoi_cloud_pct", 100)
        n_tile   = p["n_tile"]
        n_clean  = p["n_clean"]
        revisit  = _REVISIT_DAYS.get(src_name, 5)
        n_est    = total_days // revisit
        rejected = n_tile - n_clean
        pct      = f"{rejected / n_tile * 100:.0f}%" if n_tile else "—"

        print(f"  {src_name}")
        print(f"    0. Estimated available     ~{n_est}  ({revisit}-day revisit × {total_days} days)")
        if max_tile is not None:
            print(f"    1. Tile cloud ≤ {max_tile}%        {n_tile}  images kept")
        print(f"    2. AOI cloud  ≤ {max_aoi}%        {n_clean}  images kept  ({pct} rejected by cloud)")
        if target_months and month_str:
            mode_note = "flagged, all kept" if flag_only else "others excluded"
            print(f"    3. Season ({month_str})           {p['n_in_season']}  in-season  [{mode_note}]")
        if n_clean == 0:
            print(f"    ⚠  No clean images — try raising max_aoi_cloud_pct in the YAML")

        # Per-year breakdown of the images that will actually be processed
        clean_meta = p["clean_meta"]
        # When flag_only=True, clean_meta still has all AOI-clean images; narrow to in-season
        # for the breakdown so the counts match what the user sees in step 3.
        if flag_only and target_months:
            display_meta = [
                m for m in clean_meta
                if m.get("date") and is_in_season(date.fromisoformat(m["date"]), target_months)
            ]
        else:
            display_meta = [m for m in clean_meta if m.get("date")]

        if display_meta:
            # Group dates by year
            by_year: dict[str, list[str]] = {}
            for m in display_meta:
                yr = m["date"][:4]
                by_year.setdefault(yr, []).append(m["date"])

            n_total = len(display_meta)
            show_dates = n_total <= 60   # print individual dates only for manageable counts

            print(f"\n    Year breakdown  ({n_total} images total):")
            print(f"    {'Year':<6}  {'PRE/POST':<8}  {'N':>3}  {'Dates' if show_dates else ''}")
            print(f"    {'─'*6}  {'─'*8}  {'─'*3}  {'─'*30 if show_dates else ''}")
            for yr in sorted(by_year.keys()):
                dates_in_yr = sorted(by_year[yr])
                first_d = date.fromisoformat(dates_in_yr[0])
                last_d  = date.fromisoformat(dates_in_yr[-1])
                if last_d < reference_date:
                    pp = "PRE"
                elif first_d >= reference_date:
                    pp = "POST"
                else:
                    pp = "PRE+POST"
                count = len(dates_in_yr)
                if show_dates:
                    date_str = "  " + ", ".join(d[5:] for d in dates_in_yr)  # MM-DD
                else:
                    date_str = ""
                print(f"    {yr:<6}  {pp:<8}  {count:>3}{date_str}")
            print()

    print(f"{'═' * W}\n")


def _write_outputs(cfg, all_rows, flag_rows, aoi_meta, gee_counts, out_dir):
    output_cfg = cfg.get("output", {})
    run_name   = cfg["run"]["name"]

    if output_cfg.get("stats_csv", True) and all_rows:
        df = rows_to_dataframe(all_rows)
        p  = out_dir / "stats.csv"
        df.to_csv(p, index=False)
        logger.info("Stats CSV: %s (%d rows)", p, len(df))

    if output_cfg.get("flag_report", True) and flag_rows:
        flag_df = pd.DataFrame(flag_rows).sort_values("date").reset_index(drop=True)
        p = out_dir / "flag_report.csv"
        flag_df.to_csv(p, index=False)
        logger.info("Flag report: %s (%d rows)", p, len(flag_df))

    meta = {
        "run_name":      run_name,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "config":        cfg,
        "aoi":           aoi_meta,
        "gee_counts":    gee_counts,
        "satme_version": "0.1.0",
    }
    p = out_dir / "run_metadata.json"
    with open(p, "w") as f:
        json.dump(meta, f, indent=2, default=str)
    logger.info("Run metadata: %s", p)
