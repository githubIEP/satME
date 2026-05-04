# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common commands

```bash
# Run the pipeline (asks for confirmation after Phase 1)
python main.py --config config/<your_site>.yaml

# Skip the confirm prompt
python main.py --config config/<your_site>.yaml --yes

# Pre-flight estimate only — no compute, no downloads
python main.py --config config/<your_site>.yaml --dry-run

# Connectivity / credential check (GEE, MPC, CDSE, CHIRPS)
python check_usage.py --config config/<your_site>.yaml

# Plot results from a completed run
python visualize.py outputs/runs/<run_name>/stats.csv

# Corporate networks with SSL inspection (Zscaler etc.)
python main.py --config <...> --no-ssl-verify
```

### Tests

```bash
pytest                                 # run everything; GEE tests auto-skip if not authed
pytest -m "not gee"                    # offline tests only
pytest tests/test_indices.py           # one file
pytest tests/test_aoi.py::test_name    # one test
```

GEE-dependent tests are gated by `@pytest.mark.gee` and require `gee_connection` (see `tests/conftest.py`). They `pytest.skip` cleanly when `earthengine authenticate` has not been run, so the suite is safe to run on machines without GEE access.

The default `tests/conftest.py` references `config/makaveti_example.yaml` and a hard-coded `gee_project: gdelt-proj-489101` — both are personal to this repo's author. Override `EARTHENGINE_PROJECT` or edit the fixture if reusing elsewhere.

## Architecture

### Three-phase pipeline (`satme/pipeline.py`)

`pipeline.run(cfg)` is the single orchestrator. It runs in strict order:

1. **Phase 1 — Filter.** For each enabled source, query GEE → tile-cloud filter → AOI-pixel cloud filter (`prefilter_by_aoi_cloud`) → batch metadata. Prints a coverage table and prompts for confirmation. For Sentinel-2 with `copernicus_fallback: true`, the date range is split at `gee_cutoff_date` (default 2019-01-01) and the pre-cutoff portion is queried against the CDSE OData catalog.
2. **Phase 2 — Compute.** Stats are pulled in **batched** `reduceRegion(s)` calls. For multi-tile (3×3 surrounding boxes) runs, batches are fixed at 4 images × 9 tiles to stay under GEE's ~50 concurrent-aggregation quota — *do not raise this without checking*. CHIRPS rainfall is fetched in one batch and joined onto every row. CSV outputs are written **before** Phase 3 so a cancelled download leaves tabular data intact.
3. **Phase 3 — Download.** Optional GeoTIFF export, only if `export_geotiff: true` for some source. One HTTP request per file; failures are logged but do not abort the run.

### Two backends for Sentinel-2

The same `sentinel2` source resolves to two different code paths depending on date:

- **Post-cutoff (default ≥ 2019-01-01)** → `satme/sources/sentinel2.py` via GEE (`COPERNICUS/S2_SR_HARMONIZED`).
- **Pre-cutoff** → `satme/sources/copernicus_s2.py`: CDSE OData for catalog/metadata (no auth needed for search), Microsoft Planetary Computer COGs for band reads (anonymous SAS tokens), `rasterio` windowed reads (~50×50 px around the AOI), local stats. CDSE rows carry `_cdse: True` in their `meta` dict — this flag distinguishes them throughout the pipeline.

`copernicus_auth.from_cfg(cfg)` returns `(token_mgr, session)` once per run and is shared across all sources.

### Source / backend split

- `satme/sources/*.py` answers *what data exists and how do I mask it*. Each subclasses `SatelliteSource` (`sources/base.py`). The pipeline calls `get_collection`, `apply_cloud_mask`, `compute_index`, `aoi_quality_fn` — the AOI quality function is the per-source hook that lets SAR return `0` (radar sees through cloud) and VIIRS use `cf_cvg` instead of SCL.
- `satme/backends/` is an in-progress abstraction (currently a stub with notes in `backends/__init__.py` and `backends/base.py`). The pipeline still calls GEE-specific helpers directly — do not assume backend abstraction is wired in.

### SSL patches in `main.py`

`main.py` lines 33–117 patch `ssl` and `urllib3` **before any network imports** to work around Python 3.12+ stricter TLS-EOF handling that breaks on corporate SSL-inspection proxies. Three patches: `OP_IGNORE_UNEXPECTED_EOF` on urllib3 contexts, the same on `ssl.create_default_context`, and a `SSLSocket.read` wrapper that converts `SSLZeroReturnError` to clean EOF. If you reorganise imports in `main.py`, the SSL block must stay at the top.

### Config

YAML, validated by `_validate_config` in `main.py`. Required: `run.name`, `run.reference_date`, `aoi.mode`, `date_range.start/end`, `sources` (with at least one `enabled: true`), `output`. `aoi.mode` is `point_radius` (lat/lon + `radius_m`) or `polygon`. `aoi.surrounding_boxes: true` activates a 3×3 grid (`center`, `N`, `NE`, …) — only valid with `point_radius`. Reference docs in `config/REFERENCE.md`; `config/config_example.yaml` is the only YAML checked in (`.gitignore` excludes `config/*.yaml` except the example).

### Outputs

Written to `outputs/runs/{run.name}/`: `stats.csv` (one row per image per tile), `flag_report.csv` (every image seen incl. rejected), `run_metadata.json` (full config + AOI WKT + counts). `visualize.py` reads `stats.csv` and auto-detects which sources are present to choose which plots to produce.

## `david/` — parallel sand-dam impact study

`david/` is a **separate, self-contained** analysis of the Machakos sand dam (constructed October 2018) addressing the same question as `satme/` — *did the dam change vegetation?* — but with a different stack. Don't conflate the two: they share no code, different data backends, different outputs.

| | `satme/` (main pipeline) | `david/exploration/` |
|---|---|---|
| Imagery | GEE + CDSE/MPC fallback; S2, S1, VIIRS | MPC STAC only, Sentinel-2 L2A |
| Compute | Server-side GEE `reduceRegion` | Local pixel cubes via `xarray`/`dask`, cached to zarr |
| Rainfall | CHIRPS (via GEE) per-image | Open-Meteo / ERA5 daily, plain HTTP |
| Indices | NDVI/NDWI/NDMI/EVI/NDBI/BSI + SAR | NDVI, MNDWI |
| Output | `outputs/runs/<name>/stats.csv` | `david/exploration/outputs/` + `REPORT.md`/`REPORT.pdf` |

### Same imagery, different plumbing

Both pipelines read the **same ESA Sentinel-2 L2A archive**, but the scene lists and pixel values are not byte-identical:

- **Catalogs.** `satme` uses GEE `COPERNICUS/S2_SR_HARMONIZED` post-2019 and CDSE STAC + MPC COGs pre-2019. `david` uses MPC STAC `sentinel-2-l2a` for the whole 2016–2025 window.
- **Reflectance scaling.** GEE `_HARMONIZED` auto-corrects ESA's Jan-2022 baseline-04.00 −1000 offset; MPC serves whatever is in storage. Pre-2019 the two converge (both end up reading MPC).
- **Bands.** `satme` loads `B2/B3/B4/B8/B8A/B11/SCL` (B8A is needed for NDMI). `david` loads `B02/B03/B04/B08/B11/SCL` — no B8A, since it only computes NDVI + MNDWI.
- **SCL invalid classes.** `satme/cloud_mask.py` masks `[0,1,3,8,9,10]`; `david/exploration/config.py` also masks class 11 (snow/ice).
- **Cloud filter math.** `satme` uses YAML-configurable `max_tile_cloud_pct` + per-pixel `max_aoi_cloud_pct`. `david` is hardcoded: `MAX_SCENE_CLOUD = 80`, `MIN_VALID_FRACTION = 0.85`.

For the Machakos AOI most scenes appear in both, but cleaned/masked pixels are not directly comparable across the two pipelines.

### Running

From the repo root:

```bash
python -m david.exploration.scripts.step01_build_cache         # ~30–60 min, network-bound (only if cache absent/stale)
python -m david.exploration.scripts.step02_fetch_precipitation
python -m david.exploration.scripts.step03_run_analysis
python -m david.exploration.scripts.step04_render_outputs
```

If `david/_cache_fulltimeseries/` is present, step03 + step04 finish in under a minute. Step01 rebuilds the per-AOI zarr cache from MPC and is the slow step.

### Layout notes

- `exploration/config.py` holds AOI coords, dates, and thresholds (DAM_CONSTRUCTION_DATE, drought window, NDVI/MNDWI cutoffs). Edit there, not in scripts.
- `exploration/fetch.py` does a two-pass STAC search: SCL-only first to compute clear-pixel fraction and reject cloudy scenes, then full bands for survivors.
- KML filenames (`sanddam.kml`, `sandam-upstream.kml`) predate the elevation check that confirmed flow direction — the channel/orientation mapping in `step03_run_analysis.py` is the authoritative one.
- `REPORT.md` image paths are written as `backend/exploration/outputs/...` for compatibility with the original repo; pandoc rebuilds use `--resource-path=david` to resolve them. See `david/README.md` for the exact pandoc invocation.
