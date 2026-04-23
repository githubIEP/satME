# satME — Satellite Monitoring and Evaluation Pipeline

A data pipeline for extracting time-series statistics from satellite imagery over user-defined areas of interest (AOIs). Designed for monitoring environmental and infrastructure change around specific sites — dam construction, land-cover change, economic activity — over multi-year periods.

---

## Table of Contents

1. [What it does](#what-it-does)
2. [Accounts and credentials you need](#accounts-and-credentials-you-need)
3. [Installation](#installation)
4. [Quick start](#quick-start)
5. [How the pipeline works](#how-the-pipeline-works)
6. [Configuration reference](#configuration-reference)
7. [Output files explained](#output-files-explained)
8. [Visualizing results](#visualizing-results)
9. [Checking service connectivity](#checking-service-connectivity)
10. [Key files overview](#key-files-overview)
11. [Data sources explained](#data-sources-explained)

---

## What it does

Given a config file describing a study site, a date range, and the satellite sources to use, satME:

1. **Queries** Google Earth Engine (GEE) and the Copernicus STAC catalog for imagery covering the AOI
2. **Filters** by cloud cover at both the full-tile and AOI level
3. **Computes** spectral indices (NDVI, NDWI, NDMI, etc.) or backscatter ratios (RVI, VH/VV for SAR) or nighttime radiance (VIIRS) over the AOI
4. **Writes** statistics (mean, std, percentiles) to a `stats.csv` file, one row per image per AOI tile
5. **Appends** CHIRPS rainfall accumulation columns to every row for precipitation context
6. **Produces** visualizations — time-series plots, tile comparisons, nighttime light trends

---

## Accounts and credentials you need

| Service | Required? | What for | How to set up |
|---|---|---|---|
| **Google Earth Engine** | **Required** | Post-2019 Sentinel-2, Sentinel-1, VIIRS, CHIRPS | Create a Cloud project at [code.earthengine.google.com](https://code.earthengine.google.com), run `earthengine authenticate` |
| **Microsoft Planetary Computer (MPC)** | Automatic | Pre-2019 Sentinel-2 band reads (COG files) | Nothing — anonymous SAS tokens are fetched automatically |
| **Copernicus Data Space (CDSE)** | Optional | Fallback for rare products not in MPC | Free account at [dataspace.copernicus.eu](https://dataspace.copernicus.eu); set via env vars `CDSE_USERNAME` / `CDSE_PASSWORD` |

### GEE setup (one-time per machine)

```bash
# 1. Install the Earth Engine CLI
pip install earthengine-api

# 2. Authenticate — opens a browser to sign in with your Google account
earthengine authenticate

# 3. Note your GEE Cloud project ID
#    (visible at console.cloud.google.com — make sure Earth Engine API is enabled)
```

### Environment variables (recommended instead of putting credentials in YAML)

```bash
# Linux / macOS
export EARTHENGINE_PROJECT="your-gee-project-id"
export CDSE_USERNAME="your@email.com"      # optional
export CDSE_PASSWORD="your_password"       # optional

# Windows (PowerShell)
$env:EARTHENGINE_PROJECT = "your-gee-project-id"
$env:CDSE_USERNAME = "your@email.com"
$env:CDSE_PASSWORD = "your_password"
```

---

## Installation

```bash
# Clone the repo
git clone <repo-url>
cd satME

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate       # Linux/macOS
.venv\Scripts\activate          # Windows

# Install dependencies
pip install -r requirements.txt
pip install matplotlib           # for visualize.py
```

---

## Quick start

```bash
# 1. Copy the example config and edit it for your site
cp config/config_example.yaml config/my_site.yaml
# Edit config/my_site.yaml — set auth.gee_project, AOI coordinates, date range

# 2. Check that all services are accessible
python check_usage.py --config config/my_site.yaml

# 3. Dry run — prints estimated image counts without processing any data
python main.py --config config/my_site.yaml --dry-run

# 4. Full run — confirm, then download and compute
python main.py --config config/my_site.yaml

# 5. Visualize results
python visualize.py outputs/runs/my_site/stats.csv
```

---

## How the pipeline works

Running `python main.py --config config/my_site.yaml` executes three phases.

### Phase 1 — Filter

For each enabled source the pipeline:
1. Queries GEE (and the CDSE catalog for pre-cutoff Sentinel-2) for all images in the date range
2. Applies **tile-level cloud filter** (`max_tile_cloud_pct`) — fast metadata-only check
3. Applies **AOI-level cloud filter** (`max_aoi_cloud_pct`) — counts actual cloud pixels over your AOI using the Scene Classification Layer (SCL)
4. Fetches batch metadata in one GEE round-trip per source

After Phase 1, the pipeline prints a summary table showing how many images passed each filter. It then asks for confirmation before proceeding (skip with `--yes`).

### Phase 2 — Compute

For each clean image:
1. Applies cloud mask / speckle filter (source-specific)
2. Computes the requested spectral indices / SAR ratios / radiance
3. Runs `reduceRegion` to extract statistics (mean, std, percentiles) over the AOI
4. Fetches CHIRPS rainfall accumulation for each image date
5. Assembles rows and writes `stats.csv`, `flag_report.csv`, `run_metadata.json`

When `aoi.surrounding_boxes: true` is set, each image produces 9 rows — one per tile in the 3×3 grid around the study point.

### Phase 3 — Download (optional)

If any source has `export_geotiff: true`, the pipeline downloads one GeoTIFF per image per index. This is the slowest phase and can be safely skipped for most analyses.

### What happens on the terminal

```
Phase 1 — Filter
  sentinel2: 47 images in GEE (post-2019) | 12 clean (AOI cloud ≤ 20%)
  CDSE: 23 candidates found — checking AOI cloud cover…
  ...
  sentinel1: 68 images | 68 clean (SAR, no cloud filter)

Proceed with Phase 2? [y/N]

Phase 2 — Compute
  Computing Sentinel-2 stats for 12 images...
  ...
  Writing outputs/runs/my_site/stats.csv
```

---

## Configuration reference

The config is a YAML file. A fully annotated example is in [config/config_example.yaml](config/config_example.yaml). Full documentation of every key is in [config/REFERENCE.md](config/REFERENCE.md).

**Minimal working config:**

```yaml
auth:
  gee_project: "your-gee-project-id"

run:
  name: "my_site_2018_2024"
  reference_date: "2020-06-01"    # tags images as PRE or POST

aoi:
  mode: "point_radius"
  center:
    lat: -1.54320
    lon: 37.33164
  radius_m: 315

date_range:
  start: "2018-01-01"
  end:   "2024-12-31"

season:
  target_months: [6, 7, 8, 9]    # June–September
  flag_only: false

sources:
  sentinel2:
    enabled: true
    indices: [NDVI, NDWI]
    max_tile_cloud_pct: 60
    max_aoi_cloud_pct:  20

output:
  base_dir: "outputs/runs"
  stats_csv: true

stats:
  percentiles: [10, 25, 50, 75, 90]
  include_mean:    true
  include_stddev:  true
  include_min_max: true
```

**Key config sections:**

| Section | Purpose |
|---|---|
| `auth` | GEE project ID; optional CDSE credentials |
| `run` | Run name (used for output folder) and reference date |
| `aoi` | Point+radius or polygon geometry; optional 3×3 tile grid |
| `date_range` | Start and end dates (ISO format) |
| `season` | Which calendar months to include |
| `sources` | Enable/configure each satellite source |
| `stats` | Which statistics to compute |
| `output` | Output directory, GeoTIFF download settings |

---

## Output files explained

All outputs are written to `outputs/runs/{run.name}/`:

| File | Contents |
|---|---|
| `stats.csv` | Main output — one row per image per AOI tile. Columns: date, source, aoi_tile, image_id, pre_post, cloud percentages, index statistics, CHIRPS rainfall, flags. |
| `flag_report.csv` | All images seen (including rejected ones) with their filter outcomes. Useful for understanding what was excluded and why. |
| `run_metadata.json` | Full config snapshot, AOI geometry (WKT), image counts per source. Reproducibility record. |
| `{run_name}_run_summary.txt` | Human-readable summary of filter steps and image lists (generated by `visualize.py`). |
| `{run_name}_index_timeseries.png` | Sentinel-2 spectral index time series for the center tile with PRE/POST shading. |
| `{run_name}_tile_comparison.png` | Sentinel-2 NDVI across all 9 tiles (only produced for surrounding_boxes runs). |
| `{run_name}_sar_timeseries.png` | Sentinel-1 SAR index time series (RVI, VH/VV, DPSVI). |
| `{run_name}_sar_tile_comparison.png` | Sentinel-1 RVI across all 9 tiles. |
| `{run_name}_viirs_timeseries.png` | VIIRS nighttime radiance bar chart with interpretation thresholds. |

### Reading stats.csv

The key columns are:

- `date` — image acquisition date
- `source` — `sentinel2`, `sentinel1`, or `viirs`
- `aoi_tile` — `center`, `N`, `NE`, `E`, `SE`, `S`, `SW`, `W`, `NW` (or absent for single-tile runs)
- `pre_post` — `PRE` (before `run.reference_date`) or `POST` (after)
- `{INDEX}_p50` — median of the index over the AOI (most robust central estimate)
- `{INDEX}_mean` — mean; more sensitive to outliers than median
- `{INDEX}_std` — standard deviation; proxy for spatial heterogeneity
- `chirps_{N}d_mm` — accumulated rainfall over the N days before the image

### Understanding the index values

**Sentinel-2 optical indices:**

| Index | Formula | Range | High value means |
|---|---|---|---|
| NDVI | (B8−B4)/(B8+B4) | −1 to +1 | Dense green vegetation |
| NDWI | (B3−B8)/(B3+B8) | −1 to +1 | Surface water / wet vegetation |
| NDMI | (B8A−B11)/(B8A+B11) | −1 to +1 | High moisture content |
| EVI | 2.5×(B8−B4)/(B8+6×B4−7.5×B2+1) | ~0 to 1 | Vegetation (less saturation than NDVI) |
| NDBI | (B11−B8)/(B11+B8) | −1 to +1 | Built-up surfaces |
| BSI | (B11+B4−B8−B2)/(B11+B4+B8+B2) | −1 to +1 | Bare soil |

**Sentinel-1 SAR indices:**

| Index | Formula | Range | High value means |
|---|---|---|---|
| RVI | 4×VH_lin/(VV_lin+VH_lin) | 0 to 1 | Dense vegetation |
| VH_VV | VH−VV (dB) | ~−25 to −5 dB | More vegetation/depolarisation (less negative) |
| DPSVI | VV×(VV+VH)/(4×VH) | > 0 | Less vegetation (inverse of RVI) |

**VIIRS nighttime lights:**

| avg_rad (nW/cm²/sr) | Interpretation |
|---|---|
| < 0.5 | Rural / uninhabited |
| 0.5–2 | Villages / small towns |
| 2–10 | Suburban / small city |
| 10–50 | Urban / commercial |
| > 50 | Dense city / industrial / gas flare |

---

## Visualizing results

```bash
python visualize.py outputs/runs/my_site/stats.csv
```

The script auto-detects which sources are present in `stats.csv` and produces the appropriate plots:

- **Sentinel-2 present** → `_index_timeseries.png` + `_tile_comparison.png`
- **Sentinel-1 present** → `_sar_timeseries.png` + `_sar_tile_comparison.png`
- **VIIRS present** → `_viirs_timeseries.png`

All plots also write `_run_summary.txt` with the full filter pipeline record.

---

## Checking service connectivity

```bash
# With a config file (recommended — tests your specific project)
python check_usage.py --config config/my_site.yaml

# Without a config — uses environment variables only
python check_usage.py
```

Output:

```
  satME — Service Connectivity & Credential Check
  ──────────────────────────────────────────────────────
  Config: config/my_site.yaml

  [OK]   Google Earth Engine (GEE)
         Connected — project: my-project | 1234 algorithms available
         View quota / API usage at:
           https://console.cloud.google.com/apis/api/earthengine.googleapis.com/quotas

  [OK]   Microsoft Planetary Computer (MPC)
         SAS token received (N chars) — no account required

  [WARN] Copernicus Data Space (CDSE)
         No CDSE credentials configured (catalog reachable)
         CDSE auth is OPTIONAL — band reads use Microsoft Planetary Computer.
         ...

  [OK]   CHIRPS Rainfall
         CHIRPS enabled — collection: UCSB-CHG/CHIRPS/DAILY | accumulation: 30 days
```

WARN is not an error — CDSE credentials are optional.

**Where to view actual GEE quota usage:**

Go to [console.cloud.google.com](https://console.cloud.google.com) → APIs & Services → Earth Engine API → Quotas. The most relevant metric is `EECU-seconds` consumed. Free tier users get a generous monthly allowance; most single-site runs use a small fraction of it.

---

## Key files overview

```
satME/
├── main.py                    Entry point — parse args, validate config, run pipeline
├── visualize.py               Plot time series and tile comparisons from stats.csv
├── check_usage.py             Check connectivity and credentials for all services
├── requirements.txt           Python dependencies
│
├── config/
│   ├── config_example.yaml  Fully annotated example config (copy and edit this)
│   └── REFERENCE.md           Complete documentation of every config key
│
└── satme/
    ├── pipeline.py            Orchestrates all three phases (filter → compute → download)
    ├── auth.py                GEE authentication and initialisation
    ├── aoi.py                 AOI geometry construction from config
    ├── copernicus_auth.py     CDSE OAuth2 token management and session building
    ├── image_filter.py        Cloud filtering, season filtering, batch metadata fetch
    ├── stats.py               GEE statistics extraction (reduceRegion, batch fetch)
    ├── flags.py               Image quality flag assignment
    ├── indices.py             Optical spectral index formulas (NDVI, NDWI, etc.)
    ├── estimator.py           Dry-run image count estimation
    ├── downloader.py          GeoTIFF download logic (GEE getDownloadUrl / Drive)
    │
    └── sources/
        ├── base.py            SatelliteSource abstract base class
        ├── sentinel2.py       Sentinel-2 L2A — GEE source (post-2019)
        ├── sentinel1.py       Sentinel-1 GRD — SAR backscatter
        ├── viirs.py           VIIRS nighttime lights — monthly composites
        ├── chirps.py          CHIRPS daily rainfall — ancillary data
        ├── copernicus_s2.py   Sentinel-2 L2A — CDSE + MPC COG source (pre-2019)
        ├── landsat.py         Landsat 8/9 (stub)
        └── planet.py          Planet (stub — requires commercial API key)
```

### The two Sentinel-2 sources

satME uses two different backends for Sentinel-2 depending on the date:

| Period | Backend | Why |
|---|---|---|
| Post `gee_cutoff_date` (default 2019-01-01) | GEE — `COPERNICUS/S2_SR_HARMONIZED` | Full archive in GEE, fastest path |
| Pre `gee_cutoff_date` | CDSE catalog + MPC COGs | GEE archive is incomplete for many tiles before ~2019 |

For the pre-cutoff path: the CDSE OData API is queried for product metadata (public, no auth needed), then band reads use Cloud-Optimised GeoTIFFs from Microsoft Planetary Computer (also free, no account). Only about 50×50 pixel windows around the AOI are downloaded per image.

---

## Data sources explained

### Sentinel-2 L2A
- **What**: ESA optical multispectral satellite — 13 spectral bands
- **Resolution**: 10 m (visible/NIR), 20 m (red-edge, SWIR)
- **Revisit**: ~5 days (Sentinel-2A + 2B combined)
- **Archive**: November 2015 – present
- **Best for**: vegetation health, water, moisture, built-up area
- **Limitation**: cloud cover — cloudy images are masked/excluded

### Sentinel-1 SAR
- **What**: ESA radar satellite — microwave backscatter, not reflected light
- **Resolution**: 10 m (IW mode)
- **Revisit**: ~6 days
- **Archive**: April 2014 – present
- **Best for**: vegetation structure, soil moisture, surface water; **works through clouds**
- **Key note**: always use a single orbit direction (ASCENDING or DESCENDING) — mixing produces geometric artefacts

### VIIRS Nighttime Lights
- **What**: Suomi NPP / NOAA-20 Day/Night Band — measures nighttime radiance
- **Resolution**: ~500 m
- **Temporal**: monthly composites
- **Archive**: April 2012 – present
- **Best for**: economic activity, electrification, urban extent, post-conflict recovery
- **Output**: one row per month (not per overpass)

### CHIRPS Rainfall
- **What**: Climate Hazards Group InfraRed Precipitation with Stations — modelled rainfall
- **Resolution**: ~5.5 km
- **Temporal**: daily
- **Archive**: January 1981 – present (~3-week lag)
- **How it appears**: `chirps_{N}d_mm` column on every satellite image row — N-day accumulated rainfall before the image acquisition date
- **Interpretation note**: short-term (7–30 day) rainfall correlates with NDVI mainly during the dry-to-wet transition; seasonal (90-day) accumulation is a better drought index

---

## Corporate network note

On networks with SSL inspection (Zscaler, Blue Coat, etc.) you may see `SSLEOFError` or `UNEXPECTED_EOF_WHILE_READING`. Run with:

```bash
python main.py --config config/my_site.yaml --no-ssl-verify
```

Or set your proxy explicitly in the config:

```yaml
auth:
  https_proxy: "http://proxy.company.com:8080"
```
