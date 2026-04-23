# SatME Configuration Reference

Complete guide to writing a run config YAML.  Every key is documented with its
type, default value, and valid options.  Copy `makaveti_example.yaml` as your
starting point — this file explains what each field means.

---

## Top-level structure

```yaml
run:          # Run identification
aoi:          # Area of Interest geometry
date_range:   # Temporal window
season:       # Optional seasonal filter
sources:      # One block per satellite / data product
output:       # Where and how to write results
stats:        # Which statistics to compute
```

---

## `run`

```yaml
run:
  name: "my_run_name"          # String — used as output subfolder name
  reference_date: "YYYY-MM-DD" # Intervention/event date; images are tagged PRE or POST
```

`reference_date` is the date of the event you're studying (dam construction, conflict onset,
disaster, etc.).  Images within 60 days of this date are additionally flagged `NEAR_INTERVENTION`.

---

## `aoi`

Two modes — pick one:

### Mode A: point + radius (most common)

```yaml
aoi:
  mode: "point_radius"
  center:
    lat: -1.54351   # Decimal degrees, negative = South
    lon: 37.33258   # Decimal degrees, negative = West
  radius_m: 500     # Half-width of bounding square in metres
                    # radius_m: 500   →  1 km × 1 km square   (0.25 km²)
                    # radius_m: 2500  →  5 km × 5 km square   (6.25 km²)
                    # radius_m: 5000  →  10 km × 10 km square (25 km²)
```

### Mode B: polygon

```yaml
aoi:
  mode: "polygon"
  coordinates:          # List of [lon, lat] pairs — close the ring (first = last)
    - [37.328163, -1.54069]
    - [37.334633, -1.54069]
    - [37.334633, -1.545666]
    - [37.328163, -1.545666]
    - [37.328163, -1.54069]  # closing vertex
```

---

## `date_range`

```yaml
date_range:
  start: "2016-01-01"   # ISO 8601 date string (inclusive)
  end:   "2024-12-31"   # ISO 8601 date string (inclusive)
```

Each satellite has a coverage start date — requests before that date return
nothing (documented per-satellite below).

---

## `season`

Optional filter to restrict analysis to specific months of the year.

```yaml
season:
  target_months: [8, 9]   # List of integers 1–12; empty list = all months
  flag_only: true          # true  = keep all images but flag out-of-season ones
                           # false = exclude out-of-season images entirely
```

Use `flag_only: true` (default) if you want the full time series in the output
but want to know which images fall outside your analysis window.

---

## `sources`

Each satellite source has its own block under `sources:`.  Set `enabled: true`
to activate it.  Multiple sources can run in the same config — they produce
separate rows in the master CSV, distinguished by the `source` column.

---

### Sentinel-2 L2A (optical, multispectral)

**GEE collection:** `COPERNICUS/S2_SR_HARMONIZED`
**Coverage:** November 2015 – present
**Revisit:** ~5 days (10 days per satellite, two satellites)
**Resolution:** 10 m (visible/NIR), 20 m (red-edge/SWIR/SCL)
**Cloud masking:** Scene Classification Layer (SCL) — automatic, no configuration needed
**Best for:** Vegetation health, water bodies, soil moisture, land cover change

```yaml
sources:
  sentinel2:
    enabled: true
    collection: "COPERNICUS/S2_SR_HARMONIZED"   # Do not change

    # ── Cloud filtering ───────────────────────────────────────────────────
    max_tile_cloud_pct: 20    # Reject images where >20% of the full satellite
                               # tile is cloud-covered (cheap metadata filter)
    max_aoi_cloud_pct: 10     # Reject images where >10% of YOUR AOI is cloud-
                               # covered (computed from SCL band over your AOI)
                               # Set to 100 to disable AOI-level filtering

    # ── Spectral indices ─────────────────────────────────────────────────
    # List the indices you want computed. Each appears as a set of columns
    # in the output CSV: {INDEX}_mean, {INDEX}_std, {INDEX}_min, {INDEX}_max,
    # {INDEX}_p10, {INDEX}_p25, {INDEX}_p50, {INDEX}_p75, {INDEX}_p90
    indices:
      - NDVI    # vegetation health
      - NDWI    # surface water
      - NDMI    # soil/vegetation moisture

    # ── Optional outputs ─────────────────────────────────────────────────
    export_geotiff: false   # true = download one GeoTIFF per image per index
```

#### Sentinel-2 available bands

| Band | Name              | Wavelength (nm) | Resolution | Notes                          |
|------|-------------------|-----------------|------------|--------------------------------|
| B2   | Blue              | 492             | 10 m       |                                |
| B3   | Green             | 560             | 10 m       |                                |
| B4   | Red               | 665             | 10 m       |                                |
| B5   | Red Edge 1        | 704             | 20 m       | Vegetation stress              |
| B6   | Red Edge 2        | 741             | 20 m       | Vegetation stress              |
| B7   | Red Edge 3        | 783             | 20 m       | Vegetation stress              |
| B8   | NIR (broad)       | 833             | 10 m       | Vegetation, biomass            |
| B8A  | Red Edge 4 / NIR  | 865             | 20 m       | Moisture (NDMI uses this)      |
| B11  | SWIR 1            | 1614            | 20 m       | Moisture, soil water           |
| B12  | SWIR 2            | 2202            | 20 m       | Soil minerals, fire            |
| SCL  | Scene Class Layer | —               | 20 m       | Used internally for cloud mask |

#### Sentinel-2 available indices

| Index  | Formula                                              | Bands         | Range      | What it measures                                 |
|--------|------------------------------------------------------|---------------|------------|--------------------------------------------------|
| NDVI   | (B8 − B4) / (B8 + B4)                               | B8, B4        | −1 to +1   | Vegetation density and health                    |
| NDWI   | (B3 − B8) / (B3 + B8)                               | B3, B8        | −1 to +1   | Open water / surface moisture                    |
| NDMI   | (B8A − B11) / (B8A + B11)                           | B8A, B11      | −1 to +1   | Vegetation & soil moisture, subsurface water     |
| EVI    | 2.5 × (B8−B4) / (B8 + 6B4 − 7.5B2 + 1)             | B8, B4, B2    | −1 to +1   | Vegetation — less saturation than NDVI           |
| SAVI   | 1.5 × (B8−B4) / (B8+B4+0.5)                         | B8, B4        | −1.5 to +1.5| Vegetation in sparse/bare areas                  |
| MNDWI  | (B3 − B11) / (B3 + B11)                             | B3, B11       | −1 to +1   | Water — suppresses built-up/vegetation signal    |
| NDBI   | (B11 − B8) / (B11 + B8)                             | B11, B8       | −1 to +1   | Built-up / impervious surfaces                   |
| BSI    | ((B11+B4)−(B8+B2)) / ((B11+B4)+(B8+B2))             | B11,B4,B8,B2  | −1 to +1   | Bare soil and land degradation                   |
| GNDVI  | (B8 − B3) / (B8 + B3)                               | B8, B3        | −1 to +1   | Chlorophyll / crop health                        |
| NDRE   | (B8A − B5) / (B8A + B5)                             | B8A, B5*      | −1 to +1   | Early vegetation stress (more sensitive than NDVI)|

*B5 must be added to `_BANDS` in `satme/sources/sentinel2.py` before NDRE can be used.

**Interpreting values:**

- NDVI > 0.5 → dense healthy vegetation; < 0.2 → bare ground or sparse cover
- NDWI > 0 → open water likely present
- NDMI > 0.2 → high vegetation moisture; < −0.2 → dry soil / drought stress
- NDBI > 0 → built-up surface dominant in pixel

---

### Sentinel-1 SAR (radar, all-weather)

**GEE collection:** `COPERNICUS/S1_GRD`
**Coverage:** April 2014 – present
**Revisit:** ~6 days
**Resolution:** 10 m (IW mode, default)
**Cloud penetration:** Full — SAR is unaffected by cloud cover
**Status:** Stub — not yet implemented in the pipeline

```yaml
sources:
  sentinel1:
    enabled: false          # Change to true once implemented
    collection: "COPERNICUS/S1_GRD"
    orbit_direction: "ASCENDING"    # "ASCENDING" or "DESCENDING"
                                    # Use the same direction for all images
                                    # to ensure consistent viewing geometry
    instrument_mode: "IW"           # IW = Interferometric Wide Swath (land standard)
                                    # EW = Extra Wide (maritime, polar)
    polarizations: ["VV", "VH"]     # ["VV"] = co-pol only
                                    # ["VV", "VH"] = dual-pol (recommended for land)
    speckle_filter: "lee"           # "lee" or "refined_lee" or null
                                    # SAR images have speckle (salt-and-pepper noise)
                                    # Filtering smooths this before index computation
    indices:
      - RVI        # Radar Vegetation Index
      - VH_VV      # Backscatter ratio — soil moisture proxy
    export_geotiff: false
```

#### Sentinel-1 available bands (backscatter)

SAR measures microwave energy reflected back to the satellite, not sunlight.
Values are in decibels (dB) — log scale.  Wetter/denser surfaces scatter more.

| Band | Polarization              | Resolution | What it measures                                         |
|------|---------------------------|------------|----------------------------------------------------------|
| VV   | Vertical–Vertical         | 10 m       | Surface roughness; sensitive to soil moisture and water  |
| VH   | Vertical–Horizontal       | 10 m       | Volume scattering; sensitive to vegetation structure     |
| HH   | Horizontal–Horizontal     | 10 m       | Available in EW mode; common in Arctic/maritime use      |
| HV   | Horizontal–Vertical       | 10 m       | Volume scattering alternative to VH                     |

IW (land standard) mode provides VV + VH.  HH/HV require switching to EW mode.

#### Sentinel-1 available indices

No optical formulas apply — all S1 indices use backscatter ratios.

| Index  | Formula                                    | Bands     | Range        | What it measures                                        |
|--------|--------------------------------------------|-----------|--------------|---------------------------------------------------------|
| RVI    | 4×VH / (VV + VH)                           | VV, VH    | 0 to 1       | Radar Vegetation Index — vegetation density/structure   |
| VH_VV  | VH − VV  (dB difference)                  | VV, VH    | dB           | Depolarisation ratio — soil moisture and crop growth   |
| VV_lin | 10^(VV/10)                                 | VV        | linear       | Linear backscatter (not dB) — for change detection     |
| VH_lin | 10^(VH/10)                                 | VH        | linear       | Linear backscatter                                     |
| DPSVI  | VV × (VV + VH) / (4 × VH)                | VV, VH    | unitless     | Dual-Pol SAR Vegetation Index                          |

**Interpreting values:**

- Higher VH → more vegetation volume scattering
- Higher VV → wetter soil surface or open water
- RVI near 0 → bare soil or water; near 1 → dense vegetation
- VH−VV becomes more negative over water (VV dominates); near 0 over crops

---

### Suomi NPP — VIIRS Nighttime Lights (VIIRS DNB)

**GEE collection (monthly):** `NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG`
**GEE collection (annual):**  `NOAA/VIIRS/DNB/ANNUAL_V1`
**Coverage:** April 2012 – present
**Revisit:** Daily composited to monthly or annual
**Resolution:** ~500 m (0.004167°)
**Cloud masking:** Stray-light corrected, cloud-free composited (built into product)
**Best for:** Human settlement, economic activity, infrastructure, electrification,
             post-conflict/disaster recovery, forced displacement

```yaml
sources:
  viirs:
    enabled: true
    collection: "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG"  # monthly composites (recommended)
    # collection: "NOAA/VIIRS/DNB/ANNUAL_V1"           # annual composites (less noise)

    # VIIRS has no cloud threshold — the monthly composite is already cloud-free.
    # Images with insufficient cloud-free observations are masked automatically.
    min_cf_cvg: 1           # Minimum cloud-free coverage count per pixel
                             # (cf_cvg band — how many cloud-free nights went into
                             # the composite). Set to 1 to require at least one
                             # cloud-free observation. Higher = more reliable.

    export_geotiff: false   # true = download monthly radiance rasters
```

#### VIIRS available bands

| Band      | Name                          | Unit           | Resolution | Notes                                               |
|-----------|-------------------------------|----------------|------------|-----------------------------------------------------|
| avg_rad   | Average Radiance              | nW/cm²/sr      | ~500 m     | Primary output — mean nighttime radiance in AOI     |
| cf_cvg    | Cloud-Free Coverage           | count (nights) | ~500 m     | How many cloud-free nights went into the composite  |

The pipeline reduces `avg_rad` over the AOI using the same stats as optical indices
(mean, std, min, max, percentiles).  `cf_cvg` is used as a data quality flag —
low values mean the monthly composite is based on few observations.

#### VIIRS available indices

VIIRS is a single-band product — there are no multi-band spectral indices.
Instead, the statistics of `avg_rad` over the AOI are the primary outputs.

| Metric          | Interpretation                                                          |
|-----------------|-------------------------------------------------------------------------|
| avg_rad_mean    | Mean nighttime radiance over AOI — primary economic activity proxy       |
| avg_rad_p90     | 90th percentile — captures bright spots (urban cores, active sites)      |
| avg_rad_p50     | Median — robust to outliers from fires or flares                         |
| avg_rad_std     | Standard deviation — spread of light; high = heterogeneous settlement    |
| avg_rad_max     | Maximum pixel — identifies the single brightest point (e.g. gas flare)  |

**Interpreting values:**

- avg_rad < 0.5 nW/cm²/sr → rural / uninhabited
- avg_rad 0.5–5 → small towns / dispersed settlement
- avg_rad > 5 → urban areas / industrial sites
- avg_rad > 50 → dense urban cores / ports / airports
- A sudden drop in avg_rad (pre→post) → conflict, displacement, power outage
- A sustained rise → economic recovery, electrification, new settlements

**Note on temporal resolution:** VIIRS is monthly, not daily.  One output row
per month, not per satellite overpass.  You cannot match VIIRS images 1:1 with
Sentinel-2 overpasses.  Use `date_range` to span your full study period and let
the pipeline produce a monthly time series alongside the optical time series.

---

### CHIRPS Rainfall (ancillary — not a satellite sensor)

**GEE collection:** `UCSB-CHG/CHIRPS/DAILY`
**Coverage:** January 1981 – present (near real-time, ~3-week lag)
**Resolution:** ~5.5 km (0.05°)
**Cloud masking:** Not applicable — CHIRPS is a modelled rainfall product
**Best for:** Precipitation control variable; isolating satellite signal from
             rainfall effects on vegetation and soil moisture

```yaml
sources:
  chirps:
    enabled: true
    collection: "UCSB-CHG/CHIRPS/DAILY"   # Do not change
    accumulation_days: 30    # Sum rainfall over the N days BEFORE each image date
                              # 30 = one month of antecedent rainfall (default)
                              # 7  = one week (short-term moisture)
                              # 90 = seasonal drought index
    export_geotiff: false    # true = download accumulated rainfall rasters
```

The CHIRPS value is added as `chirps_30d_mm` (or `chirps_Nd_mm`) to each row
of the stats CSV.  It represents the mean accumulated rainfall in mm over the
AOI for the N days preceding each satellite image's acquisition date.

---

## `output`

```yaml
output:
  base_dir: "outputs/runs"       # Root directory for all output files
                                  # Run outputs go in: {base_dir}/{run.name}/

  stats_csv: true                 # Write stats.csv (one row per image)
  flag_report: true               # Write flag_report.csv (all images including rejected)

  # GeoTIFF options (only relevant if any source has export_geotiff: true)
  skip_existing: true             # Skip GeoTIFFs already on disk (resume a run)

  download_method: "auto"         # "auto"  = getDownloadUrl for small AOIs, Drive for large
                                  # "local" = always getDownloadUrl (no Drive required)
                                  # "drive" = always export to Google Drive

  size_threshold_km2: 25.0        # AOIs larger than this use Drive export
                                  # getDownloadUrl has a ~32 MB / ~100 km² limit

  drive_folder: "satme_exports"   # Google Drive folder name (Drive export only)
```

---

## `stats`

Controls which statistics are computed for each index over the AOI.

```yaml
stats:
  percentiles: [10, 25, 50, 75, 90]   # Percentiles to compute; empty list = none
                                        # Each becomes a column: {INDEX}_p{N}
  include_mean:   true    # {INDEX}_mean
  include_stddev: true    # {INDEX}_std
  include_min_max: true   # {INDEX}_min and {INDEX}_max
```

All stats apply to valid (non-masked) pixels only.  For optical sources,
"valid" means pixels not classified as cloud, shadow, or no-data by the
SCL mask.  For VIIRS, "valid" means pixels with cf_cvg ≥ min_cf_cvg.

---

## Output files

Every run writes to `{output.base_dir}/{run.name}/`:

| File              | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| `stats.csv`       | One row per clean image per source. Wide format — all indices as columns.  |
| `flag_report.csv` | All images seen (including rejected). Records why each was excluded.        |
| `run_metadata.json` | Full config snapshot, AOI geometry (WKT), GEE image counts, timestamp.  |
| `geotiffs/`       | Index GeoTIFFs if `export_geotiff: true` (one file per image per index).   |

### `stats.csv` columns

```
date | source | image_id | pre_post |
aoi_cloud_pct | tile_cloud_pct |
{INDEX}_mean | {INDEX}_std | {INDEX}_min | {INDEX}_max | {INDEX}_p{N} ... |
chirps_{N}d_mm |
flags |
mgrs_tile | orbit_number | processing_baseline
```

### Flag codes

| Flag                | Meaning                                                           |
|---------------------|-------------------------------------------------------------------|
| `OUT_OF_SEASON`     | Image date is outside `target_months`                             |
| `HIGH_TILE_CLOUD`   | Tile-level cloud % exceeds `max_tile_cloud_pct`                   |
| `HIGH_AOI_CLOUD`    | AOI-level cloud % exceeds `max_aoi_cloud_pct`                     |
| `PARTIAL_AOI_COVERAGE` | Image does not fully cover the AOI                             |
| `PRE_INTERVENTION`  | Image date is before `reference_date`                             |
| `POST_INTERVENTION` | Image date is on or after `reference_date`                        |
| `NEAR_INTERVENTION` | Image date is within 60 days of `reference_date`                  |
| `NO_DATA`           | No valid pixels found in AOI after masking                        |

---

## Satellite comparison summary

| Attribute          | Sentinel-2          | Sentinel-1 SAR      | VIIRS Nightlights          |
|--------------------|---------------------|---------------------|----------------------------|
| Type               | Optical             | Radar (SAR)         | Thermal/optical (nighttime)|
| GEE collection     | COPERNICUS/S2_SR_HARMONIZED | COPERNICUS/S1_GRD | NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG |
| Archive start      | Nov 2015            | Apr 2014            | Apr 2012                   |
| Revisit            | ~5 days             | ~6 days             | Monthly composite          |
| Resolution         | 10–20 m             | 10 m                | ~500 m                     |
| Cloud affected?    | Yes                 | No                  | No (pre-composited)        |
| Primary output     | Spectral indices    | Backscatter ratios  | Radiance (nW/cm²/sr)       |
| Implemented        | Yes                 | Stub                | Stub                       |

---

## Minimal working config

```yaml
run:
  name: "my_study"
  reference_date: "2020-01-01"

aoi:
  mode: "point_radius"
  center: { lat: 0.0, lon: 0.0 }
  radius_m: 1000

date_range:
  start: "2018-01-01"
  end:   "2023-12-31"

sources:
  sentinel2:
    enabled: true
    max_tile_cloud_pct: 30
    max_aoi_cloud_pct: 20
    indices: [NDVI, NDWI]
    export_geotiff: false
  chirps:
    enabled: true
    accumulation_days: 30
    export_geotiff: false

output:
  base_dir: "outputs/runs"
  download_method: "local"

stats:
  percentiles: [25, 50, 75]
  include_mean: true
  include_stddev: true
  include_min_max: false
```
