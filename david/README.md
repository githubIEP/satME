# Sand Dam Vegetation Impact Assessment

Self-contained exploration of the Machakos sand dam (constructed October 2018, AOI -1.5435, 37.3326), separate from the production `satme/` pipeline.

## Layout

```
david/
├── exploration/            # Python package — analysis pipeline
│   ├── config.py           # AOIs, dates, thresholds
│   ├── cache.py            # zarr open/save
│   ├── indices.py          # NDVI, MNDWI, valid masks
│   ├── fetch.py            # STAC search + cache build
│   ├── aggregate.py        # per-pixel pre/post, DOY binning, ring buffers
│   ├── stats.py            # MK, Theil-Sen, Mann-Whitney, DiD scalars
│   ├── precipitation.py    # Open-Meteo / ERA5 daily precipitation
│   ├── visualisation.py    # all rendering
│   ├── outputs/            # generated figures, CSVs and JSON
│   └── scripts/
│       ├── step01_build_cache.py
│       ├── step02_fetch_precipitation.py
│       ├── step03_run_analysis.py
│       └── step04_render_outputs.py
├── _cache_fulltimeseries/  # ~28 MB; per-AOI zarr + precipitation CSV
├── sanddam.kml             # WSW arm of channel (upstream of dam wall)
├── sandam-upstream.kml     # NNE arm of channel (downstream of dam wall)
├── requirements.txt
├── REPORT.md               # the report (v6.4, IEP house style)
└── REPORT.pdf              # PDF rendering of REPORT.md
```

The KML file names predate the elevation check that confirmed flow direction. The mapping in code is correct (see `exploration/scripts/step03_run_analysis.py`).

## Running

From the `satME/` repo root:

```
python -m david.exploration.scripts.step03_run_analysis
python -m david.exploration.scripts.step04_render_outputs
```

If `_cache_fulltimeseries/` is present, both scripts complete in under a minute. If the cache has been deleted or invalidated, run `step01_build_cache.py` first (slow, ~30 to 60 minutes, network-bound).

## Regenerating REPORT.pdf

```
pandoc david/REPORT.md -o david/REPORT.pdf \
  --pdf-engine=xelatex \
  --resource-path=david \
  --toc \
  -V geometry:a4paper \
  -V geometry:margin=2cm \
  -V colorlinks=true \
  -V mainfont="DejaVu Serif" \
  -V monofont="DejaVu Sans Mono"
```

Image paths in `REPORT.md` are written as `backend/exploration/outputs/...` for compatibility with the original repo. After moving into `satME/david/`, the actual outputs are at `david/exploration/outputs/...`. The `--resource-path=david` flag tells pandoc to also look there. Update image paths in `REPORT.md` if you want a cleaner build invocation.

## Dependencies

```
pip install -r requirements.txt
```

Plus, for the analysis pipeline used here:

```
pip install pystac-client planetary-computer odc-stac rioxarray xarray zarr \
  pandas numpy scipy matplotlib dask shapely pyproj rasterio
```

The full set is implicit in the existing imports; the requirements.txt was kept light because the original repo bundled with the productised DevSat backend rather than this exploration alone.
