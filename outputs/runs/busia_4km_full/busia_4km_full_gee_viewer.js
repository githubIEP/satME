// ============================================================
// Busia WV2 AOI — GEE Viewer
// Sentinel-2: NDVI + NDMI  |  VIIRS: Nighttime Lights
// Date range: 2019-01-01 to 2024-12-31
//
// AOI matches the WorldView-2 image extents (Feb + May 2025).
// Coords derived from WV2 image bounds reprojected to WGS84.
//
// Paste into code.earthengine.google.com and click Run.
// ============================================================


// ── AOI — matches WV2 image footprint ────────────────────────
// Source: rasterio bounds of 25FEB11 and 25MAY27 M2AS TIFs,
// reprojected from UTM zone 36N to WGS84. Both images cover
// almost identical extents (< 2 m difference at each edge).
var aoi = ee.Geometry.Polygon([[
  [34.169575, 0.431325],   // SW
  [34.187615, 0.431325],   // SE
  [34.187615, 0.449809],   // NE
  [34.169575, 0.449809],   // NW
  [34.169575, 0.431325]    // close ring
]]);

var START = '2019-01-01';
var END   = '2024-12-31';


// ════════════════════════════════════════════════════════════
// SENTINEL-2 — NDVI & NDMI
// ════════════════════════════════════════════════════════════

// ── Cloud mask (SCL-based, Sentinel-2 L2A) ────────────────────
// SCL classes masked out:
//   0  = No data
//   1  = Saturated / defective
//   3  = Cloud shadow
//   8  = Cloud medium probability
//   9  = Cloud high probability
//   10 = Thin cirrus
function maskS2clouds(img) {
  var scl   = img.select('SCL');
  var valid = scl.neq(0).and(scl.neq(1)).and(scl.neq(3))
                .and(scl.neq(8)).and(scl.neq(9)).and(scl.neq(10));
  return img.updateMask(valid);
}

// ── Load and filter Sentinel-2 ────────────────────────────────
var s2 = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
  .filterBounds(aoi)
  .filterDate(START, END)
  .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 40))
  .map(maskS2clouds);

print('Sentinel-2 images (cloud < 40%):', s2.size());

// ── Compute NDVI and NDMI for each image ─────────────────────
// NDVI = (B8 - B4) / (B8 + B4)
//   B8  = NIR (~833 nm)
//   B4  = Red (~665 nm)
//   High NDVI (+0.6 to +0.9) = dense healthy vegetation
//   Low  NDVI (+0.1 to +0.3) = sparse / stressed vegetation
//   Negative NDVI            = water, bare soil, built surface

// NDMI = (B8A - B11) / (B8A + B11)
//   B8A = Narrow NIR (~865 nm)
//   B11 = SWIR-1     (~1610 nm)
//   High NDMI (+0.3 to +0.6) = high vegetation moisture
//   Low  NDMI (-0.1 to +0.1) = moderate moisture / sparse canopy
//   Negative NDMI            = dry vegetation / bare soil

function addIndices(img) {
  var ndvi = img.normalizedDifference(['B8',  'B4' ]).rename('NDVI');
  var ndmi = img.normalizedDifference(['B8A', 'B11']).rename('NDMI');
  return img.addBands([ndvi, ndmi])
            .set('system:time_start', img.get('system:time_start'));
}

var s2_idx = s2.map(addIndices);

// ── Seasonal composites for map display ──────────────────────
// Dry season: Jan–Feb (low NDVI, high BSI)
// Wet season: May–Jun (peak NDVI, high NDMI)
var dry = s2_idx
  .filter(ee.Filter.calendarRange(1, 2, 'month'))
  .median();

var wet = s2_idx
  .filter(ee.Filter.calendarRange(5, 6, 'month'))
  .median();

var fullMedian = s2_idx.median();


// ════════════════════════════════════════════════════════════
// VIIRS — NIGHTTIME LIGHTS
// ════════════════════════════════════════════════════════════

// ── Quality mask (require at least 1 cloud-free night) ────────
function maskViirs(img) {
  return img.updateMask(img.select('cf_cvg').gte(1));
}

var viirs = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG')
  .filterBounds(aoi)
  .filterDate(START, END)
  .map(maskViirs)
  .select(['avg_rad', 'cf_cvg']);

print('VIIRS monthly composites:', viirs.size());

// Annual means for map layers
var viirs2019 = viirs.filterDate('2019-01-01', '2019-12-31').mean();
var viirs2022 = viirs.filterDate('2022-01-01', '2022-12-31').mean();
var viirs2024 = viirs.filterDate('2024-01-01', '2024-12-31').mean();
var viirsAll  = viirs.mean();

// avg_rad thresholds (nW/cm²/sr):
//   < 0.5    Rural / uninhabited
//   0.5 – 2  Villages / small towns   <-- expected range for this AOI
//   2 – 10   Suburban / small city
//   10 – 50  Urban / commercial
//   > 50     Dense city / industrial / gas flare


// ════════════════════════════════════════════════════════════
// VISUALISATION PARAMETERS
// ════════════════════════════════════════════════════════════

var ndviVis = {
  bands: ['NDVI'],
  min: -0.1, max: 0.9,
  palette: ['#d73027','#f46d43','#fdae61','#fee08b',
            '#d9ef8b','#a6d96a','#66bd63','#1a9850']
};

var ndmiVis = {
  bands: ['NDMI'],
  min: -0.3, max: 0.5,
  palette: ['#8c510a','#bf812d','#dfc27d','#f6e8c3',
            '#c7eae5','#80cdc1','#35978f','#01665e']
};

var rgbVis = {
  bands: ['B4', 'B3', 'B2'],
  min: 0, max: 2500
};

var radVis = {
  bands: ['avg_rad'],
  min: 0, max: 3,
  palette: ['#000000','#1a1a2e','#16213e','#0f3460',
            '#533483','#e94560','#f5a623','#ffffff']
};


// ════════════════════════════════════════════════════════════
// MAP LAYERS
// ════════════════════════════════════════════════════════════

Map.centerObject(aoi, 13);
Map.setOptions('HYBRID');

// True colour — full period median
Map.addLayer(fullMedian, rgbVis, 'S2 True Colour (median 2019-2024)', false);

// NDVI
Map.addLayer(fullMedian, ndviVis, 'NDVI — Full period median', true);
Map.addLayer(dry,        ndviVis, 'NDVI — Dry season median (Jan–Feb)', false);
Map.addLayer(wet,        ndviVis, 'NDVI — Wet season median (May–Jun)', false);

// NDMI
Map.addLayer(fullMedian, ndmiVis, 'NDMI — Full period median', false);
Map.addLayer(dry,        ndmiVis, 'NDMI — Dry season median (Jan–Feb)', false);
Map.addLayer(wet,        ndmiVis, 'NDMI — Wet season median (May–Jun)', false);

// VIIRS nightlights
Map.addLayer(viirsAll,  radVis, 'VIIRS — Mean radiance 2019-2024', false);
Map.addLayer(viirs2019, radVis, 'VIIRS — Mean radiance 2019', false);
Map.addLayer(viirs2022, radVis, 'VIIRS — Mean radiance 2022', false);
Map.addLayer(viirs2024, radVis, 'VIIRS — Mean radiance 2024', false);

// AOI boundary
Map.addLayer(
  ee.Image().paint(aoi, 1, 2),
  {palette: ['#ff0000']},
  'AOI boundary'
);


// ════════════════════════════════════════════════════════════
// TIME SERIES CHARTS
// ════════════════════════════════════════════════════════════

// ── NDVI time series ──────────────────────────────────────────
var ndviChart = ui.Chart.image.series({
  imageCollection: s2_idx.select('NDVI'),
  region: aoi,
  reducer: ee.Reducer.median(),
  scale: 10,
  xProperty: 'system:time_start'
})
.setChartType('LineChart')
.setOptions({
  title: 'Busia 4km — NDVI 2019–2024  (Sentinel-2, median over AOI)',
  vAxis: {
    title: 'NDVI',
    viewWindow: {min: -0.1, max: 1.0},
    gridlines: {count: 6}
  },
  hAxis: {title: 'Date'},
  lineWidth: 2,
  pointSize: 3,
  colors: ['#1a9850'],
  series: {0: {labelInLegend: 'NDVI median'}},
});
print(ndviChart);

// ── NDMI time series ──────────────────────────────────────────
var ndmiChart = ui.Chart.image.series({
  imageCollection: s2_idx.select('NDMI'),
  region: aoi,
  reducer: ee.Reducer.median(),
  scale: 10,
  xProperty: 'system:time_start'
})
.setChartType('LineChart')
.setOptions({
  title: 'Busia 4km — NDMI 2019–2024  (Sentinel-2, median over AOI)',
  vAxis: {
    title: 'NDMI',
    viewWindow: {min: -0.4, max: 0.6},
    gridlines: {count: 6}
  },
  hAxis: {title: 'Date'},
  lineWidth: 2,
  pointSize: 3,
  colors: ['#01665e'],
  series: {0: {labelInLegend: 'NDMI median'}},
});
print(ndmiChart);

// ── VIIRS nightlights time series ─────────────────────────────
var viirsChart = ui.Chart.image.series({
  imageCollection: viirs.select('avg_rad'),
  region: aoi,
  reducer: ee.Reducer.median(),
  scale: 500,
  xProperty: 'system:time_start'
})
.setChartType('ColumnChart')
.setOptions({
  title: 'Busia 4km — VIIRS Nighttime Radiance 2019–2024  (monthly median over AOI)',
  vAxis: {
    title: 'avg_rad (nW/cm²/sr)',
    viewWindow: {min: 0, max: 3},
    gridlines: {count: 4}
  },
  hAxis: {title: 'Date'},
  colors: ['#f5a623'],
  series: {0: {labelInLegend: 'avg_rad monthly median'}},
});
print(viirsChart);

// ── Annual VIIRS summary ──────────────────────────────────────
print('=== Annual mean VIIRS avg_rad over AOI ===');
var years = ee.List.sequence(2019, 2024);
var annualViirs = years.map(function(y) {
  var yr   = ee.Number(y);
  var mean = viirs
    .filterDate(ee.Date.fromYMD(yr, 1, 1), ee.Date.fromYMD(yr, 12, 31))
    .select('avg_rad')
    .mean()
    .reduceRegion({reducer: ee.Reducer.median(), geometry: aoi, scale: 500})
    .get('avg_rad');
  return ee.Feature(null, {year: yr, avg_rad_median: mean});
});
print(ee.FeatureCollection(annualViirs));


// ════════════════════════════════════════════════════════════
// INTERPRETATION GUIDE
// ════════════════════════════════════════════════════════════
//
// NDVI seasonal pattern (western Kenya, Teso South):
//   Jan–Feb  : dry season — NDVI 0.3–0.5 (bare fields, stubble)
//   Mar–Apr  : long rains onset — NDVI rising rapidly
//   May–Jun  : long rains peak — NDVI 0.6–0.8 (dense crop canopy)
//   Jul–Aug  : second dry — NDVI dip
//   Oct–Nov  : short rains — second smaller NDVI peak
//
// NDMI interpretation:
//   > +0.3   High vegetation moisture (active crop growth)
//     0 to +0.3  Moderate moisture
//   < 0      Dry vegetation or bare soil
//
// VIIRS avg_rad thresholds (nW/cm²/sr):
//   < 0.5    Rural / uninhabited
//   0.5–2    Villages / small towns
//   2–10     Suburban / small city
//   > 10     Urban / commercial
//
// Cloud note:
//   May–Jun images may be sparse due to long-rains cloud cover.
//   The time series will show gaps — this is expected, not an error.
