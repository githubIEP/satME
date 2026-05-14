// ============================================================
// Amukura, Busia County — VIIRS Nighttime Lights Viewer
// Collection: NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG
// Date range: 2019-01-01 to 2024-12-31  (72 monthly composites)
//
// satME pipeline results:
//   Overall median avg_rad : 1.24 nW/cm2/sr  (villages / small towns)
//   Dimmest month  : Apr 2021  (0.48 nW/cm2/sr)
//   Brightest month: Oct 2024  (2.31 nW/cm2/sr)
//   Trend: ~2x increase 2019 → 2024 (electrification expansion)
//
// Paste into code.earthengine.google.com and click Run.
// ============================================================

// ── AOI ──────────────────────────────────────────────────────
var aoi = ee.Geometry.Polygon([[
  [34.14, 0.42],
  [34.16, 0.42],
  [34.16, 0.44],
  [34.14, 0.44],
  [34.14, 0.42]
]]);

// Wider view for context — VIIRS is 500m so the AOI is only ~4x4 pixels
var viewArea = aoi.buffer(10000);  // 10 km buffer for map context

// ── Load full VIIRS collection ────────────────────────────────
var viirs = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG')
  .filterBounds(aoi)
  .filterDate('2019-01-01', '2024-12-31')
  .select(['avg_rad', 'cf_cvg']);

print('Total monthly composites:', viirs.size());

// ── Quality mask (cf_cvg >= 1) ────────────────────────────────
function maskViirs(img) {
  var valid = img.select('cf_cvg').gte(1);
  return img.updateMask(valid);
}

var viirsMasked = viirs.map(maskViirs);

// ── Visualisation parameters ──────────────────────────────────
// Scale: 0–3 nW/cm2/sr covers the rural-to-small-town range at Amukura
// Anything > 3 is likely a flare, market, or nearby town centre
var radVis = {
  bands: ['avg_rad'],
  min: 0,
  max: 3,
  palette: [
    '#000000',  // black  — no light
    '#1a1a2e',  // near-black
    '#16213e',  // dark blue
    '#0f3460',  // blue
    '#533483',  // purple
    '#e94560',  // pink
    '#f5a623',  // orange
    '#ffffff'   // white  — very bright
  ]
};

// ── Composite images for map display ─────────────────────────
// Early period (2019) vs recent (2024) — shows the trend
var avg2019 = viirsMasked
  .filterDate('2019-01-01', '2019-12-31')
  .mean();

var avg2024 = viirsMasked
  .filterDate('2024-01-01', '2024-12-31')
  .mean();

// Full period mean
var avgAll = viirsMasked.mean();

// Dimmest and brightest single months from satME analysis
var dimmest   = viirsMasked
  .filterDate('2021-04-01', '2021-04-30').first();  // p50 = 0.48

var brightest = viirsMasked
  .filterDate('2024-10-01', '2024-10-31').first();  // p50 = 2.31

// ── Add map layers ────────────────────────────────────────────
Map.addLayer(avgAll,   radVis, 'Mean radiance 2019-2024',     true);
Map.addLayer(avg2019,  radVis, 'Mean radiance 2019',          false);
Map.addLayer(avg2024,  radVis, 'Mean radiance 2024',          false);
Map.addLayer(dimmest,  radVis, 'Dimmest month  (Apr 2021)',   false);
Map.addLayer(brightest,radVis, 'Brightest month (Oct 2024)',  false);

// AOI outline
Map.addLayer(
  ee.Image().paint(aoi, 1, 2),
  {palette: ['#00ff00']},
  'AOI boundary (green)'
);

// ── Centre map ────────────────────────────────────────────────
// Zoom out — at zoom 13 VIIRS pixels are too small to see well
Map.centerObject(aoi, 11);
Map.setOptions('HYBRID');  // satellite + roads for context

// ── Time series chart ─────────────────────────────────────────
// This is the most useful output — shows the 2019-2024 trend
var chart = ui.Chart.image.series({
  imageCollection: viirsMasked.select('avg_rad'),
  region: aoi,
  reducer: ee.Reducer.mean(),
  scale: 500,
  xProperty: 'system:time_start'
})
.setChartType('LineChart')
.setOptions({
  title: 'Amukura — VIIRS Nighttime Radiance 2019–2024',
  vAxis: {
    title: 'avg_rad (nW/cm²/sr)',
    viewWindow: {min: 0, max: 4}
  },
  hAxis: {title: 'Date'},
  lineWidth: 2,
  pointSize: 4,
  colors: ['#f5a623'],
  series: {0: {labelInLegend: 'Monthly mean avg_rad over AOI'}},
});

print(chart);

// ── Annual summary in console ─────────────────────────────────
print('=== Annual mean avg_rad over AOI ===');
var years = ee.List.sequence(2019, 2024);
var annualMeans = years.map(function(y) {
  var yr = ee.Number(y);
  var mean = viirsMasked
    .filterDate(
      ee.Date.fromYMD(yr, 1, 1),
      ee.Date.fromYMD(yr, 12, 31)
    )
    .select('avg_rad')
    .mean()
    .reduceRegion({reducer: ee.Reducer.mean(), geometry: aoi, scale: 500})
    .get('avg_rad');
  return ee.Feature(null, {year: yr, avg_rad_mean: mean});
});
print(ee.FeatureCollection(annualMeans));

// ── Interpretation guide ──────────────────────────────────────
// avg_rad thresholds (nW/cm2/sr):
//   < 0.5   Rural / uninhabited
//   0.5–2   Villages / small towns      <-- Amukura sits here
//   2–10    Suburban / small city
//   10–50   Urban / commercial
//   > 50    Dense city / industrial / gas flare
//
// The upward trend (0.8 in 2019 → 1.6 in 2024) reflects
// ongoing electrification in Busia County under Kenya's
// Last Mile Connectivity Programme.
