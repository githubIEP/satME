// ============================================================
// Amukura, Busia County — Wet vs Dry Season Viewer
// Sentinel-2 L2A | COPERNICUS/S2_SR_HARMONIZED
//
// Dry image : 20 Feb 2023  (NDVI p50 = 0.377, BSI = +0.158, cloud = 0%)
// Wet image : 21 May 2023  (NDVI p50 = 0.740, BSI = -0.158, cloud = 0%)
// NDVI swing: +0.363 — same year, zero cloud, maximum contrast
//
// Paste this entire script into code.earthengine.google.com and click Run.
// ============================================================

// ── AOI ──────────────────────────────────────────────────────
var aoi = ee.Geometry.Polygon([[
  [34.14, 0.42],
  [34.16, 0.42],
  [34.16, 0.44],
  [34.14, 0.44],
  [34.14, 0.42]
]]);

// ── Load the two specific images by system:index ─────────────
var collection = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED');

var dryImage = collection
  .filter(ee.Filter.eq('system:index', '20230220T074941_20230220T080450_T36NXF'))
  .first();

var wetImage = collection
  .filter(ee.Filter.eq('system:index', '20230521T074611_20230521T080451_T36NXF'))
  .first();

// ── Cloud mask (SCL-based, same logic as satME pipeline) ─────
// SCL classes masked out: 0=no data, 1=saturated, 3=shadow,
//                         8=cloud medium prob, 9=cloud high prob, 10=cirrus
function maskClouds(img) {
  var scl = img.select('SCL');
  var mask = scl.neq(0).and(scl.neq(1)).and(scl.neq(3))
                .and(scl.neq(8)).and(scl.neq(9)).and(scl.neq(10));
  return img.updateMask(mask).divide(10000); // scale to 0–1 reflectance
}

var dryMasked = maskClouds(dryImage);
var wetMasked = maskClouds(wetImage);

// ── NDVI computation ─────────────────────────────────────────
function addNDVI(img) {
  return img.normalizedDifference(['B8', 'B4']).rename('NDVI');
}

var dryNDVI = addNDVI(dryMasked);
var wetNDVI  = addNDVI(wetMasked);

// ── BSI computation ──────────────────────────────────────────
// BSI = ((B11 + B4) - (B8 + B2)) / ((B11 + B4) + (B8 + B2))
function addBSI(img) {
  var num = img.select('B11').add(img.select('B4'))
              .subtract(img.select('B8').add(img.select('B2')));
  var den = img.select('B11').add(img.select('B4'))
              .add(img.select('B8').add(img.select('B2')));
  return num.divide(den).rename('BSI');
}

var dryBSI = addBSI(dryMasked);
var wetBSI  = addBSI(wetMasked);

// ── Visualisation parameters ─────────────────────────────────
var trueColorVis = {
  bands: ['B4', 'B3', 'B2'],
  min: 0.0,
  max: 0.25,
  gamma: 1.2
};

var ndviVis = {
  min: 0.1,
  max: 0.8,
  palette: [
    '#d73027', // red    — bare / very sparse
    '#fc8d59', // orange
    '#fee08b', // yellow — sparse
    '#d9ef8b', // light green
    '#91cf60', // green
    '#1a9850'  // dark green — dense canopy
  ]
};

var bsiVis = {
  min: -0.2,
  max: 0.25,
  palette: [
    '#1a9850', // dark green — vegetated (low BSI)
    '#d9ef8b', // yellow-green
    '#fee08b', // yellow
    '#fc8d59', // orange
    '#d73027'  // red — bare soil (high BSI)
  ]
};

// ── Add layers ────────────────────────────────────────────────
// True colour
Map.addLayer(dryMasked,  trueColorVis, 'DRY  — True Colour  (20 Feb 2023)', true);
Map.addLayer(wetMasked,  trueColorVis, 'WET  — True Colour  (21 May 2023)', true);

// NDVI
Map.addLayer(dryNDVI, ndviVis, 'DRY  — NDVI  (20 Feb 2023)  p50=0.377', false);
Map.addLayer(wetNDVI,  ndviVis, 'WET  — NDVI  (21 May 2023)  p50=0.740', false);

// BSI
Map.addLayer(dryBSI, bsiVis, 'DRY  — BSI   (20 Feb 2023)  p50=+0.158', false);
Map.addLayer(wetBSI,  bsiVis, 'WET  — BSI   (21 May 2023)  p50=-0.158', false);

// AOI boundary
Map.addLayer(
  ee.Image().paint(aoi, 1, 2),
  {palette: ['#ffffff']},
  'AOI boundary'
);

// ── Centre map on AOI ─────────────────────────────────────────
Map.centerObject(aoi, 13);
Map.setOptions('SATELLITE');

// ── Console summary ───────────────────────────────────────────
print('=== Amukura Wet vs Dry — Image Info ===');
print('DRY image (20 Feb 2023):', dryImage.select(['B4','B3','B2','B8','B11','SCL']));
print('WET image (21 May 2023):', wetImage.select(['B4','B3','B2','B8','B11','SCL']));

// Confirm NDVI stats over the AOI match satME pipeline output
var dryStats = dryNDVI.reduceRegion({
  reducer: ee.Reducer.percentile([50]).combine(ee.Reducer.mean(), sharedInputs=true),
  geometry: aoi,
  scale: 10,
  bestEffort: true
});
var wetStats = wetNDVI.reduceRegion({
  reducer: ee.Reducer.percentile([50]).combine(ee.Reducer.mean(), sharedInputs=true),
  geometry: aoi,
  scale: 10,
  bestEffort: true
});

print('DRY NDVI stats (should match: p50≈0.377, mean≈0.446):', dryStats);
print('WET NDVI stats (should match: p50≈0.740, mean≈0.706):', wetStats);

// ── Layer visibility guide ────────────────────────────────────
// In the Layers panel (top right), toggle between:
//
//   DRY True Colour  vs  WET True Colour
//     → fields look brownish/yellow in Feb, bright green in May
//
//   DRY NDVI  vs  WET NDVI
//     → reds/oranges in Feb, deep greens in May
//
//   DRY BSI  vs  WET BSI
//     → reds (bare soil) in Feb, greens (vegetated) in May
//     → BSI gives the clearest bare-soil signal
