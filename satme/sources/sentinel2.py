"""Sentinel-2 L2A source — fully implemented.

Collection : COPERNICUS/S2_SR_HARMONIZED
Archive    : November 2015 onwards
Resolution : 10 m (B2/B3/B4/B8), 20 m (B8A/B11)

Band mapping
------------
B2  = Blue
B3  = Green
B4  = Red
B8  = NIR (10 m)
B8A = Red Edge 4 / Narrow NIR (20 m)
B11 = SWIR1 (20 m)
SCL = Scene Classification Layer (20 m)

Cloud masking
-------------
SCL-based pixel mask.  Invalid SCL classes: 0, 1, 3, 8, 9, 10.
See cloud_mask.py for details.

Resolution note
---------------
NDMI requires B8A and B11 (both 20 m).  When computing NDMI the pipeline
works at 20 m.  All other indices use the 10 m bands.  The export scale
is controlled per-index via export_scale().

Tile-level cloud %
------------------
``CLOUDY_PIXEL_PERCENTAGE`` image property — recorded in stats but the
AOI-level cloud % (computed from pixel counts) is what drives filtering.
"""

import logging
from datetime import date

import ee

from satme.sources.base import SatelliteSource
from satme.cloud_mask import sentinel2_scl
from satme.indices import compute as compute_index_fn, validate_indices
from satme.image_filter import scl_quality_fn

logger = logging.getLogger(__name__)

# Bands included in the filtered collection (SCL always fetched for masking)
_BANDS = ["B2", "B3", "B4", "B8", "B8A", "B11", "SCL"]

# Export resolution per index (metres)
INDEX_SCALE = {
    "NDVI": 10,
    "NDWI": 10,
    "EVI":  10,
    "SAVI": 10,
    "MNDWI": 10,
    "NDBI": 10,
    "BSI":  10,
    "GNDVI": 10,
    "NDRE": 20,   # uses B5 (20 m)
    "NDMI": 20,   # uses B8A + B11 (20 m)
}
DEFAULT_SCALE = 10

# GEE properties to fetch in batch metadata
_META_PROPERTIES = [
    "CLOUDY_PIXEL_PERCENTAGE",
    "MGRS_TILE",
    "SENSING_ORBIT_NUMBER",
    "PROCESSING_BASELINE",
]


class Sentinel2Source(SatelliteSource):
    source_name    = "sentinel2"
    collection_id  = "COPERNICUS/S2_SR_HARMONIZED"
    available_bands = ["B2", "B3", "B4", "B8", "B8A", "B11"]
    archive_start  = "2015-11-01"
    default_scale  = 20   # 20 m covers NDMI's native resolution

    def __init__(self, src_cfg: dict):
        self.cfg = src_cfg
        self.max_tile_cloud     = src_cfg.get("max_tile_cloud_pct", 100)
        self.max_aoi_cloud      = src_cfg.get("max_aoi_cloud_pct", 100)
        self.requested_indices  = src_cfg.get("indices", ["NDVI"])

        issues = validate_indices(self.requested_indices, self.available_bands)
        if issues:
            raise ValueError(f"Sentinel-2 index configuration errors: {issues}")

    # ------------------------------------------------------------------ #
    # SatelliteSource interface                                            #
    # ------------------------------------------------------------------ #

    def get_collection(self, aoi: "ee.Geometry", date_range: dict) -> "ee.ImageCollection":
        """Return a tile-cloud-filtered, band-selected ee.ImageCollection."""
        col = (
            ee.ImageCollection(self.collection_id)
            .filterBounds(aoi)
            .filterDate(date_range["start"], date_range["end"])
            .filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", self.max_tile_cloud))
            .select(_BANDS)
        )
        logger.debug(
            "Sentinel-2 collection: start=%s end=%s max_tile_cloud=%s",
            date_range["start"], date_range["end"], self.max_tile_cloud,
        )
        return col

    def apply_cloud_mask(self, image: "ee.Image") -> "ee.Image":
        """Apply SCL-based cloud/shadow mask."""
        return sentinel2_scl(image)

    def get_tile_cloud_pct(self, image: "ee.Image") -> "float | None":
        try:
            val = image.get("CLOUDY_PIXEL_PERCENTAGE").getInfo()
            return float(val) if val is not None else None
        except Exception:
            return None

    def compute_index(self, image: "ee.Image", index_name: str) -> "ee.Image":
        """Compute a spectral index using the shared optical index registry."""
        return compute_index_fn(image, index_name)

    def image_metadata(self, image: "ee.Image") -> dict:
        """Pull image metadata with a single .getInfo() call (single-image path)."""
        props = image.toDictionary(
            ["system:index", "system:time_start"] + _META_PROPERTIES
        ).getInfo()
        ts_ms = props.get("system:time_start")
        img_date = date.fromtimestamp(ts_ms / 1000).isoformat() if ts_ms else None
        return {
            "image_id":            props.get("system:index"),
            "date":                img_date,
            "tile_cloud_pct":      props.get("CLOUDY_PIXEL_PERCENTAGE"),
            "mgrs_tile":           props.get("MGRS_TILE"),
            "orbit_number":        props.get("SENSING_ORBIT_NUMBER"),
            "processing_baseline": props.get("PROCESSING_BASELINE"),
        }

    def check_aoi_coverage(self, image: "ee.Image", aoi: "ee.Geometry") -> bool:
        try:
            return bool(image.geometry().contains(aoi, maxError=10).getInfo())
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    # Extensibility hooks (override base defaults)                         #
    # ------------------------------------------------------------------ #

    def aoi_quality_fn(self):
        """Return SCL-based cloud percentage function (default for optical)."""
        return scl_quality_fn

    def gee_metadata_properties(self) -> list:
        """S2-specific properties to include in batch metadata fetch."""
        return _META_PROPERTIES

    def parse_metadata_row(self, raw: dict, i: int) -> dict:
        """Parse one row from batch aggregate_array result."""
        row = super().parse_metadata_row(raw, i)   # base: image_id, date, aoi_cloud_pct, aoi_covered
        row["tile_cloud_pct"]      = raw["CLOUDY_PIXEL_PERCENTAGE"][i]
        row["mgrs_tile"]           = raw["MGRS_TILE"][i]
        row["orbit_number"]        = raw["SENSING_ORBIT_NUMBER"][i]
        row["processing_baseline"] = raw["PROCESSING_BASELINE"][i]
        return row

    # ------------------------------------------------------------------ #
    # Per-index export resolution                                          #
    # ------------------------------------------------------------------ #

    def export_scale(self, index_name: str) -> int:
        return INDEX_SCALE.get(index_name, DEFAULT_SCALE)
