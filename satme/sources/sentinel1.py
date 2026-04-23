"""Sentinel-1 SAR source — fully implemented.

Collection : COPERNICUS/S1_GRD
Archive    : April 2014 onwards
Revisit    : ~6 days
Resolution : 10 m (IW mode)

Key concepts
------------
SAR (Synthetic Aperture Radar) measures microwave energy scattered back
to the satellite — not reflected sunlight.  It operates day and night and
penetrates cloud cover completely.  Images are radar backscatter in dB
(decibels), a log scale where higher values = more energy returned.

Bands
-----
VV  Vertical-Vertical polarisation — sensitive to surface roughness,
    soil moisture, and open water.  Open water appears very dark (VV < -20 dB)
    because the flat surface reflects energy away from the sensor.

VH  Vertical-Horizontal (cross-polarised) — sensitive to volume scattering.
    Vegetation canopies depolarise the signal, so dense vegetation shows
    high VH.  Bare soil or water shows low VH.

Speckle filtering
-----------------
SAR images have speckle: random salt-and-pepper noise from coherent
backscatter interference.  The pipeline applies a simple Lee boxcar filter
(focal mean over a 3×3 window) before computing indices.  This is applied
inside apply_cloud_mask() — the name is inherited from the base class but
for SAR it means "apply pre-processing" rather than cloud masking.

Index computation — linear vs dB
---------------------------------
Backscatter values in GEE are stored in dB (log scale).  Ratio-based indices
(RVI, VH_VV) require linear-scale values to be physically meaningful.

Conversion:  linear = 10 ^ (dB / 10)

RVI is computed in linear scale; VH_VV is a simple dB subtraction (which
is equivalent to a linear ratio in log space: log(VH) - log(VV) = log(VH/VV)).

Available indices (specify in config under sources.sentinel1.indices)
---------------------------------------------------------------------
RVI    Radar Vegetation Index = 4*VH_lin / (VV_lin + VH_lin)
       Range 0–1; ~0 = bare soil/water, ~1 = dense vegetation.
       Most widely validated SAR vegetation index.

VH_VV  Cross-pol ratio in dB = VH - VV
       More negative = drier / less vegetated.
       Equivalent to log(VH/VV) in linear space.

DPSVI  Dual-Pol SAR Vegetation Index = VV_lin * (VV_lin + VH_lin) / (4 * VH_lin)
       Inverse of RVI; lower = more vegetation.

AOI quality
-----------
SAR images are unaffected by cloud cover.  The aoi_quality_fn returns 0
for every image (100% usable).  aoi_covered is still checked — partial
footprints are flagged PARTIAL_AOI_COVERAGE.

Orbit direction
---------------
Always filter to a single orbit direction (ASCENDING or DESCENDING) to
ensure consistent viewing geometry.  Mixing directions produces apparent
backscatter changes that are purely geometric artefacts.
"""

import logging
from datetime import date

import ee

from satme.sources.base import SatelliteSource
from satme.image_filter import sar_quality_fn

logger = logging.getLogger(__name__)

_COLLECTION = "COPERNICUS/S1_GRD"

# GEE properties available on S1 images
_META_PROPERTIES = [
    "orbitProperties_pass",      # "ASCENDING" or "DESCENDING"
    "relativeOrbitNumber_start", # relative orbit number
    "instrumentMode",            # "IW" for land
    "resolution_meters",         # nominal pixel spacing
]

# Valid index names for this source
_VALID_INDICES = {"RVI", "VH_VV", "DPSVI"}


class Sentinel1Source(SatelliteSource):
    source_name    = "sentinel1"
    collection_id  = _COLLECTION
    available_bands = ["VV", "VH"]
    archive_start  = "2014-04-01"
    default_scale  = 10

    def __init__(self, src_cfg: dict):
        self.cfg             = src_cfg
        self.orbit_direction = src_cfg.get("orbit_direction", "ASCENDING").upper()
        self.instrument_mode = src_cfg.get("instrument_mode", "IW")
        self.polarizations   = src_cfg.get("polarizations", ["VV", "VH"])
        self.speckle_filter  = src_cfg.get("speckle_filter", "lee")   # "lee" or None

        requested = src_cfg.get("indices", ["RVI"])
        unknown = [i for i in requested if i not in _VALID_INDICES]
        if unknown:
            raise ValueError(
                f"Sentinel-1 unknown indices: {unknown}. "
                f"Available: {sorted(_VALID_INDICES)}"
            )

    # ------------------------------------------------------------------ #
    # SatelliteSource interface                                            #
    # ------------------------------------------------------------------ #

    def get_collection(self, aoi: "ee.Geometry", date_range: dict) -> "ee.ImageCollection":
        """Filter S1 GRD collection by AOI, date, orbit direction, and mode."""
        col = (
            ee.ImageCollection(_COLLECTION)
            .filterBounds(aoi)
            .filterDate(date_range["start"], date_range["end"])
            .filter(ee.Filter.eq("instrumentMode", self.instrument_mode))
            .filter(ee.Filter.eq("orbitProperties_pass", self.orbit_direction))
        )

        # Filter to images that contain all requested polarisations
        for pol in self.polarizations:
            col = col.filter(
                ee.Filter.listContains("transmitterReceiverPolarisation", pol)
            )

        col = col.select(self.polarizations)

        logger.debug(
            "Sentinel-1 collection: start=%s end=%s orbit=%s mode=%s",
            date_range["start"], date_range["end"],
            self.orbit_direction, self.instrument_mode,
        )
        return col

    def apply_cloud_mask(self, image: "ee.Image") -> "ee.Image":
        """Apply speckle filter (SAR pre-processing — no cloud masking needed).

        Uses a focal mean (Lee-style boxcar) over a 3×3 pixel window to
        reduce speckle noise before index computation.  The filtered bands
        replace the original in the returned image.

        If speckle_filter is None (disabled in config), returns the image
        unchanged.
        """
        if not self.speckle_filter:
            return image

        filtered_bands = []
        for pol in self.polarizations:
            band = image.select(pol)
            # Focal mean: simple Lee boxcar approximation
            filtered = band.focal_mean(
                radius=1,
                kernelType="square",
                units="pixels",
                iterations=1,
            )
            filtered_bands.append(filtered.rename(pol))

        return image.addBands(filtered_bands, overwrite=True)

    def get_tile_cloud_pct(self, image: "ee.Image") -> None:
        # Not applicable for SAR
        return None

    def compute_index(self, image: "ee.Image", index_name: str) -> "ee.Image":
        """Compute a SAR-based index.

        All ratio indices that mix VV and VH are computed in linear scale
        (converted from dB first).  VH_VV uses dB arithmetic directly.
        """
        vv_db = image.select("VV")
        vh_db = image.select("VH")

        # Convert dB → linear for ratio-based indices
        # linear = 10 ^ (dB / 10)
        vv_lin = ee.Image(10).pow(vv_db.divide(10))
        vh_lin = ee.Image(10).pow(vh_db.divide(10))

        if index_name == "RVI":
            # RVI = 4*VH_lin / (VV_lin + VH_lin)
            # Range: 0 (bare/water) to 1 (dense vegetation)
            return (
                vh_lin.multiply(4)
                .divide(vv_lin.add(vh_lin))
                .rename("RVI")
            )

        if index_name == "VH_VV":
            # VH_VV = VH - VV  (dB subtraction ≡ log-scale ratio)
            # More negative → drier / less vegetated
            return vh_db.subtract(vv_db).rename("VH_VV")

        if index_name == "DPSVI":
            # DPSVI = VV_lin * (VV_lin + VH_lin) / (4 * VH_lin)
            # Lower values → more vegetation (inverse of RVI)
            return (
                vv_lin.multiply(vv_lin.add(vh_lin))
                .divide(vh_lin.multiply(4))
                .rename("DPSVI")
            )

        raise KeyError(
            f"Unknown Sentinel-1 index '{index_name}'. "
            f"Available: {sorted(_VALID_INDICES)}"
        )

    def image_metadata(self, image: "ee.Image") -> dict:
        """Pull S1 image metadata in a single .getInfo() call."""
        props = image.toDictionary(
            ["system:index", "system:time_start"] + _META_PROPERTIES
        ).getInfo()
        ts_ms = props.get("system:time_start")
        img_date = date.fromtimestamp(ts_ms / 1000).isoformat() if ts_ms else None
        return {
            "image_id":          props.get("system:index"),
            "date":              img_date,
            "tile_cloud_pct":    None,
            "orbit_direction":   props.get("orbitProperties_pass"),
            "orbit_number":      props.get("relativeOrbitNumber_start"),
            "instrument_mode":   props.get("instrumentMode"),
        }

    def check_aoi_coverage(self, image: "ee.Image", aoi: "ee.Geometry") -> bool:
        try:
            return bool(image.geometry().contains(aoi, maxError=10).getInfo())
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    # Extensibility hooks                                                  #
    # ------------------------------------------------------------------ #

    def aoi_quality_fn(self):
        """SAR is unaffected by cloud — always returns quality score 0."""
        return sar_quality_fn

    def gee_metadata_properties(self) -> list:
        return _META_PROPERTIES

    def parse_metadata_row(self, raw: dict, i: int) -> dict:
        """Parse one row from the batch aggregate_array result."""
        row = super().parse_metadata_row(raw, i)
        row["tile_cloud_pct"]  = None
        row["orbit_direction"] = raw["orbitProperties_pass"][i]
        row["orbit_number"]    = raw["relativeOrbitNumber_start"][i]
        row["instrument_mode"] = raw["instrumentMode"][i]
        return row
