"""VIIRS Nighttime Lights source — fully implemented.

Satellite  : Suomi NPP (launched 2011) and NOAA-20 (launched 2017)
Instrument : VIIRS Day/Night Band (DNB) — panchromatic 0.5 µm

GEE collections
---------------
Monthly composites (default, recommended):
  NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG
  - Stray-light corrected, cloud-free composite
  - Band avg_rad: mean radiance in nW/cm²/sr
  - Band cf_cvg:  number of cloud-free nights in the composite
  - April 2012 – present, ~500 m resolution

Annual composites (optional, lower noise):
  NOAA/VIIRS/DNB/ANNUAL_V1
  - Band: average (nW/cm²/sr), cf_cvg
  - 2015 – present

Signal
------
There are no multi-band spectral indices for VIIRS.  The avg_rad band
IS the output signal.  The pipeline computes mean, std, min, max, and
percentiles of avg_rad over the AOI — the same reduction applied to
NDVI or NDMI for optical sources.

Specify ``indices: ["avg_rad"]`` in the config to activate this output.

AOI quality
-----------
Monthly composites are already cloud-free by construction.  Quality is
assessed via the cf_cvg band: pixels with fewer than min_cf_cvg cloud-free
nights are masked before reduction.  The aoi_quality_fn returns the
fraction of AOI pixels that fall below this threshold (0 = all good,
100 = no usable pixels), consistent with the 0–100 score used by other
sources.

Interpreting avg_rad (nW/cm²/sr)
---------------------------------
< 0.5    Rural / uninhabited
0.5–2    Villages / small towns
2–10     Suburban / small city
10–50    Urban cores / commercial
> 50     Dense city / port / airport / gas flare

A sharp drop in avg_rad after reference_date → displacement, power outage,
or economic collapse.  A recovery toward pre-event values → reconstruction.

Temporal note
-------------
VIIRS produces monthly composites.  One output row per month — there is
no concept of a per-overpass image.  The date column in stats.csv will
show the first day of each composite month.
"""

import logging
from datetime import date

import ee

from satme.sources.base import SatelliteSource
from satme.image_filter import viirs_quality_fn

logger = logging.getLogger(__name__)

_MONTHLY_COLLECTION = "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG"
_ANNUAL_COLLECTION  = "NOAA/VIIRS/DNB/ANNUAL_V1"
_RADIANCE_BAND      = "avg_rad"
_COVERAGE_BAND      = "cf_cvg"

_VALID_SIGNALS = {"avg_rad"}

# GEE properties available on VIIRS monthly images
_META_PROPERTIES: list = []   # VIIRS composites have no useful extra properties


class VIIRSSource(SatelliteSource):
    source_name    = "viirs"
    collection_id  = _MONTHLY_COLLECTION
    available_bands = [_RADIANCE_BAND, _COVERAGE_BAND]
    archive_start  = "2012-04-01"
    default_scale  = 500   # native VIIRS DNB resolution (~0.004167°)

    def __init__(self, src_cfg: dict):
        self.cfg        = src_cfg
        self.min_cf_cvg = int(src_cfg.get("min_cf_cvg", 1))

        # Allow switching to annual composites via config
        collection_id = src_cfg.get("collection", _MONTHLY_COLLECTION)
        if "ANNUAL" in collection_id.upper():
            self.collection_id = _ANNUAL_COLLECTION
            # Annual composites use "average" not "avg_rad"
            self._radiance_band = "average"
        else:
            self.collection_id  = _MONTHLY_COLLECTION
            self._radiance_band = _RADIANCE_BAND

        # Validate requested signals
        requested = src_cfg.get("indices", ["avg_rad"])
        unknown = [s for s in requested if s not in _VALID_SIGNALS and s != "average"]
        if unknown:
            raise ValueError(
                f"VIIRS unknown signals: {unknown}. "
                f"Available: {sorted(_VALID_SIGNALS)}"
            )

    # ------------------------------------------------------------------ #
    # SatelliteSource interface                                            #
    # ------------------------------------------------------------------ #

    def get_collection(self, aoi: "ee.Geometry", date_range: dict) -> "ee.ImageCollection":
        """Filter VIIRS monthly composite collection by AOI and date range."""
        col = (
            ee.ImageCollection(self.collection_id)
            .filterBounds(aoi)
            .filterDate(date_range["start"], date_range["end"])
            .select([self._radiance_band, _COVERAGE_BAND])
        )
        logger.debug(
            "VIIRS collection: %s start=%s end=%s",
            self.collection_id, date_range["start"], date_range["end"],
        )
        return col

    def apply_cloud_mask(self, image: "ee.Image") -> "ee.Image":
        """Mask pixels where cf_cvg < min_cf_cvg (insufficient cloud-free nights).

        The monthly composite is already cloud-free; this masks only pixels
        where too few cloud-free nights contributed to the composite, making
        the value unreliable.
        """
        valid = image.select(_COVERAGE_BAND).gte(self.min_cf_cvg)
        return image.updateMask(valid)

    def get_tile_cloud_pct(self, image: "ee.Image") -> None:
        # Not applicable — VIIRS monthly composites have no tile cloud property
        return None

    def compute_index(self, image: "ee.Image", index_name: str) -> "ee.Image":
        """Return the radiance band renamed to the requested signal name.

        VIIRS is a single-band product.  The "index" IS the avg_rad band.
        This method just selects and renames it so the pipeline can treat
        VIIRS the same way as any other source (stacking bands, reduceRegion).
        """
        if index_name in (_RADIANCE_BAND, "average", "avg_rad"):
            return image.select(self._radiance_band).rename(index_name)
        raise KeyError(
            f"VIIRS only supports '{self._radiance_band}' as a signal, not '{index_name}'."
        )

    def image_metadata(self, image: "ee.Image") -> dict:
        """Pull VIIRS image metadata in a single .getInfo() call."""
        props = image.toDictionary(
            ["system:index", "system:time_start"]
        ).getInfo()
        ts_ms = props.get("system:time_start")
        img_date = date.fromtimestamp(ts_ms / 1000).isoformat() if ts_ms else None
        return {
            "image_id":       props.get("system:index"),
            "date":           img_date,
            "tile_cloud_pct": None,
        }

    def check_aoi_coverage(self, image: "ee.Image", aoi: "ee.Geometry") -> bool:
        # VIIRS is global — always covers the AOI
        return True

    # ------------------------------------------------------------------ #
    # Extensibility hooks                                                  #
    # ------------------------------------------------------------------ #

    def aoi_quality_fn(self):
        """cf_cvg-based quality function — fraction of low-coverage pixels."""
        return viirs_quality_fn(self.min_cf_cvg)

    def gee_metadata_properties(self) -> list:
        return _META_PROPERTIES   # empty — VIIRS has no useful extra properties

    def parse_metadata_row(self, raw: dict, i: int) -> dict:
        """Parse one row from the batch aggregate_array result."""
        row = super().parse_metadata_row(raw, i)
        row["tile_cloud_pct"] = None   # not applicable
        # No extra S2-style properties for VIIRS
        return row
