"""Landsat 8/9 source — stub (not yet implemented).

When implemented, this will merge:
  - LANDSAT/LC08/C02/T1_L2 (Landsat 8, from 2013)
  - LANDSAT/LC09/C02/T1_L2 (Landsat 9, from 2021)

Key implementation notes for when this is built out:
  - Band names differ from Sentinel-2:
      SR_B2=Blue, SR_B3=Green, SR_B4=Red, SR_B5=NIR, SR_B6=SWIR1
  - Collection 2 L2 requires scale factors:
      surface_reflectance = DN * 0.0000275 + (-0.2)
  - Cloud mask via QA_PIXEL bitmask — bits 3 (cloud shadow) and 4 (cloud)
  - 30 m resolution vs Sentinel-2's 10 m — relevant for small AOIs
  - Value: extends pre-intervention baseline to 2013; cross-checks S2 values
  - For archive back to 1984, add LANDSAT/LT05 (Landsat 5) and
    LANDSAT/LE07 (Landsat 7) collections separately

To enable: set sources.landsat.enabled: true in the config and implement
all abstract methods below.
"""

from satme.sources.base import SatelliteSource


class LandsatSource(SatelliteSource):
    source_name = "landsat"
    collection_id = ""   # will be set to merged L8+L9 collection
    available_bands = ["SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6"]
    archive_start = "2013-02-11"   # Landsat 8 launch date

    def __init__(self, src_cfg: dict):
        self.cfg = src_cfg

    def get_collection(self, aoi, date_range):
        raise NotImplementedError(
            "Landsat source is not yet implemented.  "
            "See satme/sources/landsat.py for implementation notes."
        )

    def apply_cloud_mask(self, image):
        raise NotImplementedError("Landsat source is not yet implemented.")

    def get_tile_cloud_pct(self, image):
        raise NotImplementedError("Landsat source is not yet implemented.")

    def compute_index(self, image, index_name):
        raise NotImplementedError("Landsat source is not yet implemented.")

    def image_metadata(self, image):
        raise NotImplementedError("Landsat source is not yet implemented.")

    def check_aoi_coverage(self, image, aoi):
        raise NotImplementedError("Landsat source is not yet implemented.")
