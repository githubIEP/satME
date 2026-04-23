"""Planet API source — interface-complete stub.

Access model
------------
Planet is a COMMERCIAL API — entirely separate from GEE.
Requires:
  1. An active Planet subscription or education license
  2. A Planet API key (set as the PLANET_API_KEY environment variable)
  3. The Planet Python SDK: pip install planet

Product options
---------------
  PSScene       : PlanetScope, 3 m resolution, daily revisit
  SkySatCollect : SkySat, 50 cm resolution, tasking required

Workflow (when implemented)
---------------------------
  1. Search the Planet Data API for scenes matching AOI + date + cloud filter
  2. Create an order via the Planet Orders API
  3. Poll until order status == "success"
  4. Download the GeoTIFF bundle to data/raw/
  5. Compute NDVI (and other indices) locally using rasterio + numpy

Value for this project
----------------------
  3 m resolution is the only freely accessible path to resolving the dam
  wall itself and fine-scale plot-level vegetation change that Sentinel-2's
  10 m cannot achieve.

To activate
-----------
  1. Obtain a Planet license / API key
  2. pip install planet
  3. Set PLANET_API_KEY environment variable
  4. Set sources.planet.enabled: true in the config
  5. Implement all abstract methods below, following the workflow above
"""

import os
from satme.sources.base import SatelliteSource

_SETUP_INSTRUCTIONS = (
    "Planet source requires:\n"
    "  1. A Planet API key in the PLANET_API_KEY environment variable\n"
    "  2. The planet SDK installed: pip install planet\n"
    "  3. sources.planet.enabled: true in the config\n"
    "See https://developers.planet.com/docs/apis/ for API documentation."
)


class PlanetSource(SatelliteSource):
    source_name = "planet"
    collection_id = ""   # Planet uses its own API, not GEE collection IDs
    available_bands = ["B", "G", "R", "N"]   # Blue, Green, Red, Near-IR (PSScene)
    archive_start = "2016-01-01"   # PlanetScope global coverage from ~2016

    def __init__(self, src_cfg: dict):
        self.cfg = src_cfg
        self.api_key = os.environ.get("PLANET_API_KEY")

    def _check_available(self):
        if not self.api_key:
            raise NotImplementedError(
                "Planet API key not found (PLANET_API_KEY environment variable).\n"
                + _SETUP_INSTRUCTIONS
            )
        try:
            import planet  # noqa: F401
        except ImportError:
            raise NotImplementedError(
                "Planet SDK not installed.  Run: pip install planet\n"
                + _SETUP_INSTRUCTIONS
            )

    def get_collection(self, aoi, date_range):
        self._check_available()
        raise NotImplementedError(
            "Planet source is not yet implemented.  "
            "See satme/sources/planet.py for implementation notes.\n"
            + _SETUP_INSTRUCTIONS
        )

    def apply_cloud_mask(self, image):
        self._check_available()
        raise NotImplementedError("Planet source is not yet implemented.")

    def get_tile_cloud_pct(self, image):
        self._check_available()
        raise NotImplementedError("Planet source is not yet implemented.")

    def compute_index(self, image, index_name):
        self._check_available()
        raise NotImplementedError("Planet source is not yet implemented.")

    def image_metadata(self, image):
        self._check_available()
        raise NotImplementedError("Planet source is not yet implemented.")

    def check_aoi_coverage(self, image, aoi):
        self._check_available()
        raise NotImplementedError("Planet source is not yet implemented.")

    def estimate_scene_count(self, aoi_wkt: str, date_range: dict) -> dict:
        """Return a cost estimate dict without actually ordering data.

        This is the only method that doesn't require a full implementation —
        it can query the Planet search API to count available scenes
        and multiply by the per-scene cost.

        Returns
        -------
        dict with keys: scene_count, estimated_usd, note
        """
        return {
            "scene_count": None,
            "estimated_usd": None,
            "note": (
                "Planet cost estimation requires a valid API key and the planet SDK.  "
                "Typical PlanetScope pricing: contact Planet for current rates.  "
                "Education licenses may have zero marginal cost per scene."
            ),
        }
