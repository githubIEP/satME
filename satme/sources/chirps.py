"""CHIRPS daily rainfall source — fully implemented.

Collection : UCSB-CHC/CHIRPS/DAILY  (free, available on GEE)
Resolution : ~5.5 km (0.05 degree)
Coverage   : 1981 to near-present, global (50°S–50°N)

For each satellite image date, CHIRPS accumulates the daily precipitation
rasters over the preceding N days (default 30, configurable) and reduces
over the AOI to produce a single scalar value: mean rainfall in mm.

This scalar is appended as a ``chirps_30d_mm`` column in the master CSV.
Optionally, the rainfall raster itself is downloaded as a GeoTIFF.

CHIRPS is a modelled product — no cloud masking is required.
"""

import logging
from datetime import date, timedelta

import ee

from satme.sources.base import SatelliteSource

logger = logging.getLogger(__name__)

_COLLECTION = "UCSB-CHG/CHIRPS/DAILY"
_BAND = "precipitation"


class ChirpsSource(SatelliteSource):
    source_name = "chirps"
    collection_id = _COLLECTION
    available_bands = [_BAND]
    archive_start = "1981-01-01"

    def __init__(self, src_cfg: dict):
        """
        Parameters
        ----------
        src_cfg:
            The ``sources.chirps`` block from the config dict.
        """
        self.cfg = src_cfg
        self.accumulation_days = int(src_cfg.get("accumulation_days", 30))
        self.export_geotiff = src_cfg.get("export_geotiff", False)

    # ------------------------------------------------------------------ #
    # SatelliteSource interface                                            #
    # (CHIRPS has no per-image cloud masking — these are no-ops)          #
    # ------------------------------------------------------------------ #

    def get_collection(self, aoi: "ee.Geometry", date_range: dict) -> "ee.ImageCollection":
        return (
            ee.ImageCollection(_COLLECTION)
            .filterBounds(aoi)
            .filterDate(date_range["start"], date_range["end"])
            .select(_BAND)
        )

    def apply_cloud_mask(self, image: "ee.Image") -> "ee.Image":
        # CHIRPS is a modelled product — no cloud masking
        return image

    def get_tile_cloud_pct(self, image: "ee.Image") -> float | None:
        return None  # not applicable

    def compute_index(self, image: "ee.Image", index_name: str) -> "ee.Image":
        raise NotImplementedError("CHIRPS does not support spectral indices.")

    def image_metadata(self, image: "ee.Image") -> dict:
        props = image.toDictionary(["system:index", "system:time_start"]).getInfo()
        ts_ms = props.get("system:time_start")
        image_date = date.fromtimestamp(ts_ms / 1000).isoformat() if ts_ms else None
        return {"image_id": props.get("system:index"), "date": image_date}

    def check_aoi_coverage(self, image: "ee.Image", aoi: "ee.Geometry") -> bool:
        # CHIRPS is global — always covers the AOI
        return True

    # ------------------------------------------------------------------ #
    # CHIRPS-specific methods                                              #
    # ------------------------------------------------------------------ #

    def accumulate(self, image_date: date, aoi: "ee.Geometry") -> "ee.Image":
        """Return a single accumulated rainfall image for the N days before image_date.

        Parameters
        ----------
        image_date:
            The date of the satellite image to match.
        aoi:
            AOI geometry (used for filterBounds).

        Returns
        -------
        ee.Image
            Single-band image: sum of daily precipitation over accumulation_days.
            Band name: ``chirps_Nd_mm`` where N = accumulation_days.
        """
        end_date = image_date
        start_date = image_date - timedelta(days=self.accumulation_days)

        accumulated = (
            ee.ImageCollection(_COLLECTION)
            .filterBounds(aoi)
            .filterDate(start_date.isoformat(), end_date.isoformat())
            .select(_BAND)
            .sum()
            .rename(f"chirps_{self.accumulation_days}d_mm")
        )
        return accumulated

    def get_rainfall_scalar(self, image_date: date, aoi: "ee.Geometry") -> float | None:
        """Fetch the mean accumulated rainfall over the AOI as a Python float.

        This is the primary output for the stats CSV — one number per image date.

        Parameters
        ----------
        image_date:
            Sensing date of the matched satellite image.
        aoi:
            AOI geometry.

        Returns
        -------
        float | None
            Mean accumulated rainfall in mm, or None on failure.
        """
        col_name = f"chirps_{self.accumulation_days}d_mm"
        accumulated = self.accumulate(image_date, aoi)

        result = accumulated.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=aoi,
            scale=5566,  # native CHIRPS resolution (~0.05°)
            maxPixels=1e6,
        ).get(col_name)

        try:
            value = result.getInfo()
            return round(float(value), 3) if value is not None else None
        except Exception as exc:
            logger.warning("CHIRPS scalar fetch failed for %s: %s", image_date, exc)
            return None

    def get_download_image(self, image_date: date, aoi: "ee.Geometry") -> "ee.Image":
        """Return the accumulated rainfall ee.Image clipped to the AOI (for GeoTIFF export)."""
        return self.accumulate(image_date, aoi).clip(aoi)

    def batch_rainfall_scalars(
        self, image_dates: list, aoi: "ee.Geometry"
    ) -> list:
        """Fetch accumulated rainfall for all image dates in ONE .getInfo() call.

        Uses ee.List.map() server-side to compute the 30-day accumulated
        rainfall mean over the AOI for each date, then retrieves the full
        list in a single round-trip.

        Parameters
        ----------
        image_dates:
            List of datetime.date objects — one per clean image.
        aoi:
            AOI geometry.

        Returns
        -------
        list[float | None]
            Rainfall values in mm, in the same order as image_dates.
            None where the computation failed or returned no data.
        """
        n_days = self.accumulation_days
        col_name = f"chirps_{n_days}d_mm"

        # Build server-side list of ISO date strings
        date_strings = ee.List([d.isoformat() for d in image_dates])

        def _accumulate_for_date(date_str):
            end   = ee.Date(date_str)
            start = end.advance(-n_days, "day")
            accumulated = (
                ee.ImageCollection(_COLLECTION)
                .filterBounds(aoi)
                .filterDate(start, end)
                .select(_BAND)
                .sum()
                .rename(col_name)
            )
            result = accumulated.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=aoi,
                scale=5566,
                maxPixels=1e6,
                bestEffort=True,
            )
            # Return the scalar (or -9999 sentinel if null)
            val = result.get(col_name)
            return ee.Algorithms.If(
                ee.Algorithms.IsEqual(val, None),
                ee.Number(-9999),
                ee.Number(val),
            )

        # Server-side map over date list — one computation graph, one round-trip
        values = date_strings.map(_accumulate_for_date).getInfo()

        return [
            None if v == -9999 else round(float(v), 3)
            for v in values
        ]
