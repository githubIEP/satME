"""Abstract base class that every satellite source must implement.

Any class that inherits from SatelliteSource and implements all abstract
methods can be dropped into the pipeline without further changes.

Extensibility hooks (non-abstract, override as needed)
-------------------------------------------------------
aoi_quality_fn()
    Returns a server-side function (image, aoi, scale) -> ee.Number (0–100).
    The pipeline passes this into prefilter_by_aoi_cloud so cloud filtering
    is source-specific.  Default: SCL-based cloud % (optical sensors).
    SAR sources override to return 0 always (radar sees through cloud).
    VIIRS overrides to use cf_cvg (cloud-free coverage count).

gee_metadata_properties()
    List of GEE image property names to fetch in the batch metadata call.
    The pipeline always fetches system:index, system:time_start, aoi_cloud_pct,
    aoi_covered.  Each source adds its own extra properties here.

parse_metadata_row(raw, i)
    Converts one row of the batch aggregate_array result into a plain dict
    with the standard keys expected by the pipeline (image_id, date,
    tile_cloud_pct, aoi_cloud_pct, aoi_covered, …).

default_scale
    Pixel scale in metres used for reduceRegion in stats computation.
    10 m for most sources; 20 m for Sentinel-2 (NDMI uses 20 m bands);
    500 m for VIIRS.
"""

from abc import ABC, abstractmethod
from datetime import date as _date


class SatelliteSource(ABC):
    """Interface contract for all satellite data sources."""

    # ------------------------------------------------------------------ #
    # Subclasses must set these class-level attributes                     #
    # ------------------------------------------------------------------ #

    #: Human-readable name used in logs and CSV `source` column
    source_name: str = ""

    #: GEE collection ID (or STAC collection ID for MPC backend)
    collection_id: str = ""

    #: Bands available in this source — used by the index validator
    available_bands: list = []

    #: Earliest date the collection starts (ISO string)
    archive_start: str = ""

    #: Pixel scale in metres for reduceRegion (override per source)
    default_scale: int = 10

    # ------------------------------------------------------------------ #
    # Abstract interface — must be implemented by every source             #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def get_collection(self, aoi: object, date_range: dict) -> object:
        """Return a filtered collection for the given AOI and date range.

        Parameters
        ----------
        aoi:
            Backend-native geometry (ee.Geometry for GEE).
        date_range:
            Dict with ``start`` and ``end`` keys (ISO date strings).

        Returns
        -------
        ee.ImageCollection
        """

    @abstractmethod
    def apply_cloud_mask(self, image: object) -> object:
        """Return a pre-processed copy of the image ready for index computation.

        For optical sensors: applies cloud/shadow mask (e.g. SCL).
        For SAR: applies speckle filter (no cloud masking).
        For pre-composited products (VIIRS): applies data-quality mask.

        Returns
        -------
        ee.Image
            Same image with invalid pixels masked and/or noise reduced.
        """

    @abstractmethod
    def get_tile_cloud_pct(self, image: object) -> "float | None":
        """Extract tile-level cloud percentage from image metadata.

        Returns None if not applicable (SAR, VIIRS monthly composites).
        """

    @abstractmethod
    def compute_index(self, image: object, index_name: str) -> object:
        """Compute one signal band and return a single-band ee.Image.

        For optical: spectral index (NDVI, NDWI, …).
        For SAR: backscatter ratio (RVI, VH_VV, …).
        For VIIRS: direct band selection (avg_rad).

        The returned image must be named after index_name.
        """

    @abstractmethod
    def image_metadata(self, image: object) -> dict:
        """Extract image-level metadata as a plain dict (one getInfo call).

        Must include at minimum: date, image_id.
        Used only for single-image inspection — the pipeline uses
        the batch path (parse_metadata_row) instead.
        """

    @abstractmethod
    def check_aoi_coverage(self, image: object, aoi: object) -> bool:
        """Return True if the image footprint fully covers the AOI."""

    # ------------------------------------------------------------------ #
    # Extensibility hooks — override in subclasses as needed               #
    # ------------------------------------------------------------------ #

    def aoi_quality_fn(self):
        """Return a server-side quality function for prefilter_by_aoi_cloud.

        The returned callable has signature:
            (image: ee.Image, aoi: ee.Geometry, scale: int) -> ee.Number

        The number represents a "badness" score 0–100:
            0   = image is perfect (no cloud / full coverage)
            100 = image is completely unusable

        The pipeline filters out images where this score exceeds
        max_aoi_cloud_pct (or max_aoi_quality_pct for non-optical sources).

        Default
        -------
        SCL-based cloud percentage — correct for Sentinel-2 L2A.
        SAR sources return a constant 0 (radar sees through cloud).
        VIIRS overrides to use cf_cvg (cloud-free coverage count).
        """
        from satme.image_filter import scl_quality_fn
        return scl_quality_fn

    def gee_metadata_properties(self) -> list:
        """Source-specific GEE properties to include in batch metadata fetch.

        The pipeline always fetches:
            system:index, system:time_start, aoi_cloud_pct, aoi_covered

        Return extra properties here (e.g. CLOUDY_PIXEL_PERCENTAGE,
        MGRS_TILE for Sentinel-2; ORBIT_PROPERTIES_PASS for Sentinel-1).
        """
        return []

    def parse_metadata_row(self, raw: dict, i: int) -> dict:
        """Parse one row from the batch aggregate_array result.

        Parameters
        ----------
        raw:
            Dict of {property: [value, …]} returned by ee.Dictionary.getInfo().
        i:
            Index into each list (one per image).

        Returns
        -------
        dict
            Must contain at minimum:
                image_id, date, tile_cloud_pct, aoi_cloud_pct, aoi_covered

        The default implementation handles the four always-fetched properties.
        Subclasses should call super().parse_metadata_row(raw, i) and extend
        the result with their source-specific fields.
        """
        ts_ms = raw["system:time_start"][i]
        img_date = (
            _date.fromtimestamp(ts_ms / 1000).isoformat() if ts_ms is not None else None
        )
        aoi_cloud = raw["aoi_cloud_pct"][i]
        return {
            "image_id":      raw["system:index"][i],
            "date":          img_date,
            "tile_cloud_pct": None,   # subclasses override with source property
            "aoi_cloud_pct": round(float(aoi_cloud), 2) if aoi_cloud is not None else None,
            "aoi_covered":   bool(raw["aoi_covered"][i]) if raw["aoi_covered"][i] is not None else False,
        }

    def export_scale(self, index_name: str) -> int:
        """Return the export resolution in metres for a given index/signal.

        Default: source's default_scale.  Override in sources with
        per-index resolution variation (e.g. Sentinel-2 NDMI at 20 m).
        """
        return self.default_scale
