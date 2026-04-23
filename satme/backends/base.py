"""Abstract Backend interface.

A Backend encapsulates everything the pipeline needs that is NOT provided by
a SatelliteSource — i.e. the infrastructure services:

    auth          → how to authenticate
    aoi           → how to represent an area of interest
    filter        → how to filter a collection by cloud cover / coverage
    metadata      → how to pull per-image metadata in bulk
    stats         → how to compute index statistics over a collection
    download      → how to retrieve pixel data as GeoTIFFs

Why this split?
───────────────
SatelliteSource answers: "what data exists and how do I mask it?"
Backend answers:         "how do I efficiently query, filter, and reduce it?"

GEE collapses both into one paradigm (server-side lazy graphs).
MPC separates them: STAC for querying, xarray for computation.

Current pipeline call sites that map to Backend methods
───────────────────────────────────────────────────────
  auth.initialise(...)                    → Backend.authenticate()
  aoi_module.build(cfg)                   → Backend.build_aoi(cfg)
  prefilter_by_aoi_cloud(col, aoi, ...)   → Backend.filter_by_cloud(col, aoi, threshold)
  batch_image_metadata(col)               → Backend.fetch_metadata(col)
  map_stats_over_collection(col, ...)     → Backend.map_stats(col, ...)   [lazy]
  fetch_stats_batch(col, ...)             → Backend.fetch_stats(col, ...)  [triggers compute]
  downloader.download_index_geotiff(...)  → Backend.download_geotiff(...)
"""

from abc import ABC, abstractmethod


class Backend(ABC):
    """Interface contract for all compute backends."""

    # ------------------------------------------------------------------ #
    # Authentication                                                       #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def authenticate(self, cfg: dict) -> None:
        """Authenticate with the backend service.

        Parameters
        ----------
        cfg:
            The full run config dict (auth credentials live under cfg["auth"]).

        Side effects
        ------------
        Sets up any session/token state needed for subsequent calls.
        """

    @abstractmethod
    def verify_connection(self) -> dict:
        """Verify the backend is reachable and credentials are valid.

        Returns
        -------
        dict
            Backend-specific diagnostics (e.g. {"status": "ok", "catalog": "..."}).

        Raises
        ------
        RuntimeError
            If the connection or credentials are invalid.
        """

    # ------------------------------------------------------------------ #
    # AOI                                                                  #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def build_aoi(self, cfg: dict) -> tuple:
        """Build a backend-native AOI geometry from config.

        Parameters
        ----------
        cfg:
            Full run config.

        Returns
        -------
        (geometry, metadata_dict)
            geometry     : backend-native type (ee.Geometry for GEE;
                           shapely.Polygon / GeoJSON dict for MPC/STAC).
            metadata_dict: plain dict with at minimum area_km2 and wkt keys.
        """

    # ------------------------------------------------------------------ #
    # Collection filtering                                                 #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def get_raw_collection(self, source, aoi: object, date_range: dict) -> object:
        """Return a date+bounds filtered collection for a source.

        Parameters
        ----------
        source:
            A SatelliteSource instance (provides collection_id / catalog info).
        aoi:
            Backend-native AOI geometry.
        date_range:
            Dict with keys ``start`` and ``end`` (ISO date strings).

        Returns
        -------
        object
            Backend-native collection type:
              GEE → ee.ImageCollection
              MPC → list[pystac.Item]
        """

    @abstractmethod
    def filter_by_cloud(
        self,
        collection: object,
        aoi: object,
        max_aoi_cloud_pct: float,
    ) -> tuple:
        """Filter collection to images with AOI cloud % below threshold.

        Returns both the clean subset and the full annotated collection
        so the pipeline can build a complete flag report.

        Parameters
        ----------
        collection:
            Output of get_raw_collection().
        aoi:
            Backend-native AOI geometry.
        max_aoi_cloud_pct:
            Rejection threshold (0–100).

        Returns
        -------
        (clean_collection, full_collection)
            Both are backend-native collection types.
            Each image/item must have aoi_cloud_pct and aoi_covered
            attached as metadata.
        """

    # ------------------------------------------------------------------ #
    # Metadata                                                             #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def fetch_metadata(self, collection: object) -> list[dict]:
        """Pull per-image metadata for all items in one call.

        Returns
        -------
        list[dict]
            One dict per image with at minimum:
              image_id, date, tile_cloud_pct, aoi_cloud_pct, aoi_covered
        """

    # ------------------------------------------------------------------ #
    # Statistics                                                           #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def compute_stats(
        self,
        collection: object,
        index_names: list,
        aoi: object,
        stats_cfg: dict,
        cloud_mask_fn: callable,
        scale: int,
    ) -> list[dict]:
        """Compute index statistics over all images in one batch.

        This combines what GEE splits into map_stats_over_collection +
        fetch_stats_batch — some backends (MPC/xarray) cannot separate
        the lazy graph construction from the materialisation step.

        Returns
        -------
        list[dict]
            One dict per image, keyed by image_id.  Stat keys use _std
            (not _stdDev) to match the CSV column convention.
        """

    # ------------------------------------------------------------------ #
    # Rainfall (optional)                                                  #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def fetch_rainfall(
        self,
        image_dates: list,
        aoi: object,
        accumulation_days: int,
    ) -> list:
        """Fetch accumulated rainfall for each date in one batch.

        Returns
        -------
        list[float | None]
            One value per date, same order as image_dates.
            None where data is unavailable.
        """

    # ------------------------------------------------------------------ #
    # Download                                                             #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def download_geotiff(
        self,
        image: object,
        index_name: str,
        image_date: str,
        source_name: str,
        aoi: object,
        scale: int,
        output_path: "pathlib.Path",
    ) -> None:
        """Download a single-band index image as a GeoTIFF.

        Parameters
        ----------
        image:
            Backend-native image object (ee.Image for GEE; xarray.DataArray
            for MPC).
        output_path:
            Absolute path to write the .tif file.
        """
