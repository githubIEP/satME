"""Build (or refresh) the per-AOI zarr cache from Sentinel-2 L2A.

Slow (~30–60 min cold). Skips work if a valid cache exists.

    python -m backend.exploration.scripts.step01_build_cache
    python -m backend.exploration.scripts.step01_build_cache --force
    python -m backend.exploration.scripts.step01_build_cache --treatment-only
    python -m backend.exploration.scripts.step01_build_cache --control-only
"""

from __future__ import annotations

import argparse
import shutil

from .. import cache, config, fetch


def get_or_build(name: str, lat: float, lon: float, catalog, force: bool):
    if force:
        path = cache.cache_path_for(name)
        if path.exists():
            print(f"  --force: removing {path}")
            shutil.rmtree(path)
    cached = cache.load_cached(name)
    if cached is not None:
        return cached
    return fetch.build_per_scene_cache(name, lat, lon, catalog)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--force", action="store_true",
                   help="ignore caches and refetch")
    p.add_argument("--treatment-only", action="store_true")
    p.add_argument("--control-only", action="store_true")
    args = p.parse_args()

    catalog = fetch.open_catalog()
    print(f"Cache version: {config.CACHE_VERSION}")
    print(f"Cache dir:     {config.CACHE_DIR}")

    if not args.control_only:
        print("\n=== treatment AOI ===")
        get_or_build(
            config.TREATMENT_NAME, config.TREATMENT_LAT, config.TREATMENT_LON,
            catalog, args.force,
        )
    if not args.treatment_only:
        print("\n=== control AOI ===")
        get_or_build(
            config.CONTROL_NAME, config.CONTROL_LAT, config.CONTROL_LON,
            catalog, args.force,
        )
    print("\nDone.")


if __name__ == "__main__":
    main()
