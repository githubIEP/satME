"""Backend abstraction layer — not yet wired into the pipeline.

This package defines what a "backend" is: the set of services the pipeline
calls that are NOT part of the satellite source itself.

Current state
─────────────
The pipeline hard-codes the GEE backend — it calls GEE-specific helpers
(prefilter_by_aoi_cloud, map_stats_over_collection, etc.) directly.
A Backend object would wrap those into a single swappable dependency.

To migrate the pipeline to use this abstraction:
  1. Implement a concrete Backend subclass (GEEBackend, MPCBackend, etc.)
  2. Pass the backend instance into pipeline.run()
  3. Replace direct module calls with backend.filter_collection(),
     backend.compute_stats(), etc.

See backends/base.py for the interface.
See backends/mpc/ for the Microsoft Planetary Computer stub.
"""
