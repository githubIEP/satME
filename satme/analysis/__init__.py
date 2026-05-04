"""Analysis layer — pre/post pixel statistics, corridor decay, DiD, REPORT.md.

Vendored from the standalone david/exploration/ study and parameterised so
the same code runs against any satme run config.

Phase 4 of pipeline.run() calls _runtime_config.configure(cfg, out_dir)
once before invoking aggregate / stats / visualisation, which then read
the same names that the original david package read (KML_PATHS,
DAM_CONSTRUCTION_DATE, …) from this runtime module.
"""
