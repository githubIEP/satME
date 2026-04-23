"""SatME pipeline entry point.

Usage
-----
    python main.py --config config/makaveti_example.yaml

Options
-------
    --config PATH        Path to a YAML config file (required)
    --yes                Skip the confirmation prompt and run immediately
    --log-level LEVEL    Logging level: DEBUG, INFO, WARNING, ERROR (default: INFO)
    --gee-project ID     Override the GEE project ID (also: EARTHENGINE_PROJECT env var)
    --dry-run            Print the pre-flight estimate and exit without processing
    --no-ssl-verify      Disable SSL certificate verification (use on corporate networks
                         with SSL inspection if you see UNEXPECTED_EOF_WHILE_READING)

Exit codes
----------
    0  Completed successfully
    1  Config validation error
    2  GEE authentication / connection error
    3  Run cancelled by user

SSL note
--------
Python 3.12+ is stricter about TLS EOF handling.  On institutional networks with
SSL inspection (proxy re-signs certificates), this commonly causes:
  SSLEOFError: [SSL: UNEXPECTED_EOF_WHILE_READING]
The OP_IGNORE_UNEXPECTED_EOF patch below fixes this transparently by restoring
the pre-3.12 lenient behaviour for all outbound SSL connections.
"""

# ── SSL patch — must run before any network imports ──────────────────────────
# Root cause: Python 3.12+ + TLS 1.3 raises SSLEOFError / SSLZeroReturnError
# when the remote peer closes the TLS session before urllib3 finishes reading.
# Common on institutional networks whose SSL-inspection proxy (Zscaler, etc.)
# performs a clean TLS shutdown that Python 3.12+ treats as a protocol error.
#
# Why the previous single-module patch was insufficient (urllib3 ≥ 2.0):
#   urllib3.connection does `from urllib3.util.ssl_ import create_urllib3_context`
#   at import time, creating a local reference bound to the ORIGINAL function.
#   Replacing the attribute on urllib3.util.ssl_ afterwards has no effect on
#   that already-bound local reference.
#
# Fix: patch the source module attribute AND walk sys.modules to replace every
# already-cached reference in any module that imported the function by name.
import ssl as _ssl

if hasattr(_ssl, "OP_IGNORE_UNEXPECTED_EOF"):
    _EOF_FLAG = _ssl.OP_IGNORE_UNEXPECTED_EOF

    # Patch 1: urllib3 — covers both current and future imports
    try:
        import urllib3.util.ssl_ as _u3ssl
        _orig_u3ctx = _u3ssl.create_urllib3_context

        def _patched_u3ctx(*args, **kwargs):
            ctx = _orig_u3ctx(*args, **kwargs)
            ctx.options |= _EOF_FLAG
            return ctx

        # Replace in the source module (future imports will get the patched version)
        _u3ssl.create_urllib3_context = _patched_u3ctx

        # Also replace in every already-loaded module that cached the original by name
        # (urllib3.connection, urllib3.connectionpool, etc. import it at package load time)
        import sys as _sys
        for _mod in list(_sys.modules.values()):
            if (
                _mod is not None
                and getattr(_mod, "create_urllib3_context", None) is _orig_u3ctx
            ):
                _mod.create_urllib3_context = _patched_u3ctx

    except Exception:
        pass

    # Patch 2: ssl.create_default_context — covers httplib2 and any library that
    # accesses the function via the ssl module attribute (not a local binding).
    _orig_cdc = _ssl.create_default_context

    def _patched_cdc(*args, **kwargs):
        ctx = _orig_cdc(*args, **kwargs)
        ctx.options |= _EOF_FLAG
        return ctx

    _ssl.create_default_context = _patched_cdc

# Patch 3: ssl.SSLSocket.read — treat SSLZeroReturnError (code 6) as clean EOF.
#
# Python 3.12+ raises SSLZeroReturnError when the remote peer sends a TLS
# close_notify alert (SSL_ERROR_ZERO_RETURN).  Before 3.12 this returned b""
# (empty bytes), signalling EOF to the caller.  Corporate SSL-inspection proxies
# (Zscaler, Blue Coat, etc.) routinely send close_notify after each response,
# so on Python 3.12+ every response on such networks raises this error.
#
# OP_IGNORE_UNEXPECTED_EOF (Patch 1 above) covers SSL_ERROR_SYSCALL + EOF
# (error code 8) but does NOT cover SSL_ERROR_ZERO_RETURN (code 6).
# Patching ssl.SSLSocket.read() at the Python level restores the pre-3.12
# behaviour for code 6 without disabling certificate verification.
#
# ssl.SSLSocket is defined in ssl.py (not a C extension), so it is patchable.
try:
    _orig_ssl_read = _ssl.SSLSocket.read

    def _patched_ssl_read(self, len=1024, buffer=None):
        try:
            if buffer is not None:
                return _orig_ssl_read(self, len, buffer)
            return _orig_ssl_read(self, len)
        except _ssl.SSLZeroReturnError:
            # Return empty bytes / zero-length write to signal clean EOF
            return 0 if buffer is not None else b""

    _ssl.SSLSocket.read = _patched_ssl_read
except Exception:
    pass
# ─────────────────────────────────────────────────────────────────────────────

import argparse
import logging
import sys
from pathlib import Path

import yaml


# ─────────────────────────────────────────────────────────────────────────────
# Config validation
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = [
    ("run", "name"),
    ("run", "reference_date"),
    ("aoi", "mode"),
    ("date_range", "start"),
    ("date_range", "end"),
    ("sources",),
    ("output",),
]


def _validate_config(cfg: dict) -> list[str]:
    """Return a list of validation errors (empty = valid)."""
    errors = []

    for field_path in REQUIRED_FIELDS:
        obj = cfg
        for key in field_path:
            if not isinstance(obj, dict) or key not in obj:
                errors.append(f"Missing required field: {' > '.join(field_path)}")
                break
            obj = obj[key]

    # AOI mode-specific validation
    aoi = cfg.get("aoi", {})
    mode = aoi.get("mode")
    if mode == "point_radius":
        center = aoi.get("center", {})
        if "lat" not in center or "lon" not in center:
            errors.append("aoi.center must have lat and lon for point_radius mode")
        if "radius_m" not in aoi:
            errors.append("aoi.radius_m is required for point_radius mode")
    elif mode == "polygon":
        if "coordinates" not in aoi or not aoi["coordinates"]:
            errors.append("aoi.coordinates is required for polygon mode")
    elif mode is not None:
        errors.append(f"Unknown aoi.mode '{mode}' — use 'point_radius' or 'polygon'")

    # Date range validation
    dr = cfg.get("date_range", {})
    try:
        from datetime import date
        start = date.fromisoformat(dr.get("start", ""))
        end   = date.fromisoformat(dr.get("end", ""))
        if end <= start:
            errors.append("date_range.end must be after date_range.start")
    except (ValueError, TypeError):
        errors.append("date_range.start and .end must be valid ISO dates (YYYY-MM-DD)")

    # At least one source must be enabled
    sources = cfg.get("sources", {})
    if not any(v.get("enabled", False) for v in sources.values() if isinstance(v, dict)):
        errors.append("At least one source must have enabled: true")

    return errors


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="satme",
        description="SatME — satellite monitoring and evaluation data pipeline",
    )
    parser.add_argument(
        "--config", required=True, metavar="PATH",
        help="Path to a YAML config file",
    )
    parser.add_argument(
        "--yes", "-y", action="store_true",
        help="Skip the confirmation prompt and run immediately",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    parser.add_argument(
        "--gee-project", metavar="ID",
        help="Override the GEE project ID",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the pre-flight estimate and exit without processing any data",
    )
    parser.add_argument(
        "--no-ssl-verify", action="store_true",
        help=(
            "Disable SSL certificate verification — fallback for corporate networks "
            "where OP_IGNORE_UNEXPECTED_EOF alone is not sufficient"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("satme")

    # Hard disable SSL verification if requested (last-resort fallback)
    if args.no_ssl_verify:
        import urllib3
        import requests
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        _orig_request = requests.Session.request

        def _no_verify_request(self, *a, **kw):
            kw.setdefault("verify", False)
            return _orig_request(self, *a, **kw)

        requests.Session.request = _no_verify_request
        logger.warning("SSL verification disabled — use only on trusted networks")

    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
        return 1

    with open(config_path) as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(f"ERROR: Failed to parse config YAML:\n{exc}", file=sys.stderr)
            return 1

    if not isinstance(cfg, dict):
        print("ERROR: Config file must be a YAML mapping.", file=sys.stderr)
        return 1

    # Apply CLI overrides
    if args.gee_project:
        cfg.setdefault("auth", {})["gee_project"] = args.gee_project

    # Validate config
    errors = _validate_config(cfg)
    if errors:
        print("ERROR: Config validation failed:", file=sys.stderr)
        for err in errors:
            print(f"  • {err}", file=sys.stderr)
        return 1

    logger.info("Config loaded: %s", config_path)

    # Dry-run: estimate only
    if args.dry_run:
        from satme import auth, aoi as aoi_module, estimator
        try:
            gee_project = cfg.get("auth", {}).get("gee_project")
            auth.initialise(project_id=gee_project)
            auth.verify_connection()
            geometry, aoi_meta = aoi_module.build(cfg)
            offline_est = estimator.estimate(cfg, aoi_meta)
            gee_counts  = estimator._count_images_gee(cfg, geometry)
            estimator.print_estimate(offline_est, gee_counts)
            print("  Dry run complete — no data was downloaded.")
        except Exception as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        return 0

    # Full run
    from satme.pipeline import run
    try:
        out_dir = run(cfg, skip_confirm=args.yes)
    except KeyboardInterrupt:
        print("\n  Interrupted by user.", file=sys.stderr)
        return 3
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        print(f"\nERROR: Pipeline failed — {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
