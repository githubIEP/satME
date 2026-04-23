"""Check connectivity, authentication, and quota for all satME data services.

Usage
-----
    python check_usage.py
    python check_usage.py --config config/makaveti_example.yaml

Each checker reports:
  OK      — service is reachable and credentials are valid
  WARN    — service responds but something is limited or optional
  FAIL    — service is unreachable or credentials are invalid

Exit codes
----------
  0  All checks passed (OK or WARN)
  1  One or more checks FAILED

Adding a new service
--------------------
Implement a function with signature:

    def check_<name>(cfg: dict) -> CheckResult:
        ...

Return a CheckResult(status, summary, details=[]).
Register it in CHECKERS at the bottom of this file.
"""

import argparse
import os
import sys
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# -----------------------------------------------------------------------------
# Result type
# -----------------------------------------------------------------------------

@dataclass
class CheckResult:
    status:  str          # "OK", "WARN", or "FAIL"
    summary: str          # one-line human-readable outcome
    details: list[str] = field(default_factory=list)   # extra lines printed below

    @property
    def ok(self) -> bool:
        return self.status in ("OK", "WARN")


# -----------------------------------------------------------------------------
# Individual service checkers
# -----------------------------------------------------------------------------

def check_gee(cfg: dict) -> CheckResult:
    """Google Earth Engine — auth, project, algorithm count."""
    auth_cfg = cfg.get("auth", {})
    project_id = (
        auth_cfg.get("gee_project")
        or os.environ.get("EARTHENGINE_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )

    if not project_id:
        return CheckResult(
            "FAIL",
            "No GEE project ID configured",
            [
                "Set auth.gee_project in your config YAML, or set the",
                "EARTHENGINE_PROJECT environment variable.",
                "Create a project at: https://code.earthengine.google.com",
            ],
        )

    try:
        import ee
    except ImportError:
        return CheckResult("FAIL", "earthengine-api not installed (pip install earthengine-api)")

    try:
        from satme.auth import initialise, verify_connection
        service_account_key = auth_cfg.get("service_account_key")
        initialise(project_id=project_id, service_account_key=service_account_key)
        info = verify_connection()
        alg_count = info.get("algorithm_count", "?")
        return CheckResult(
            "OK",
            f"Connected — project: {project_id} | {alg_count} algorithms available",
            [
                "View quota / API usage at:",
                "  https://console.cloud.google.com/apis/api/earthengine.googleapis.com/quotas",
                f"  (filter by project: {project_id})",
            ],
        )
    except EnvironmentError as exc:
        return CheckResult("FAIL", str(exc))
    except Exception as exc:
        hint = []
        if "not registered" in str(exc).lower() or "project" in str(exc).lower():
            hint = [
                "Ensure the Earth Engine API is enabled for this project:",
                "  https://console.cloud.google.com/apis/library/earthengine.googleapis.com",
            ]
        elif "credentials" in str(exc).lower() or "authenticate" in str(exc).lower():
            hint = [
                "Run: earthengine authenticate",
                "Then retry this check.",
            ]
        return CheckResult("FAIL", f"GEE connection failed: {exc}", hint)


def check_mpc(cfg: dict) -> CheckResult:
    """Microsoft Planetary Computer — anonymous SAS token endpoint."""
    _MPC_SAS_URL = "https://planetarycomputer.microsoft.com/api/sas/v1/token/sentinel-2-l2a"

    try:
        import requests
    except ImportError:
        return CheckResult("FAIL", "requests not installed (pip install requests)")

    try:
        from satme.copernicus_auth import build_session
        session = build_session(cfg)
        resp = session.get(_MPC_SAS_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        token = data.get("token", "")
        if token:
            # Token is a SAS query string — show its length as a proxy for validity
            return CheckResult(
                "OK",
                f"SAS token received ({len(token)} chars) — no account required",
                [
                    "MPC provides free anonymous access to Sentinel-2 L2A COGs.",
                    "No sign-up or API key is needed.",
                    "MPC usage info: https://planetarycomputer.microsoft.com",
                ],
            )
        else:
            return CheckResult(
                "WARN",
                "SAS endpoint responded but token field is empty",
                [f"Response: {str(data)[:200]}"],
            )
    except Exception as exc:
        return CheckResult(
            "FAIL",
            f"MPC SAS endpoint unreachable: {exc}",
            ["Check network connectivity to planetarycomputer.microsoft.com"],
        )


def check_cdse(cfg: dict) -> CheckResult:
    """Copernicus Data Space Ecosystem — catalog ping + optional auth test."""
    _CDSE_CATALOG = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$top=0&$count=true"

    try:
        import requests
    except ImportError:
        return CheckResult("FAIL", "requests not installed")

    # -- 1. Catalog ping (no auth needed) -------------------------------------
    try:
        from satme.copernicus_auth import build_session, from_cfg
        session = build_session(cfg)
        resp = session.get(_CDSE_CATALOG, timeout=15)
        catalog_ok = resp.status_code == 200
    except Exception as exc:
        return CheckResult(
            "WARN",
            f"CDSE catalog unreachable: {exc}",
            [
                "CDSE catalog search will be skipped.",
                "Pre-2019 data will still be read from Microsoft Planetary Computer COGs.",
            ],
        )

    # -- 2. Auth test (only if credentials are present) ------------------------
    auth_cfg = cfg.get("auth", {})
    username = auth_cfg.get("cdse_username") or os.environ.get("CDSE_USERNAME", "")
    password = auth_cfg.get("cdse_password") or os.environ.get("CDSE_PASSWORD", "")

    catalog_status = "catalog reachable" if catalog_ok else f"catalog HTTP {resp.status_code}"

    if not username or not password:
        return CheckResult(
            "WARN",
            f"No CDSE credentials configured ({catalog_status})",
            [
                "CDSE auth is OPTIONAL — band reads use Microsoft Planetary Computer.",
                "Credentials are only needed as a last-resort direct download fallback.",
                "To add credentials:",
                "  Option A: set CDSE_USERNAME and CDSE_PASSWORD environment variables",
                "  Option B: set auth.cdse_username / auth.cdse_password in the config",
                "  Register free at: https://dataspace.copernicus.eu",
            ],
        )

    try:
        token_mgr, _ = from_cfg(cfg)
        if token_mgr is None:
            return CheckResult(
                "WARN",
                f"Credentials found but TokenManager was not created ({catalog_status})",
            )
        token = token_mgr.get_token()
        return CheckResult(
            "OK",
            f"Authenticated as {username} ({catalog_status})",
            [
                "Token obtained successfully.",
                "CDSE usage dashboard: https://dataspace.copernicus.eu/quota",
            ],
        )
    except Exception as exc:
        return CheckResult(
            "FAIL",
            f"CDSE auth failed for {username}: {exc}",
            [
                "Check your CDSE credentials.",
                "Note: CDSE auth is OPTIONAL — the pipeline will still work without it.",
            ],
        )


def check_chirps(cfg: dict) -> CheckResult:
    """CHIRPS rainfall — part of the GEE collection, no separate auth needed."""
    sources = cfg.get("sources", {})
    chirps_cfg = sources.get("chirps", {})

    if not chirps_cfg.get("enabled", False):
        return CheckResult(
            "WARN",
            "CHIRPS is disabled in this config (sources.chirps.enabled: false)",
            ["Enable it to append rainfall accumulation columns to stats.csv."],
        )

    # CHIRPS uses GEE — if GEE is working, CHIRPS works too
    return CheckResult(
        "OK",
        f"CHIRPS enabled — collection: {chirps_cfg.get('collection', 'UCSB-CHG/CHIRPS/DAILY')} "
        f"| accumulation: {chirps_cfg.get('accumulation_days', 30)} days",
        [
            "CHIRPS uses the same GEE connection as Sentinel-2/1 — no separate auth.",
            "Archive: January 1981 – present (~3-week lag)",
        ],
    )


# -----------------------------------------------------------------------------
# Checker registry — add new checkers here
# -----------------------------------------------------------------------------

CHECKERS: list[tuple[str, callable]] = [
    ("Google Earth Engine (GEE)", check_gee),
    ("Microsoft Planetary Computer (MPC)", check_mpc),
    ("Copernicus Data Space (CDSE)", check_cdse),
    ("CHIRPS Rainfall", check_chirps),
]


# -----------------------------------------------------------------------------
# Formatting
# -----------------------------------------------------------------------------

_STATUS_ICONS = {
    "OK":   "  OK  ",
    "WARN": " WARN ",
    "FAIL": " FAIL ",
}

_STATUS_COLORS = {
    "OK":   "\033[32m",   # green
    "WARN": "\033[33m",   # yellow
    "FAIL": "\033[31m",   # red
}
_RESET = "\033[0m"


def _color(text: str, status: str, use_color: bool) -> str:
    if not use_color:
        return text
    return f"{_STATUS_COLORS.get(status, '')}{text}{_RESET}"


def _print_result(name: str, result: CheckResult, use_color: bool) -> None:
    icon = _STATUS_ICONS[result.status]
    icon_colored = _color(f"[{result.status}]", result.status, use_color)
    print(f"  {icon_colored}  {name}")
    print(f"         {result.summary}")
    for line in result.details:
        print(f"         {line}")
    print()


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="check_usage",
        description="Check connectivity and credentials for all satME data services",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            If no --config is supplied, only checks that do not require a config
            (MPC) are run with full detail; others run with environment variable
            credentials only.
        """),
    )
    parser.add_argument(
        "--config", metavar="PATH",
        help="Path to a YAML config file (optional)",
    )
    parser.add_argument(
        "--no-color", action="store_true",
        help="Disable ANSI color output",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    # Load config (empty dict if none provided)
    cfg: dict = {}
    if args.config:
        config_path = Path(args.config)
        if not config_path.exists():
            print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
            return 1
        try:
            with open(config_path) as f:
                cfg = yaml.safe_load(f) or {}
        except yaml.YAMLError as exc:
            print(f"ERROR: Failed to parse config: {exc}", file=sys.stderr)
            return 1

    use_color = not args.no_color and sys.stdout.isatty()

    print()
    print("  satME — Service Connectivity & Credential Check")
    print("  " + "-" * 52)
    if args.config:
        print(f"  Config: {args.config}")
    else:
        print("  Config: (none — using environment variables only)")
    print()

    results = []
    for name, checker_fn in CHECKERS:
        try:
            result = checker_fn(cfg)
        except Exception as exc:
            result = CheckResult("FAIL", f"Checker raised an unexpected error: {exc}")
        _print_result(name, result, use_color)
        results.append(result)

    # Summary
    n_ok   = sum(1 for r in results if r.status == "OK")
    n_warn = sum(1 for r in results if r.status == "WARN")
    n_fail = sum(1 for r in results if r.status == "FAIL")

    print("  " + "-" * 52)
    summary = f"  {n_ok} OK  |  {n_warn} WARN  |  {n_fail} FAIL"
    if n_fail > 0:
        print(_color(summary, "FAIL", use_color))
    elif n_warn > 0:
        print(_color(summary, "WARN", use_color))
    else:
        print(_color(summary, "OK", use_color))
    print()

    return 1 if n_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
