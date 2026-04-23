"""OAuth2 token management for the Copernicus Data Space Ecosystem (CDSE).

CDSE uses a Keycloak-based OAuth2 password-flow endpoint.  Tokens expire after
600 seconds; this manager refreshes automatically with a 30-second safety margin.

Credentials are read from (highest priority first):
  1. ``auth.cdse_username`` / ``auth.cdse_password`` in the YAML config
  2. ``CDSE_USERNAME`` / ``CDSE_PASSWORD`` environment variables

Proxy detection (highest priority first):
  1. ``auth.https_proxy`` in the YAML config
  2. ``HTTPS_PROXY`` / ``https_proxy`` environment variables
  3. System proxy settings (Windows Registry / macOS System Preferences)
     via ``urllib.request.getproxies()``

On corporate networks (Zscaler, Blue Coat, etc.), ``requests`` does not
always pick up the system proxy automatically.  ``build_session()`` does
the detection explicitly so CDSE calls route through the same proxy as the
rest of the network traffic.

Register a free account at https://dataspace.copernicus.eu to obtain credentials.
"""

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

_TOKEN_URL = (
    "https://identity.dataspace.copernicus.eu"
    "/auth/realms/CDSE/protocol/openid-connect/token"
)


def build_session(cfg: dict) -> requests.Session:
    """Return a ``requests.Session`` configured for CDSE network access.

    Proxy resolution order
    ----------------------
    1. ``auth.https_proxy`` in the YAML config  (explicit override)
    2. ``HTTPS_PROXY`` / ``https_proxy`` environment variables
    3. System proxy from ``urllib.request.getproxies()``
       — reads Windows Registry (IE/Edge settings) or macOS System Prefs.
       PAC-file entries are skipped (requests cannot execute PAC scripts).

    The same session is reused for both token fetches and STAC API calls so
    the proxy is applied consistently.
    """
    session = requests.Session()

    # ── Proxy detection ───────────────────────────────────────────────────────
    auth_cfg  = cfg.get("auth", {})
    proxy_url = (
        auth_cfg.get("https_proxy")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
    )

    if not proxy_url:
        try:
            import urllib.request
            sys_proxies = urllib.request.getproxies()
            candidate = sys_proxies.get("https") or sys_proxies.get("http", "")
            # Skip PAC / WPAD URLs — requests cannot execute PAC scripts
            if candidate and not candidate.lower().startswith("pac"):
                proxy_url = candidate
        except Exception:
            pass

    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})
        logger.info("CDSE: routing via proxy %s", proxy_url)
    else:
        logger.debug("CDSE: no proxy detected — using direct connection")

    return session


class TokenManager:
    """Fetches and auto-refreshes a CDSE bearer token."""

    def __init__(self, username: str, password: str, session: requests.Session) -> None:
        self._username = username
        self._password = password
        self._session  = session
        self._token: str | None = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        """Return a valid bearer token, refreshing if necessary."""
        if self._token and time.time() < self._expires_at - 30:
            return self._token
        logger.debug("Fetching CDSE OAuth2 token…")
        try:
            resp = self._session.post(
                _TOKEN_URL,
                data={
                    "grant_type": "password",
                    "client_id":  "cdse-public",
                    "username":   self._username,
                    "password":   self._password,
                },
                timeout=30,
            )
        except requests.exceptions.ConnectionError as exc:
            raise RuntimeError(
                f"Cannot reach CDSE authentication server: {exc}\n"
                "  Check network connectivity to identity.dataspace.copernicus.eu.\n"
                "  If you are on a corporate network, set auth.https_proxy in the\n"
                "  YAML config (e.g. https_proxy: 'http://proxy.company.com:8080')\n"
                "  or the HTTPS_PROXY environment variable."
            ) from exc
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            try:
                detail = resp.json().get("error_description") or resp.text[:300]
            except Exception:
                detail = resp.text[:300]
            raise RuntimeError(
                f"CDSE authentication failed (HTTP {resp.status_code}): {detail}\n"
                "  Check your cdse_username / cdse_password in the config or "
                "CDSE_USERNAME / CDSE_PASSWORD environment variables.\n"
                "  You can test your credentials directly with:\n"
                "    python -c \"import requests; r = requests.post("
                "'https://identity.dataspace.copernicus.eu/auth/realms/CDSE"
                "/protocol/openid-connect/token', "
                "data={'grant_type':'password','client_id':'cdse-public',"
                "'username':'YOUR_EMAIL','password':'YOUR_PASS'}); "
                "print(r.status_code, r.json())\""
            ) from exc
        data = resp.json()
        self._token      = data["access_token"]
        self._expires_at = time.time() + data.get("expires_in", 600)
        logger.debug("CDSE token obtained, expires in %ds", data.get("expires_in", 600))
        return self._token


def from_cfg(cfg: dict) -> "tuple[TokenManager | None, requests.Session | None]":
    """Build a TokenManager + Session from config + environment variables.

    Returns ``(None, session)`` if no credentials are available — the session
    is still needed for public CDSE OData catalog searches and Microsoft
    Planetary Computer COG reads, neither of which requires a bearer token.

    Returns ``(None, None)`` only if session construction itself fails.
    """
    auth_cfg = cfg.get("auth", {})
    username = (
        auth_cfg.get("cdse_username")
        or os.environ.get("CDSE_USERNAME", "")
    )
    password = (
        auth_cfg.get("cdse_password")
        or os.environ.get("CDSE_PASSWORD", "")
    )

    session = build_session(cfg)

    if not username or not password:
        logger.info(
            "No CDSE credentials found — CDSE catalog search will still run "
            "but band reads will use Microsoft Planetary Computer (MPC) COGs only. "
            "Set auth.cdse_username / auth.cdse_password (or CDSE_USERNAME / "
            "CDSE_PASSWORD env vars) to enable CDSE direct downloads as fallback."
        )
        return None, session

    return TokenManager(username, password, session), session
