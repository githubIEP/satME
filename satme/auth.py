"""GEE authentication and initialisation.

Supports:
  - Interactive OAuth flow (default for personal accounts)
  - Service account key file
  - Application Default Credentials (ADC / Cloud environments)

The GEE project ID is required and must be supplied via the config or the
GOOGLE_CLOUD_PROJECT / EARTHENGINE_PROJECT environment variable.
"""

import os
import logging

import ee

logger = logging.getLogger(__name__)


def initialise(project_id: str | None = None, service_account_key: str | None = None) -> str:
    """Authenticate with GEE and initialise the ee library.

    Parameters
    ----------
    project_id:
        GEE cloud project ID.  Falls back to the EARTHENGINE_PROJECT or
        GOOGLE_CLOUD_PROJECT environment variables if not provided.
    service_account_key:
        Path to a service account JSON key file.  If omitted, the standard
        OAuth flow / Application Default Credentials are used.

    Returns
    -------
    str
        The resolved project ID that was used for initialisation.

    Raises
    ------
    EnvironmentError
        If no project ID can be resolved.
    ee.EEException
        If authentication or initialisation fails.
    """
    resolved_project = (
        project_id
        or os.environ.get("EARTHENGINE_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )
    if not resolved_project:
        raise EnvironmentError(
            "GEE project ID is required.  Supply it via the config file "
            "(auth.gee_project), the EARTHENGINE_PROJECT environment variable, "
            "or the GOOGLE_CLOUD_PROJECT environment variable."
        )

    if service_account_key:
        logger.info("Authenticating with service account key: %s", service_account_key)
        credentials = ee.ServiceAccountCredentials(
            email=None,  # read from key file
            key_file=service_account_key,
        )
        ee.Initialize(credentials, project=resolved_project)
    else:
        logger.info("Authenticating via OAuth / Application Default Credentials")
        try:
            ee.Initialize(project=resolved_project)
        except Exception:
            # Fall back to interactive authentication if credentials are missing
            logger.info("No existing credentials found — starting interactive auth flow")
            ee.Authenticate()
            ee.Initialize(project=resolved_project)

    logger.info("GEE initialised — project: %s", resolved_project)
    return resolved_project


def verify_connection() -> dict:
    """Probe GEE with a lightweight server call to confirm the connection works.

    Returns
    -------
    dict
        ``{"ok": True, "algorithm_count": N}`` on success.

    Raises
    ------
    ee.EEException
        If the connection test fails.
    """
    try:
        # listAlgorithms is a cheap metadata call — no pixel computation
        algorithms = ee.ApiFunction.allSignatures()
        count = len(algorithms)
        logger.info("GEE connection verified — %d algorithms available", count)
        return {"ok": True, "algorithm_count": count}
    except Exception as exc:
        raise ee.EEException(f"GEE connection test failed: {exc}") from exc
