"""MPC authentication stub.

MPC does not require user login — it uses short-lived SAS tokens that
planetary_computer.sign() appends to each asset URL automatically.
No credentials file or OAuth flow is needed for public collections.

For private/commercial collections (e.g. Sentinel-1 RTC, Landsat C2):
  - Register at https://planetarycomputer.microsoft.com
  - Set env var PC_SDK_SUBSCRIPTION_KEY or pass key to sign()

Equivalent to: satme/auth.py  (GEE version)
"""

# PSEUDOCODE — not executable

# ─── Required packages ────────────────────────────────────────────────────────
# import planetary_computer          # pip install planetary-computer
# import pystac_client

# ─── Constants ────────────────────────────────────────────────────────────────
# MPC_STAC_ENDPOINT = "https://planetarycomputer.microsoft.com/api/stac/v1"


# def initialise(subscription_key: str | None = None) -> None:
#     """Configure the planetary_computer SDK.
#
#     Parameters
#     ----------
#     subscription_key:
#         Optional — required only for private/commercial collections.
#         If None, uses the PC_SDK_SUBSCRIPTION_KEY environment variable
#         (or anonymous access for open collections like Sentinel-2 L2A).
#
#     Side effects
#     ------------
#     Sets planetary_computer.settings.subscription_key so that
#     subsequent calls to sign() and sign_inplace() work automatically.
#     """
#     if subscription_key:
#         planetary_computer.settings.set_subscription_key(subscription_key)
#     # No-op for open collections — sign() still works without a key.


# def get_catalog() -> pystac_client.Client:
#     """Return an open STAC client connected to MPC.
#
#     The client uses MPC's token modifier so that every asset URL
#     returned by search() is automatically signed.
#
#     Returns
#     -------
#     pystac_client.Client
#         Authenticated STAC client.
#     """
#     return pystac_client.Client.open(
#         MPC_STAC_ENDPOINT,
#         modifier=planetary_computer.sign_inplace,
#     )


# def verify_connection(catalog: pystac_client.Client) -> dict:
#     """Verify the catalog is reachable.
#
#     Returns
#     -------
#     dict
#         {"status": "ok", "endpoint": MPC_STAC_ENDPOINT,
#          "collections_available": N}
#
#     Equivalent to: auth.verify_connection() which calls
#     ee.ApiFunction.allSignatures() to count available GEE algorithms.
#     """
#     collections = list(catalog.get_collections())
#     return {
#         "status": "ok",
#         "endpoint": MPC_STAC_ENDPOINT,
#         "collections_available": len(collections),
#     }
