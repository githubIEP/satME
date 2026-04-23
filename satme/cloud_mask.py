"""Cloud masking logic per satellite source.

Each masking function takes an ee.Image and returns a cloud-masked ee.Image
with the same bands.  Masked pixels are set to 0 in the mask layer (i.e.
they are excluded from reducers).

Sentinel-2 L2A (SCL-based)
---------------------------
SCL values excluded:
  0  = No data
  1  = Saturated / Defective
  3  = Cloud shadow
  8  = Cloud (medium probability)
  9  = Cloud (high probability)
  10 = Thin cirrus
  11 = Snow / Ice (optional — left valid here; flag separately if needed)

The valid pixels are all pixels NOT in the excluded classes.

AOI cloud % computation
-----------------------
AOI-level cloud % is now computed server-side inside prefilter_by_aoi_cloud()
in image_filter.py, which maps the calculation over the entire collection in
one GEE operation.  This avoids per-image round-trips entirely.
"""

import ee


# SCL class values to mask out for Sentinel-2 L2A
_S2_SCL_INVALID = [0, 1, 3, 8, 9, 10]


def sentinel2_scl(image: "ee.Image") -> "ee.Image":
    """Apply SCL-based cloud mask to a Sentinel-2 L2A image.

    Parameters
    ----------
    image:
        Raw Sentinel-2 ee.Image from COPERNICUS/S2_SR_HARMONIZED.
        Must contain the SCL band.

    Returns
    -------
    ee.Image
        Cloud-masked image — invalid pixels excluded from computations.
    """
    scl = image.select("SCL")

    # Build a valid-pixel mask: 1 where SCL is NOT in the invalid list
    valid_mask = scl.neq(_S2_SCL_INVALID[0])
    for val in _S2_SCL_INVALID[1:]:
        valid_mask = valid_mask.And(scl.neq(val))

    return image.updateMask(valid_mask)
