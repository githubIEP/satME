"""Index registry — the single source of truth for spectral index definitions.

All band names are keyed to Sentinel-2 L2A.  Each entry specifies:
  - formula   : lambda taking an ee.Image, returning an ee.Image (single band)
  - bands     : list of band names the formula requires
  - valid_range: (min, max) physically valid range for the index
  - description: plain-English description

Adding a new index means adding one entry here; nothing else in the codebase
needs to change.

Usage
-----
    from satme.indices import REGISTRY, compute

    # Compute NDVI for a masked Sentinel-2 image
    ndvi_image = compute(s2_image, "NDVI")

    # Check required bands before computing
    required = REGISTRY["NDMI"]["bands"]


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SENTINEL-2 L2A — BAND REFERENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Band   Name                 Wavelength (nm)   Resolution   Notes
─────  ───────────────────  ───────────────   ──────────   ─────────────────────
B1     Coastal Aerosol      442.7             60 m         Atmospheric correction
B2     Blue                 492.4             10 m         Vegetation, water
B3     Green                559.8             10 m         Vegetation, water
B4     Red                  664.6             10 m         Vegetation
B5     Red Edge 1           704.1             20 m         Vegetation stress
B6     Red Edge 2           740.5             20 m         Vegetation stress
B7     Red Edge 3           782.8             20 m         Vegetation stress
B8     NIR (broad)          832.8             10 m         Vegetation, LAI
B8A    Red Edge 4 / NIR     864.7             20 m         Moisture, NDMI
B9     Water Vapour         945.1             60 m         Atmospheric correction
B11    SWIR 1               1613.7            20 m         Moisture, soil, fire
B12    SWIR 2               2202.4            20 m         Soil, minerals, fire
SCL    Scene Classification  —                20 m         Cloud/shadow mask (used internally)
TCI_R  True Colour (R)       —                10 m         RGB display only
TCI_G  True Colour (G)       —                10 m
TCI_B  True Colour (B)       —                10 m

Currently loaded by pipeline: B2, B3, B4, B8, B8A, B11, SCL
To add a band (e.g. B5, B12) add it to _BANDS in sentinel2.py and define
an index that uses it below — no other changes required.


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FULL INDEX CATALOG (enabled = in REGISTRY; disabled = commented out)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

──── VEGETATION ────────────────────────────────────────────────────────────────

NDVI   Normalised Difference Vegetation Index             [ENABLED]
       Formula : (B8 - B4) / (B8 + B4)
       Range   : -1 to +1  (healthy veg > 0.5)
       Bands   : B8 (NIR), B4 (Red)
       Notes   : Primary vegetation health proxy; saturates at high biomass.

EVI    Enhanced Vegetation Index                          [ENABLED]
       Formula : 2.5 × (B8 - B4) / (B8 + 6×B4 - 7.5×B2 + 1)
       Range   : -1 to +1
       Bands   : B8 (NIR), B4 (Red), B2 (Blue)
       Notes   : Reduces soil noise and atmospheric scatter vs NDVI;
                 preferred for high-biomass or aerosol-heavy regions.

SAVI   Soil-Adjusted Vegetation Index                     [ENABLED]
       Formula : 1.5 × (B8 - B4) / (B8 + B4 + 0.5)
       Range   : -1.5 to +1.5
       Bands   : B8 (NIR), B4 (Red)
       Notes   : Adds a soil brightness correction factor (L=0.5 default);
                 better than NDVI where vegetation cover < 40%.

MSAVI  Modified SAVI                                      [disabled — add B8/B4]
       Formula : (2×B8 + 1 - sqrt((2×B8+1)² - 8×(B8-B4))) / 2
       Range   : -1 to +1
       Bands   : B8 (NIR), B4 (Red)
       Notes   : Self-adjusting L factor; optimal for bare / sparse veg.
                 Avoids need to choose L manually.

OSAVI  Optimised SAVI                                     [disabled — add B8/B4]
       Formula : (B8 - B4) / (B8 + B4 + 0.16)
       Range   : -1 to +1
       Bands   : B8 (NIR), B4 (Red)
       Notes   : L fixed at 0.16 — reported as optimal for many ecosystems.

RENDVI Red-Edge NDVI                                      [disabled — add B5/B8A]
       Formula : (B8A - B5) / (B8A + B5)
       Range   : -1 to +1
       Bands   : B8A (RedEdge4), B5 (RedEdge1)
       Notes   : More sensitive to chlorophyll than standard NDVI;
                 useful for detecting early-stage stress before visible
                 change in NDVI.  Both bands at 20 m.

CIre   Chlorophyll Index Red-Edge                         [disabled — add B7/B5]
       Formula : (B7 / B5) - 1
       Range   : 0 to ~10
       Bands   : B7 (RedEdge3), B5 (RedEdge1)
       Notes   : Linear proxy for canopy chlorophyll content;
                 less saturated than NDVI in dense canopies.

LAI    Leaf Area Index (empirical from NDVI)              [disabled — add B8/B4]
       Formula : 3.618 × EVI - 0.118  (or various empirical fits)
       Range   : 0 to ~8 m²/m²
       Bands   : B8, B4, B2 (via EVI)
       Notes   : Not a direct measurement — empirical model varies by biome.

──── WATER / MOISTURE ──────────────────────────────────────────────────────────

NDWI   Normalised Difference Water Index (McFeeters 1996) [ENABLED]
       Formula : (B3 - B8) / (B3 + B8)
       Range   : -1 to +1  (open water > 0)
       Bands   : B3 (Green), B8 (NIR)
       Notes   : Delineates open water bodies; positive = water.
                 Can include wet soil and wetlands.

NDMI   Normalised Difference Moisture Index               [ENABLED]
       Formula : (B8A - B11) / (B8A + B11)
       Range   : -1 to +1
       Bands   : B8A (NIR), B11 (SWIR1)  — both 20 m
       Notes   : Vegetation/soil moisture with subsurface sensitivity.
                 Primary metric for dam impact (soil saturation zones).
                 Use 20 m scale for reduceRegion.

MNDWI  Modified NDWI (Xu 2006)                           [disabled — add B3/B11]
       Formula : (B3 - B11) / (B3 + B11)
       Range   : -1 to +1
       Bands   : B3 (Green), B11 (SWIR1)
       Notes   : Suppresses built-up land and vegetation signal better
                 than NDWI; preferred for urban water mapping.

AWEI   Automated Water Extraction Index                   [disabled — add B2/B3/B8/B11/B12]
       AWEInsh: 4×(B3-B11) - (0.25×B8 + 2.75×B12)
       AWEIsh : B2 + 2.5×B3 - 1.5×(B8+B11) - 0.25×B12
       Bands   : B2, B3, B8, B11, B12
       Notes   : AWEIsh includes shadow suppression; AWEInsh for non-shadow
                 areas. Requires adding B12 to _BANDS.

WRI    Water Ratio Index                                   [disabled — add B3/B4/B8/B11]
       Formula : (B3 + B4) / (B8 + B11)
       Range   : 0 to ~5  (> 1 = water)
       Bands   : B3, B4, B8, B11
       Notes   : Simple ratio; computationally lighter than AWEI.

LST    Land Surface Temperature (proxy)                   [disabled — Landsat only]
       Notes   : Thermal band (B10 on Landsat 8/9) required; not available
                 on Sentinel-2.  Use Landsat source for LST computation.

──── URBAN / BUILT-UP ──────────────────────────────────────────────────────────

NDBI   Normalised Difference Built-Up Index               [disabled — add B11/B8]
       Formula : (B11 - B8) / (B11 + B8)
       Range   : -1 to +1  (built-up > 0)
       Bands   : B11 (SWIR1), B8 (NIR)
       Notes   : Positive values indicate built-up surfaces; negative = veg.
                 The inverse of NDMI — one formula covers both interpretations.

IBI    Index-Based Built-Up Index                         [disabled — add B3/B4/B8/B11]
       Formula : NDBI - (NDVI + NDWI) / 2
       Bands   : B3, B4, B8, B11
       Notes   : Suppresses vegetation and water to isolate built-up signal.

UI     Urban Index                                        [disabled — add B8/B12]
       Formula : (B12 - B8A) / (B12 + B8A)
       Bands   : B12 (SWIR2), B8A (NIR)
       Notes   : Highlights impervious surfaces; requires adding B12.

──── BARE SOIL / GEOLOGY ────────────────────────────────────────────────────────

BSI    Bare Soil Index                                     [disabled — add B2/B4/B8/B11]
       Formula : ((B11 + B4) - (B8 + B2)) / ((B11 + B4) + (B8 + B2))
       Range   : -1 to +1  (bare soil > 0)
       Bands   : B2, B4, B8, B11
       Notes   : Highlights bare ground; useful for erosion / land degradation.

NDSI   Normalised Difference Snow Index                   [disabled — add B3/B11]
       Formula : (B3 - B11) / (B3 + B11)
       Range   : -1 to +1  (snow > 0.4)
       Bands   : B3 (Green), B11 (SWIR1)
       Notes   : Distinguishes snow from cloud (cloud is bright in SWIR;
                 snow is dark).  Same formula as MNDWI — context determines
                 which interpretation applies.

NBR    Normalised Burn Ratio                               [disabled — add B8/B12]
       Formula : (B8 - B12) / (B8 + B12)
       Range   : -1 to +1  (burned < -0.1)
       Bands   : B8 (NIR), B12 (SWIR2)
       Notes   : Primary burn severity metric; dNBR (pre–post) used for
                 fire damage assessment. Requires adding B12.

NBR2   Normalised Burn Ratio 2                            [disabled — add B11/B12]
       Formula : (B11 - B12) / (B11 + B12)
       Range   : -1 to +1
       Bands   : B11 (SWIR1), B12 (SWIR2)
       Notes   : Emphasises short-wave moisture; complementary to NBR for
                 smouldering detection.

SWIR   SWIR Composite Ratio                               [disabled — add B11/B12]
       Formula : B11 / B12
       Range   : 0 to ~5
       Bands   : B11 (SWIR1), B12 (SWIR2)
       Notes   : Simple; used in geology and mineralogy to distinguish
                 clay-bearing soils from non-clay.

──── CHLOROPHYLL / CROP HEALTH ──────────────────────────────────────────────────

GNDVI  Green NDVI                                         [disabled — add B3/B8]
       Formula : (B8 - B3) / (B8 + B3)
       Range   : -1 to +1
       Bands   : B8 (NIR), B3 (Green)
       Notes   : Sensitive to chlorophyll concentration at higher values
                 than NDVI; less prone to saturation in dense crops.

NDRE   Normalised Difference Red-Edge                     [disabled — add B5/B8A]
       Formula : (B8A - B5) / (B8A + B5)
       Range   : -1 to +1
       Bands   : B8A (NIR), B5 (RedEdge1)
       Notes   : Earlier stress indicator than NDVI; widely used in
                 precision agriculture.  Same formula as RENDVI above.

MTCI   MERIS Terrestrial Chlorophyll Index                [disabled — add B5/B6/B4]
       Formula : (B6 - B5) / (B5 - B4)
       Bands   : B6 (RedEdge2), B5 (RedEdge1), B4 (Red)
       Notes   : Linear with chlorophyll; does not saturate as readily
                 as NDVI.  Requires 20 m bands B5/B6.

S2REP  Sentinel-2 Red-Edge Position                       [disabled — add B4/B5/B6/B7]
       Formula : 705 + 35 × ((B4+B7)/2 - B5) / (B6 - B5)
       Bands   : B4, B5, B6, B7
       Notes   : Estimates the red-edge inflection wavelength (in nm).
                 Correlated with LAI and chlorophyll; all 20 m bands.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
To enable a disabled index:
  1. Add any missing band(s) to _BANDS in satme/sources/sentinel2.py
  2. Add the index entry to REGISTRY below
  3. List it in indices: [...] in your config YAML
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import ee

REGISTRY: dict = {
    # ── Vegetation ────────────────────────────────────────────────────────
    "NDVI": {
        # (B8 - B4) / (B8 + B4)
        "formula": lambda img: (
            img.normalizedDifference(["B8", "B4"]).rename("NDVI")
        ),
        "bands": ["B8", "B4"],
        "valid_range": (-1.0, 1.0),
        "description": (
            "Normalised Difference Vegetation Index — primary vegetation health indicator. "
            "Higher values indicate denser, healthier vegetation."
        ),
    },
    "EVI": {
        # 2.5 × (B8 - B4) / (B8 + 6×B4 - 7.5×B2 + 1)
        "formula": lambda img: (
            img.expression(
                "2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))",
                {"NIR": img.select("B8"), "RED": img.select("B4"), "BLUE": img.select("B2")},
            ).rename("EVI")
        ),
        "bands": ["B8", "B4", "B2"],
        "valid_range": (-1.0, 1.0),
        "description": (
            "Enhanced Vegetation Index — reduced soil noise compared to NDVI, "
            "better in high-biomass regions."
        ),
    },
    "SAVI": {
        # 1.5 × (B8 - B4) / (B8 + B4 + 0.5)
        "formula": lambda img: (
            img.expression(
                "1.5 * (NIR - RED) / (NIR + RED + 0.5)",
                {"NIR": img.select("B8"), "RED": img.select("B4")},
            ).rename("SAVI")
        ),
        "bands": ["B8", "B4"],
        "valid_range": (-1.5, 1.5),
        "description": (
            "Soil-Adjusted Vegetation Index — better than NDVI in sparse vegetation "
            "cover where bare soil reflectance contaminates the signal."
        ),
    },

    # ── Water / Moisture ──────────────────────────────────────────────────
    "NDWI": {
        # (B3 - B8) / (B3 + B8)
        "formula": lambda img: (
            img.normalizedDifference(["B3", "B8"]).rename("NDWI")
        ),
        "bands": ["B3", "B8"],
        "valid_range": (-1.0, 1.0),
        "description": (
            "Normalised Difference Water Index — surface water presence. "
            "Positive values indicate open water or high moisture."
        ),
    },
    "NDMI": {
        # (B8A - B11) / (B8A + B11)
        "formula": lambda img: (
            img.normalizedDifference(["B8A", "B11"]).rename("NDMI")
        ),
        "bands": ["B8A", "B11"],
        "valid_range": (-1.0, 1.0),
        "description": (
            "Normalised Difference Moisture Index — vegetation/soil moisture with "
            "subsurface sensitivity. Key metric for dam impact analysis. "
            "90th percentile is the primary output statistic."
        ),
    },
    "MNDWI": {
        # (B3 - B11) / (B3 + B11)
        # Requires B11 — already in _BANDS
        "formula": lambda img: (
            img.normalizedDifference(["B3", "B11"]).rename("MNDWI")
        ),
        "bands": ["B3", "B11"],
        "valid_range": (-1.0, 1.0),
        "description": (
            "Modified NDWI (Xu 2006) — suppresses built-up/vegetation signal better "
            "than NDWI; preferred for urban or mixed water mapping."
        ),
    },

    # ── Urban / Built-up ─────────────────────────────────────────────────
    "NDBI": {
        # (B11 - B8) / (B11 + B8)
        # Note: this is the arithmetic inverse of NDMI — positive = built-up
        "formula": lambda img: (
            img.normalizedDifference(["B11", "B8"]).rename("NDBI")
        ),
        "bands": ["B11", "B8"],
        "valid_range": (-1.0, 1.0),
        "description": (
            "Normalised Difference Built-Up Index — positive values indicate "
            "impervious / built-up surfaces; negative = vegetation."
        ),
    },

    # ── Bare Soil / Fire ──────────────────────────────────────────────────
    "BSI": {
        # ((B11 + B4) - (B8 + B2)) / ((B11 + B4) + (B8 + B2))
        "formula": lambda img: (
            img.expression(
                "((SWIR + RED) - (NIR + BLUE)) / ((SWIR + RED) + (NIR + BLUE))",
                {
                    "SWIR": img.select("B11"),
                    "RED":  img.select("B4"),
                    "NIR":  img.select("B8"),
                    "BLUE": img.select("B2"),
                },
            ).rename("BSI")
        ),
        "bands": ["B11", "B4", "B8", "B2"],
        "valid_range": (-1.0, 1.0),
        "description": (
            "Bare Soil Index — highlights bare ground and eroded surfaces; "
            "useful for land degradation and erosion monitoring."
        ),
    },

    # ── Chlorophyll / Crop health ─────────────────────────────────────────
    "GNDVI": {
        # (B8 - B3) / (B8 + B3)
        "formula": lambda img: (
            img.normalizedDifference(["B8", "B3"]).rename("GNDVI")
        ),
        "bands": ["B8", "B3"],
        "valid_range": (-1.0, 1.0),
        "description": (
            "Green NDVI — sensitive to chlorophyll concentration at higher canopy "
            "density; less prone to saturation than NDVI."
        ),
    },
    "NDRE": {
        # (B8A - B5) / (B8A + B5)
        # Requires adding B5 to _BANDS in sentinel2.py
        "formula": lambda img: (
            img.normalizedDifference(["B8A", "B5"]).rename("NDRE")
        ),
        "bands": ["B8A", "B5"],
        "valid_range": (-1.0, 1.0),
        "description": (
            "Normalised Difference Red-Edge — earlier stress indicator than NDVI; "
            "detects chlorophyll reduction before visible NDVI decline. "
            "Requires B5 added to _BANDS."
        ),
    },
}


def compute(image: "ee.Image", index_name: str) -> "ee.Image":
    """Compute a spectral index and return a single-band ee.Image.

    Parameters
    ----------
    image:
        Cloud-masked ee.Image with at least the bands required by the index.
    index_name:
        Key into REGISTRY (e.g. "NDVI").

    Returns
    -------
    ee.Image
        Single-band image named after the index.

    Raises
    ------
    KeyError
        If index_name is not in REGISTRY.
    """
    if index_name not in REGISTRY:
        raise KeyError(
            f"Unknown index '{index_name}'. "
            f"Available: {sorted(REGISTRY.keys())}"
        )
    return REGISTRY[index_name]["formula"](image)


def required_bands(index_name: str) -> list:
    """Return the list of bands required to compute an index."""
    return REGISTRY[index_name]["bands"]


def validate_indices(requested: list, available_bands: list) -> list:
    """Check that all requested indices can be computed from available_bands.

    Returns a list of indices that cannot be computed (missing bands).
    """
    missing = []
    for idx in requested:
        if idx not in REGISTRY:
            missing.append(f"{idx} (not in registry)")
            continue
        needed = REGISTRY[idx]["bands"]
        absent = [b for b in needed if b not in available_bands]
        if absent:
            missing.append(f"{idx} (missing bands: {absent})")
    return missing
