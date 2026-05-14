"""
parse_imd.py — Parse Maxar/DigitalGlobe .IMD metadata files into readable text.

Reads all .IMD files found in data/wv2/ and prints a structured, human-readable
summary for each one. Also saves a JSON file alongside each IMD for programmatic use.

Usage:
    python parse_imd.py                        # parse all IMDs in data/wv2/
    python parse_imd.py data/wv2/25FEB11.IMD  # parse a specific file

IMD format:
    key = value;               — top-level key/value pairs
    BEGIN_GROUP = GROUP_NAME   — opens a named section
        key = value;           — group-level key/value pairs
    END_GROUP = GROUP_NAME     — closes the section
"""

import re
import json
import sys
from pathlib import Path

# ── Band reference — makes output human-readable ──────────────────────────────
BAND_NAMES = {
    "BAND_C":  "Coastal Blue (~425 nm)",
    "BAND_B":  "Blue         (~480 nm)",
    "BAND_G":  "Green        (~545 nm)",
    "BAND_Y":  "Yellow       (~605 nm)",
    "BAND_R":  "Red          (~660 nm)",
    "BAND_RE": "Red Edge     (~725 nm)",
    "BAND_N":  "NIR1         (~833 nm)",
    "BAND_N2": "NIR2         (~950 nm)",
    "BAND_P":  "Panchromatic (~450-800 nm)",
}

# ── Friendly labels for common keys ──────────────────────────────────────────
KEY_LABELS = {
    # Image-level
    "version":               "IMD version",
    "generationTime":        "Product generated",
    "productOrderId":        "Order ID",
    "bandId":                "Band type",
    "numRows":               "Image rows (height px)",
    "numColumns":            "Image columns (width px)",
    "productLevel":          "Processing level",
    "productType":           "Product type",
    "radiometricLevel":      "Radiometric correction",
    "bitsPerPixel":          "Bit depth",
    "compressionType":       "Compression",
    # Band-level
    "absCalFactor":          "Absolute calibration factor",
    "effectiveBandwidth":    "Effective bandwidth (μm)",
    "TDILevel":              "TDI level (sensitivity)",
    "ULLon":                 "Upper-left longitude",
    "ULLat":                 "Upper-left latitude",
    "LRLon":                 "Lower-right longitude",
    "LRLat":                 "Lower-right latitude",
    # Acquisition
    "satId":                 "Satellite",
    "firstLineTime":         "Acquisition time (UTC)",
    "cloudCover":            "Cloud cover fraction",
    "meanSunEl":             "Mean sun elevation (°)",
    "meanSunAz":             "Mean sun azimuth (°)",
    "meanSatEl":             "Mean satellite elevation (°)",
    "meanOffNadirViewAngle": "Mean off-nadir angle (°)",
    "meanCollectedGSD":      "Mean collected GSD (m)",
    "colSpacing":            "Column spacing / pixel size (m)",
    "rowSpacing":            "Row spacing / pixel size (m)",
    "PNIIRS":                "Image quality (PNIIRS 0-10)",
    # Projection
    "mapProjName":           "Map projection",
    "mapZone":               "UTM zone",
    "mapHemi":               "Hemisphere",
    "productGSD":            "Product GSD (m)",
    "DEMCorrection":         "DEM correction applied",
    "terrainHae":            "Terrain elevation HAE (m)",
}

SATELLITES = {
    "WV02": "WorldView-2",
    "WV03": "WorldView-3",
    "WV04": "WorldView-4",
    "GE01": "GeoEye-1",
    "QB02": "QuickBird-2",
}


# ── Parser ────────────────────────────────────────────────────────────────────
def parse_imd(path: Path) -> dict:
    """
    Parse an IMD file into a nested dictionary.
    Top-level keys go into the root dict.
    BEGIN_GROUP/END_GROUP sections become nested dicts under their group name.
    """
    root = {}
    stack = [root]       # stack of dicts; top = current scope
    group_names = []     # parallel stack of group names for display

    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip().rstrip(";")
        if not line or line == "END":
            continue

        if line.startswith("BEGIN_GROUP"):
            group = line.split("=", 1)[1].strip()
            new_dict = {}
            stack[-1][group] = new_dict
            stack.append(new_dict)
            group_names.append(group)

        elif line.startswith("END_GROUP"):
            stack.pop()
            if group_names:
                group_names.pop()

        elif "=" in line:
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"')

            # Try to coerce to int or float
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    pass

            stack[-1][key] = val

    return root


# ── Formatter ─────────────────────────────────────────────────────────────────
def format_imd(data: dict, filename: str) -> str:
    """Render a parsed IMD dict as a readable text report."""
    lines = []
    sep = "═" * 62

    lines.append(sep)
    lines.append(f"  IMD METADATA  —  {filename}")
    lines.append(sep)

    # ── Image overview ────────────────────────────────────────────────────────
    lines.append("\n  IMAGE OVERVIEW")
    lines.append("  " + "─" * 58)

    overview_keys = [
        "generationTime", "productOrderId", "bandId",
        "numRows", "numColumns", "productLevel",
        "radiometricLevel", "bitsPerPixel", "compressionType",
    ]
    for k in overview_keys:
        if k in data:
            label = KEY_LABELS.get(k, k)
            lines.append(f"  {label:<38}  {data[k]}")

    # ── Acquisition details (from IMAGE_1 group) ──────────────────────────────
    img_group = data.get("IMAGE_1", {})
    if img_group:
        lines.append("\n  ACQUISITION DETAILS")
        lines.append("  " + "─" * 58)

        sat_raw = img_group.get("satId", "")
        sat_name = SATELLITES.get(sat_raw, sat_raw)
        lines.append(f"  {'Satellite':<38}  {sat_name} ({sat_raw})")

        acq_keys = [
            "firstLineTime", "cloudCover", "meanSunEl", "meanSunAz",
            "meanSatEl", "meanOffNadirViewAngle", "meanCollectedGSD", "PNIIRS",
        ]
        for k in acq_keys:
            if k in img_group:
                label = KEY_LABELS.get(k, k)
                val   = img_group[k]
                # Format cloud cover as percentage
                if k == "cloudCover":
                    val = f"{float(val)*100:.1f}%"
                lines.append(f"  {label:<38}  {val}")

    # ── Map projection ────────────────────────────────────────────────────────
    proj_group = data.get("MAP_PROJECTED_PRODUCT", {})
    if proj_group:
        lines.append("\n  MAP PROJECTION")
        lines.append("  " + "─" * 58)
        proj_keys = [
            "mapProjName", "mapZone", "mapHemi",
            "colSpacing", "rowSpacing", "productGSD",
            "DEMCorrection", "terrainHae",
        ]
        for k in proj_keys:
            if k in proj_group:
                label = KEY_LABELS.get(k, k)
                lines.append(f"  {label:<38}  {proj_group[k]}")

        # Corner coordinates
        lines.append(f"\n  {'Corner coordinates (lon/lat)':<38}")
        ul = (proj_group.get("ULX"), proj_group.get("ULY"))
        lr = (proj_group.get("LRX"), proj_group.get("LRY"))
        lines.append(f"    Upper-left  (UTM):  X={ul[0]}  Y={ul[1]}")
        lines.append(f"    Lower-right (UTM):  X={lr[0]}  Y={lr[1]}")

    # ── Per-band calibration ──────────────────────────────────────────────────
    band_groups = {k: v for k, v in data.items()
                   if k.startswith("BAND_") and isinstance(v, dict)}
    if band_groups:
        lines.append("\n  BAND CALIBRATION")
        lines.append("  " + "─" * 58)
        lines.append(f"  {'Band':<24}  {'absCalFactor':>14}  {'Bandwidth (μm)':>16}  {'TDI':>5}")
        lines.append("  " + "-" * 58)
        for band_key in ["BAND_C","BAND_B","BAND_G","BAND_Y",
                         "BAND_R","BAND_RE","BAND_N","BAND_N2","BAND_P"]:
            if band_key not in band_groups:
                continue
            bg   = band_groups[band_key]
            name = BAND_NAMES.get(band_key, band_key)
            cal  = bg.get("absCalFactor", "—")
            bw   = bg.get("effectiveBandwidth", "—")
            tdi  = bg.get("TDILevel", "—")
            if isinstance(cal, float):
                cal = f"{cal:.6e}"
            if isinstance(bw, float):
                bw = f"{bw:.4f}"
            lines.append(f"  {name:<24}  {cal:>14}  {bw:>16}  {tdi:>5}")

        lines.append("\n  absCalFactor : multiply DN × absCalFactor to get at-sensor radiance")
        lines.append("                 (W/m²/sr/μm). Not needed for ratio indices (NDVI etc).")
        lines.append("  TDI level    : time-delay integration — higher = more light collected")
        lines.append("                 per scan line (tradeoff: motion blur risk at high TDI).")

    lines.append("\n" + sep)
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) > 1:
        # Specific file(s) passed as arguments
        imd_files = [Path(p) for p in sys.argv[1:] if Path(p).suffix.upper() == ".IMD"]
    else:
        # Auto-discover all IMD files in data/wv2/
        imd_files = sorted(Path("data/wv2").glob("*.IMD"))

    if not imd_files:
        print("No .IMD files found. Pass a path as argument or place files in data/wv2/")
        return

    for imd_path in imd_files:
        print(f"\nParsing: {imd_path}")
        data = parse_imd(imd_path)

        # Print formatted report
        report = format_imd(data, imd_path.name)
        print(report)

        # Save JSON alongside the IMD
        json_path = imd_path.with_suffix(".json")
        json_path.write_text(json.dumps(data, indent=2))
        print(f"  JSON saved: {json_path}")


if __name__ == "__main__":
    main()
