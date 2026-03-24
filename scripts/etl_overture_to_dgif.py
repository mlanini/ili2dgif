#!/usr/bin/env python3
"""
ETL Pipeline: Overture Maps → DGIF GeoPackage

Orchestrates the full ETL process:
  Phase 1  — Discover pre-downloaded Overture GeoParquet files
  Phase 2  — Create empty DGIF GeoPackage (schema import from DGIF_V3.ili)
  Phase 3  — Transform and load: apply mapping table, insert into DGIF GPKG

The Overture data files must be downloaded externally (e.g. via
DuckDB CLI, overturemaps-py, or the Overture Maps Explorer website) and
placed in a local directory.  Supported formats: GeoParquet (.parquet,
.geoparquet) and GeoJSON (.geojson, .json).  Files must be named:
    overture_{theme}_{type}.parquet   (or .geoparquet / .geojson / .json)
    e.g. overture_buildings_building.parquet
         overture_transportation_segment.geojson

Prerequisites:
  - Java 8+ (java in PATH)
  - Python 3.12 with GDAL/OGR (QGIS bundled, Parquet driver required)
  - ili2gpkg 5.3.1 in ressources/ili2gpkg-5.3.1/
  - DGIF_V3.ili in models/
  - Overture_to_DGIF_V3.csv in models/
  - No internet access required

Usage:
    python etl_overture_to_dgif.py --parquet-dir C:/tmp/overture_parquet
    python etl_overture_to_dgif.py --parquet-dir C:/tmp/overture_parquet --themes buildings,transportation
"""

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path


# ============================================================================
# QGIS / GDAL environment setup (Windows)
# ============================================================================
def _find_qgis_root() -> str | None:
    """Auto-detect QGIS installation directory on Windows."""
    if sys.platform != "win32":
        return None
    base = Path(os.environ.get("PROGRAMFILES", r"C:\Program Files"))
    candidates = sorted(base.glob("QGIS *"), reverse=True)
    return str(candidates[0]) if candidates else None


def _setup_qgis_env(qgis_root: str | None = None) -> None:
    """Prepend QGIS DLL directories to PATH and set GDAL/PROJ env vars."""
    if qgis_root is None:
        qgis_root = _find_qgis_root()
    if qgis_root is None:
        return
    qgis = Path(qgis_root)
    extra_paths = [
        str(qgis / "bin"),
        str(qgis / "apps" / "gdal" / "bin"),
        str(qgis / "apps" / "Python312"),
        str(qgis / "apps" / "Python312" / "Scripts"),
        str(qgis / "apps" / "Qt5" / "bin"),
    ]
    current = os.environ.get("PATH", "")
    os.environ["PATH"] = os.pathsep.join(extra_paths) + os.pathsep + current
    os.environ["GDAL_DATA"] = str(qgis / "apps" / "gdal" / "share" / "gdal")
    os.environ["PROJ_LIB"] = str(qgis / "share" / "proj")


_setup_qgis_env()


# ============================================================================
# ANSI colour helpers
# ============================================================================
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
GREY = "\033[90m"
RESET = "\033[0m"


def info(msg: str) -> None:
    print(f"{CYAN}[INFO]{RESET} {msg}")


def ok(msg: str) -> None:
    print(f"{GREEN}[OK]{RESET} {msg}")


def warn(msg: str) -> None:
    print(f"{YELLOW}[WARNING]{RESET} {msg}")


def skip(msg: str) -> None:
    print(f"{YELLOW}[SKIP]{RESET} {msg}")


def error(msg: str) -> None:
    print(f"{RED}[ERROR]{RESET} {msg}", file=sys.stderr)


def banner(title: str) -> None:
    print()
    print(f"{CYAN}================================================================{RESET}")
    print(f"{CYAN}  {title}{RESET}")
    print(f"{CYAN}================================================================{RESET}")


def run_java(args: list[str], label: str) -> int:
    """Run a java command, stream output, return exit code."""
    cmd = ["java"] + args
    info(f"{label}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f"  {GREY}{line.rstrip()}{RESET}")
    proc.wait()
    return proc.returncode


def file_size_mb(path: str | Path) -> float:
    return round(os.path.getsize(path) / (1024 * 1024), 1)


# ============================================================================
# Overture Maps themes/types — expected file naming convention
# ============================================================================
# Files must be named overture_{theme}_{type}.{ext}
# Supported extensions: .parquet, .geoparquet, .geojson, .json
OVERTURE_THEME_TYPES = [
    ("buildings", "building", "Buildings"),
    ("buildings", "building_part", "Building parts"),
    ("transportation", "segment", "Transportation segments"),
    ("transportation", "connector", "Transportation connectors"),
    ("base", "infrastructure", "Base infrastructure"),
    ("base", "land", "Base land"),
    ("base", "land_cover", "Base land cover"),
    ("base", "land_use", "Base land use"),
    ("base", "water", "Base water"),
    ("places", "place", "Places (POIs)"),
    ("divisions", "division", "Administrative divisions"),
    ("divisions", "division_area", "Administrative areas"),
    ("divisions", "division_boundary", "Administrative boundaries"),
    ("addresses", "address", "Addresses"),
]


def discover_parquet_files(
    parquet_dir: Path,
    themes_filter: set[str] | None = None,
) -> dict[tuple[str, str], Path]:
    """Discover Overture data files in a directory.

    Looks for files named overture_{theme}_{type}.{ext} where ext is one of:
    .parquet, .geoparquet, .geojson, .json.

    Also recognises common download naming patterns such as
    ``overture-{release}-{type}-{bbox}.{ext}`` (Overture Maps Explorer)
    and ``{theme}_{type}.{ext}``.
    """
    _SUPPORTED_EXTS = [".parquet", ".geoparquet", ".geojson", ".json"]

    found: dict[tuple[str, str], Path] = {}

    if not parquet_dir.is_dir():
        return found

    for theme, otype, description in OVERTURE_THEME_TYPES:
        if themes_filter and theme not in themes_filter:
            continue

        # Try canonical names first (parquet preferred over geojson)
        candidates = []
        for ext in _SUPPORTED_EXTS:
            candidates.append(parquet_dir / f"overture_{theme}_{otype}{ext}")
            candidates.append(parquet_dir / f"{theme}_{otype}{ext}")
        for candidate in candidates:
            if candidate.exists():
                found[(theme, otype)] = candidate
                break

    # Fallback: scan all supported files and match by type keyword.
    # Handles naming patterns like: overture-{release}-{type}-{bbox}.{ext}
    # collected from the Overture Maps Explorer or other download tools.
    unmatched = [
        (theme, otype)
        for theme, otype, _ in OVERTURE_THEME_TYPES
        if (not themes_filter or theme in themes_filter)
        and (theme, otype) not in found
    ]
    if unmatched:
        all_files = []
        for ext in _SUPPORTED_EXTS:
            all_files.extend(sorted(parquet_dir.glob(f"*{ext}")))
        matched_paths = set(found.values())
        for pf in all_files:
            if pf in matched_paths:
                continue
            name_lower = pf.stem.lower()
            for theme, otype in list(unmatched):
                if (theme, otype) in found:
                    continue
                # Method 1: both theme and type keywords in filename
                if theme in name_lower and otype in name_lower:
                    found[(theme, otype)] = pf
                    break
                # Method 2: type as a distinct segment delimited by hyphens
                # e.g. overture-2026-03-18.0-land_cover-6.899,...
                if re.search(
                    rf'(?:^|[-]){re.escape(otype)}(?:[-.]|$)', name_lower
                ):
                    found[(theme, otype)] = pf
                    break

    return found


# ============================================================================
# Main
# ============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="ETL Pipeline: Overture Maps -> DGIF GeoPackage"
    )
    parser.add_argument(
        "--parquet-dir",
        required=True,
        help="Directory containing pre-downloaded Overture data files "
             "(.parquet, .geoparquet, .geojson, .json). "
             "E.g. overture_buildings_building.parquet or .geojson",
    )
    parser.add_argument(
        "--tmp-dir",
        default="C:/tmp/dgif_overture",
        help="Temporary working directory (default: C:/tmp/dgif_overture)",
    )
    parser.add_argument(
        "--output-name",
        default="DGIF_Overture.gpkg",
        help="Output DGIF GeoPackage filename (default: DGIF_Overture.gpkg)",
    )
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Skip DGIF GeoPackage schema creation if output already exists",
    )
    parser.add_argument(
        "--themes",
        default=None,
        help="Comma-separated list of themes to process (default: all). "
             "E.g. --themes buildings,transportation,base",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Path to Python interpreter with GDAL (default: current interpreter)",
    )
    args = parser.parse_args()

    # Filter themes if specified
    themes_filter = None
    if args.themes:
        themes_filter = set(t.strip().lower() for t in args.themes.split(","))

    # ========================================================================
    # Configuration
    # ========================================================================
    workspace_root = Path(__file__).resolve().parent.parent
    ili2gpkg_jar = workspace_root / "ressources" / "ili2gpkg-5.3.1" / "ili2gpkg-5.3.1.jar"
    dgif_ili = workspace_root / "models" / "DGIF_V3.ili"
    mapping_csv = workspace_root / "models" / "Overture_to_DGIF_V3.csv"
    transform_py = workspace_root / "scripts" / "etl_overture_transform.py"
    python_exe = args.python

    output_dir = workspace_root / "output"
    dgif_gpkg = output_dir / args.output_name

    tmp_dir = Path(args.tmp_dir)
    parquet_dir = Path(args.parquet_dir)

    models_dir = workspace_root / "models"
    dgif_model_dir = f"{models_dir};http://models.interlis.ch/;%JAR_DIR"

    # ========================================================================
    # Banner
    # ========================================================================
    banner("ETL Pipeline: Overture Maps -> DGIF GeoPackage")
    print()
    info(f"Parquet dir: {parquet_dir}")
    info(f"Themes:      {args.themes or 'all'}")
    print()

    # ========================================================================
    # Prerequisites check
    # ========================================================================
    print(f"{YELLOW}--- Checking prerequisites ---{RESET}")

    # Java
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        java_ver = (result.stderr or result.stdout).strip().split("\n")[0]
        ok(f"Java: {java_ver}")
    except FileNotFoundError:
        error("Java not found in PATH!")
        return 1

    # ili2gpkg
    if not ili2gpkg_jar.exists():
        error(f"ili2gpkg not found: {ili2gpkg_jar}")
        return 1
    ok(f"ili2gpkg: {ili2gpkg_jar}")

    # Required files
    for f in (dgif_ili, mapping_csv, transform_py):
        if not f.exists():
            error(f"File not found: {f}")
            return 1
        ok(f"{f.name}")

    # Python + GDAL + Parquet driver
    try:
        result = subprocess.run(
            [python_exe, "-c",
             "from osgeo import gdal, ogr; gdal.UseExceptions(); "
             "drv = ogr.GetDriverByName('Parquet'); "
             "print('GDAL', gdal.VersionInfo(), 'Parquet:', 'YES' if drv else 'NO')"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        ok(f"Python + {result.stdout.strip()}")
        if "Parquet: NO" in result.stdout:
            error("GDAL Parquet driver not available! Requires GDAL compiled with libarrow.")
            return 1
    except Exception as exc:
        error(f"Python/GDAL not available at: {python_exe} ({exc})")
        return 1

    # Parquet directory
    if not parquet_dir.is_dir():
        error(f"Parquet directory not found: {parquet_dir}")
        return 1
    ok(f"Parquet dir: {parquet_dir}")

    # Directories
    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    print()

    # ========================================================================
    # Phase 1 — Discover Overture GeoParquet files
    # ========================================================================
    banner("Phase 1: Discover Overture data files")

    parquet_files = discover_parquet_files(parquet_dir, themes_filter)

    if not parquet_files:
        error(f"No matching data files found in: {parquet_dir}")
        print()
        info("Expected file names (examples):")
        for theme, otype, desc in OVERTURE_THEME_TYPES[:5]:
            print(f"  overture_{theme}_{otype}.parquet    ({desc})")
            print(f"  overture_{theme}_{otype}.geojson    ({desc})")
        print(f"  ...")
        print()
        info("Download Overture data externally using one of:")
        info("  1. DuckDB CLI: duckdb -c \"COPY (SELECT * FROM read_parquet("
             "'s3://overturemaps-us-west-2/release/2025-05-21.0/"
             "theme=buildings/type=building/*') WHERE bbox.xmin >= 5.9 "
             "AND bbox.xmax <= 10.5 AND bbox.ymin >= 45.8 AND bbox.ymax <= 47.8) "
             "TO 'overture_buildings_building.parquet'\"")
        info("  2. overturemaps-py: overturemaps download --bbox=5.9,45.8,10.5,47.8 "
             "-f geoparquet --type=building -o overture_buildings_building.parquet")
        info("  3. Overture Maps Explorer: https://explore.overturemaps.org")
        return 1

    for (theme, otype), pf in sorted(parquet_files.items()):
        ok(f"{theme}/{otype}: {pf.name} ({file_size_mb(pf)} MB)")

    info(f"Found {len(parquet_files)} data file(s)")
    print()

    # ========================================================================
    # Phase 2 — Create empty DGIF GeoPackage (schema import)
    # ========================================================================
    banner("Phase 2: Create DGIF GeoPackage schema")

    if args.skip_schema and dgif_gpkg.exists():
        skip(f"Using existing DGIF GeoPackage: {dgif_gpkg} ({file_size_mb(dgif_gpkg)} MB)")
    else:
        if dgif_gpkg.exists():
            info(f"Removing existing: {dgif_gpkg}")
            dgif_gpkg.unlink()

        dgif_schema_log = tmp_dir / "dgif_schemaimport.log"
        dgif_schema_args = [
            "-jar", str(ili2gpkg_jar),
            "--schemaimport",
            "--dbfile", str(dgif_gpkg),
            "--defaultSrsAuth", "EPSG",
            "--defaultSrsCode", "4326",
            "--smart2Inheritance",
            "--nameByTopic",
            "--createGeomIdx",
            "--strokeArcs",
            "--createEnumTabs",
            "--createEnumTxtCol",
            "--beautifyEnumDispName",
            "--createBasketCol",
            "--createTidCol",
            "--createStdCols",
            "--createMetaInfo",
            "--createFk",
            "--createFkIdx",
            "--modeldir", dgif_model_dir,
            "--log", str(dgif_schema_log),
            str(dgif_ili),
        ]

        t0 = time.perf_counter()
        rc = run_java(dgif_schema_args, "Running ili2gpkg --schemaimport for DGIF...")
        if rc != 0:
            error(f"DGIF schema import failed! See: {dgif_schema_log}")
            return 1
        elapsed = time.perf_counter() - t0
        ok(f"DGIF GeoPackage schema created: {file_size_mb(dgif_gpkg)} MB in {elapsed:.1f}s")
    print()

    # ========================================================================
    # Phase 3 — Transform and Load (Python)
    # ========================================================================
    banner("Phase 3: Transform & Load (Python ETL)")

    # Build the list of parquet files as arguments
    parquet_args = []
    for (theme, otype), pf in sorted(parquet_files.items()):
        parquet_args.extend(["--parquet", f"{theme}/{otype}={pf}"])

    info("Running etl_overture_transform.py...")
    t0 = time.perf_counter()

    proc = subprocess.Popen(
        [
            python_exe,
            str(transform_py),
            "--dgif-gpkg", str(dgif_gpkg),
            "--mapping", str(mapping_csv),
        ] + parquet_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f"  {line.rstrip()}")
    proc.wait()

    if proc.returncode != 0:
        error("Python transform failed!")
        return 1

    elapsed = time.perf_counter() - t0
    final_size = file_size_mb(dgif_gpkg)
    ok(f"Transform completed in {elapsed:.1f}s")
    print()

    # ========================================================================
    # Summary
    # ========================================================================
    banner("ETL Pipeline Complete")
    print()
    print(f"  {GREEN}Output:  {dgif_gpkg} ({final_size} MB){RESET}")
    print(f"  {GREY}Parquet: {parquet_dir}{RESET}")
    for (theme, otype), pf in sorted(parquet_files.items()):
        print(f"  {GREY}         {pf.name} ({file_size_mb(pf)} MB){RESET}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
