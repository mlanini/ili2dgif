#!/usr/bin/env python3
"""
ETL Pipeline: swissTLM3D XTF → DGIF GeoPackage

Orchestrates the full ETL process:
  Phase 1  — Download swissTLM3D XTF archive from data.geo.admin.ch
  Phase 2  — Extract the XTF file from the ZIP
  Phase 2b — Validate the XTF against the INTERLIS model (ilivalidator)
  Phase 3  — Create an empty DGIF GeoPackage (schema import from DGIF_V3.ili)
  Phase 4  — Import XTF into a temporary swissTLM3D GeoPackage (ili2gpkg)
  Phase 5  — Transform and load: Python script reads TLM GPKG, applies
             mapping table, reprojects LV95→WGS84, writes into DGIF GPKG

Prerequisites:
  - Java 8+ (java in PATH)
  - Python 3.12 with GDAL/OGR (QGIS bundled)
  - ili2gpkg 5.5.1 in ressources/ili2gpkg-5.5.1/
  - ilivalidator 1.15.0 in ressources/ilivalidator-1.15.0/
  - DGIF_V3.ili in models/
  - swissTLM3D_ili2_V2_4.ili in models/
  - swissTLM3D_to_DGIF_V3.csv in models/
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile
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
    """Prepend QGIS DLL directories to PATH and set GDAL/PROJ env vars.

    When calling the QGIS-bundled Python from outside the QGIS shell, the
    native GDAL DLLs are not on PATH and ``from osgeo import gdal`` fails
    with ``ImportError: DLL load failed``.  This function mirrors what the
    QGIS startup bat files do.
    """
    if qgis_root is None:
        qgis_root = _find_qgis_root()
    if qgis_root is None:
        return  # Not on Windows or no QGIS found — assume GDAL is on PATH
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


# Apply QGIS environment before anything else uses GDAL
_setup_qgis_env()


# ============================================================================
# ANSI colour helpers (works on Windows Terminal / VS Code / modern consoles)
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
# Main
# ============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="ETL Pipeline: swissTLM3D XTF → DGIF GeoPackage"
    )
    parser.add_argument(
        "--tlm-url",
        default="https://data.geo.admin.ch/ch.swisstopo.swisstlm3d/"
                "swisstlm3d_2026-02-24/swisstlm3d_2026-02-24_2056_5728.xtf.zip",
        help="URL of the swissTLM3D XTF ZIP archive",
    )
    parser.add_argument(
        "--tmp-dir",
        default="C:/tmp/dgif",
        help="Temporary working directory (default: C:/tmp/dgif)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download if ZIP already exists",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip XTF validation with ilivalidator",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip extraction if xtf/ directory already exists with .xtf files",
    )
    parser.add_argument(
        "--skip-import",
        action="store_true",
        help="Skip XTF→GeoPackage import (Phase 4) if swisstlm3d_temp.gpkg already exists",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Path to Python interpreter with GDAL (default: current interpreter)",
    )
    args = parser.parse_args()

    # ========================================================================
    # Configuration
    # ========================================================================
    workspace_root = Path(__file__).resolve().parent.parent
    ili2gpkg_jar = workspace_root / "ressources" / "ili2gpkg-5.5.1" / "ili2gpkg-5.5.1.jar"
    ilivalidator_jar = workspace_root / "ressources" / "ilivalidator-1.15.0" / "ilivalidator-1.15.0.jar"
    dgif_ili = workspace_root / "models" / "DGIF_V3.ili"
    tlm_ili = workspace_root / "models" / "swissTLM3D_ili2_V2_4.ili"
    mapping_csv = workspace_root / "models" / "swissTLM3D_to_DGIF_V3.csv"
    transform_py = workspace_root / "scripts" / "etl_swisstlm3d_transform.py"
    python_exe = args.python

    output_dir = workspace_root / "output"
    dgif_gpkg = output_dir / "DGIF_swissTLM3D.gpkg"

    tmp_dir = Path(args.tmp_dir)
    zip_file = tmp_dir / "swisstlm3d.xtf.zip"
    tlm_gpkg = tmp_dir / "swisstlm3d_temp.gpkg"

    # Model directories (semicolon-separated, as expected by ili2gpkg / ilivalidator)
    models_dir = workspace_root / "models"
    dgif_model_dir = f"{models_dir};http://models.interlis.ch/;%JAR_DIR"
    # tlm_model_dir is set after Phase 2 (includes the xtf/ directory where
    # the model .ili shipped with the data resides)

    # ========================================================================
    # Banner
    # ========================================================================
    banner("ETL Pipeline: swissTLM3D → DGIF GeoPackage")
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

    # ilivalidator
    if not ilivalidator_jar.exists():
        error(f"ilivalidator not found: {ilivalidator_jar}")
        return 1
    ok(f"ilivalidator: {ilivalidator_jar}")

    # Required files
    for f in (dgif_ili, tlm_ili, mapping_csv, transform_py):
        if not f.exists():
            error(f"File not found: {f}")
            return 1
        ok(f"{f.name}")

    # Python + GDAL
    try:
        result = subprocess.run(
            [python_exe, "-c", "from osgeo import gdal; print('GDAL', gdal.VersionInfo())"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        ok(f"Python + {result.stdout.strip()}")
    except Exception as exc:
        error(f"Python/GDAL not available at: {python_exe} ({exc})")
        return 1

    # Temp directory
    tmp_dir.mkdir(parents=True, exist_ok=True)
    ok(f"Temp dir: {tmp_dir}")
    print()

    # ========================================================================
    # Phase 1 — Download
    # ========================================================================
    banner("Phase 1: Download swissTLM3D XTF")

    if args.skip_download and zip_file.exists():
        skip(f"Using existing: {zip_file}")
    else:
        info(f"Downloading from:")
        print(f"  {GREY}{args.tlm_url}{RESET}")
        info(f"Destination: {zip_file}")

        t0 = time.perf_counter()
        try:
            req = urllib.request.Request(args.tlm_url, headers={"User-Agent": "DGIF-ETL/1.0"})
            with urllib.request.urlopen(req, timeout=300) as resp:
                total = int(resp.headers.get("Content-Length", 0))
                total_mb = total / (1024 * 1024) if total else 0
                downloaded = 0
                chunk_size = 1024 * 1024  # 1 MB chunks
                with open(str(zip_file), "wb") as fout:
                    while True:
                        chunk = resp.read(chunk_size)
                        if not chunk:
                            break
                        fout.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            pct = downloaded * 100 / total
                            dl_mb = downloaded / (1024 * 1024)
                            print(
                                f"\r  {GREY}Progress: {dl_mb:.0f} / {total_mb:.0f} MB"
                                f" ({pct:.1f}%){RESET}",
                                end="", flush=True,
                            )
                print()  # newline after progress
        except Exception as exc:
            # Clean up partial download
            if zip_file.exists():
                zip_file.unlink()
            error(f"Download failed: {exc}")
            return 1
        elapsed = time.perf_counter() - t0
        ok(f"Downloaded {file_size_mb(zip_file)} MB in {elapsed:.1f}s")
    print()

    # ========================================================================
    # Phase 2 — Extract XTF from ZIP
    # ========================================================================
    banner("Phase 2: Extract XTF")

    xtf_dir = tmp_dir / "xtf"

    # Check if we can skip extraction
    existing_xtfs = sorted(xtf_dir.rglob("*.xtf")) if xtf_dir.exists() else []
    if args.skip_extract and existing_xtfs:
        skip(f"Using existing {len(existing_xtfs)} XTF file(s) in {xtf_dir}")
    else:
        if xtf_dir.exists():
            shutil.rmtree(xtf_dir)

        info("Extracting ZIP (~28 GB uncompressed, this may take a few minutes)...")
        t0 = time.perf_counter()
        with zipfile.ZipFile(str(zip_file), "r") as zf:
            zf.extractall(str(xtf_dir))
        elapsed = time.perf_counter() - t0
        ok(f"Extraction completed in {elapsed:.1f}s")

    # Find .xtf files
    xtf_files = sorted(xtf_dir.rglob("*.xtf"), key=lambda p: p.name)
    if not xtf_files:
        error("No .xtf files found in archive!")
        return 1

    total_xtf_mb = 0.0
    for xf in xtf_files:
        sz = file_size_mb(xf)
        total_xtf_mb += sz
        ok(f"{xf.name} ({sz} MB)")
    info(f"Found {len(xtf_files)} XTF file(s), total {total_xtf_mb:.0f} MB")

    # Set TLM model directory: xtf/ first (contains the .ili shipped with the
    # data, e.g. swissTLM3D_ili2_V2_4.ili), then ressources/ as fallback, then
    # the standard online repository and the JAR bundled models.
    tlm_model_dir = (
        f"{xtf_dir};"
        f"{workspace_root / 'ressources'};"
        f"http://models.interlis.ch/;"
        f"%JAR_DIR"
    )
    info(f"TLM model dir: {tlm_model_dir}")
    print()

    # ========================================================================
    # Phase 2b — Validate XTF with ilivalidator
    # ========================================================================
    banner("Phase 2b: Validate XTF (ilivalidator)")

    validation_log = tmp_dir / "ilivalidator.log"
    validation_xtf_log = tmp_dir / "ilivalidator_errors.xtf"

    if args.skip_validation:
        skip("Validation skipped (--skip-validation)")
    else:
        warn("This may take a long time for large datasets.")
        validation_ok = True
        for xf in xtf_files:
            validation_log = tmp_dir / f"ilivalidator_{xf.stem}.log"
            validation_xtf_log = tmp_dir / f"ilivalidator_{xf.stem}_errors.xtf"

            validator_args = [
                "-Xmx4096m",
                "-jar", str(ilivalidator_jar),
                "--modeldir", tlm_model_dir,
                "--log", str(validation_log),
                "--xtflog", str(validation_xtf_log),
                "--logtime",
                str(xf),
            ]

            t0 = time.perf_counter()
            rc = run_java(validator_args, f"Validating {xf.name}...")
            elapsed = time.perf_counter() - t0

            if rc == 0:
                ok(f"{xf.name} — validation passed in {elapsed:.1f}s")
            else:
                validation_ok = False
                warn(f"{xf.name} — validation errors (exit code {rc})")
                warn(f"  log:    {validation_log}")
                warn(f"  xtflog: {validation_xtf_log}")

        if not validation_ok:
            info("Continuing with import despite validation errors...")
            print(f"  {GREY}The XTF data is from an official swisstopo source; minor model{RESET}")
            print(f"  {GREY}deviations may exist but do not prevent import.{RESET}")
    print()

    # ========================================================================
    # Phase 3 — Create empty DGIF GeoPackage (schema import)
    # ========================================================================
    banner("Phase 3: Create DGIF GeoPackage schema")

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
        "--noSmartMapping",
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
    # Phase 4 — Import XTF into temporary swissTLM3D GeoPackage
    # ========================================================================
    banner("Phase 4: Import XTF into temp GeoPackage")

    if args.skip_import and tlm_gpkg.exists():
        skip(f"Using existing TLM GeoPackage: {tlm_gpkg} ({file_size_mb(tlm_gpkg)} MB)")
    else:
        if tlm_gpkg.exists():
            info(f"Removing existing: {tlm_gpkg}")
            tlm_gpkg.unlink()

        # 4a — Schema import: create the TLM GeoPackage structure from the .ili
        #      shipped with the data (found in xtf_dir)
        tlm_ili_file = list(xtf_dir.glob("*.ili"))
        if not tlm_ili_file:
            error(f"No .ili model file found in {xtf_dir}")
            return 1
        tlm_ili_file = tlm_ili_file[0]
        info(f"TLM model: {tlm_ili_file.name}")

        tlm_schema_log = tmp_dir / "tlm_schemaimport.log"
        tlm_schema_args = [
            "-jar", str(ili2gpkg_jar),
            "--schemaimport",
            "--dbfile", str(tlm_gpkg),
            "--defaultSrsAuth", "EPSG",
            "--defaultSrsCode", "2056",
            "--nameByTopic",
            "--strokeArcs",
            "--createEnumTabs",
            "--createEnumTxtCol",
            "--beautifyEnumDispName",
            "--createBasketCol",
            "--createTidCol",
            "--createStdCols",
            "--modeldir", tlm_model_dir,
            "--log", str(tlm_schema_log),
            str(tlm_ili_file),
        ]

        t0 = time.perf_counter()
        rc = run_java(tlm_schema_args, "ili2gpkg --schemaimport for TLM...")
        if rc != 0:
            error(f"TLM schema import failed! See: {tlm_schema_log}")
            return 1
        elapsed = time.perf_counter() - t0
        ok(f"TLM GeoPackage schema created in {elapsed:.1f}s")

        # 4b — Import each XTF file into the existing schema
        info(f"Importing {len(xtf_files)} XTF file(s) into temp GeoPackage...")
        warn("This may take a long time for large datasets.")
        t0_total = time.perf_counter()

        for i, xf in enumerate(xtf_files, 1):
            info(f"[{i}/{len(xtf_files)}] Importing {xf.name} ({file_size_mb(xf)} MB)...")
            per_file_log = tmp_dir / f"tlm_import_{xf.stem}.log"

            tlm_import_args = [
                "-jar", str(ili2gpkg_jar),
                "--import",
                "--dbfile", str(tlm_gpkg),
                "--defaultSrsAuth", "EPSG",
                "--defaultSrsCode", "2056",
                "--nameByTopic",
                "--strokeArcs",
                "--createEnumTabs",
                "--createEnumTxtCol",
                "--beautifyEnumDispName",
                "--createBasketCol",
                "--createTidCol",
                "--createStdCols",
                "--modeldir", tlm_model_dir,
                "--log", str(per_file_log),
                str(xf),
            ]

            t0 = time.perf_counter()
            rc = run_java(tlm_import_args, f"ili2gpkg --import {xf.name}")
            elapsed = time.perf_counter() - t0

            if rc != 0:
                error(f"Import of {xf.name} failed! See: {per_file_log}")
                return 1
            ok(f"{xf.name} imported in {elapsed:.1f}s")

        total_elapsed = time.perf_counter() - t0_total
        ok(f"TLM GeoPackage created: {file_size_mb(tlm_gpkg)} MB in {total_elapsed:.1f}s")
    print()

    # ========================================================================
    # Phase 5 — Transform and Load (Python)
    # ========================================================================
    banner("Phase 5: Transform & Load (Python ETL)")

    info("Running etl_swisstlm3d_transform.py...")
    t0 = time.perf_counter()

    proc = subprocess.Popen(
        [
            python_exe,
            str(transform_py),
            "--tlm-gpkg", str(tlm_gpkg),
            "--dgif-gpkg", str(dgif_gpkg),
            "--mapping", str(mapping_csv),
        ],
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
    print(f"  {GREEN}Output:   {dgif_gpkg} ({final_size} MB){RESET}")
    print(f"  {GREY}TLM temp: {tlm_gpkg} ({file_size_mb(tlm_gpkg)} MB){RESET}")
    print(f"  {GREY}Logs:     {dgif_schema_log}{RESET}")
    for xf in xtf_files:
        print(f"  {GREY}          {tmp_dir / f'tlm_import_{xf.stem}.log'}{RESET}")
    if not args.skip_validation:
        for xf in xtf_files:
            print(f"  {GREY}          {tmp_dir / f'ilivalidator_{xf.stem}.log'}{RESET}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
