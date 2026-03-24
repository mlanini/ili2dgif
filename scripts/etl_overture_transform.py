#!/usr/bin/env python3
"""
ETL Transform & Load: Overture Maps Parquet → DGIF GeoPackage

Reads features from local GeoParquet files (downloaded externally from
Overture Maps S3) using GDAL's Parquet driver, applies the mapping table
(Overture_to_DGIF_V3.csv), and inserts features into a DGIF-schema
GeoPackage.

Overture data is already in WGS84 (EPSG:4326) — no reprojection needed.
No DuckDB dependency — only GDAL with Parquet driver (QGIS 3.40+ / GDAL 3.6+).

The DGIF GeoPackage uses --smart2Inheritance, so each concrete class table
(e.g. cultural_building) contains ALL inherited columns from Entity and
FeatureEntity (including ageometry, beginlifespanversion, etc.).

Usage:
    python etl_overture_transform.py \\
        --dgif-gpkg output/DGIF_Overture.gpkg \\
        --mapping   models/Overture_to_DGIF_V3.csv \\
        --parquet   buildings/building=C:/tmp/overture_buildings_building.parquet \\
        --parquet   transportation/segment=C:/tmp/overture_transportation_segment.parquet
"""

import argparse
import csv
import datetime
import sqlite3
import struct
import sys
import uuid
from collections import defaultdict
from pathlib import Path

try:
    from osgeo import ogr, osr, gdal
    gdal.UseExceptions()
except ImportError:
    print("[FATAL] GDAL/OGR Python bindings not found. Install via QGIS or pip.",
          file=sys.stderr)
    sys.exit(1)


# ============================================================================
# DGIF class → DGIF topic mapping
# ============================================================================
DGIF_CLASS_TO_TOPIC = {
    # Foundation
    "GeneralLocation": "Foundation",
    # AeronauticalFacility
    "Aerodrome": "AeronauticalFacility",
    "Apron": "AeronauticalFacility",
    "Helipad": "AeronauticalFacility",
    "Heliport": "AeronauticalFacility",
    "LaunchPad": "AeronauticalFacility",
    "Runway": "AeronauticalFacility",
    "Stopway": "AeronauticalFacility",
    "Taxiway": "AeronauticalFacility",
    # Agricultural
    "AllotmentArea": "Agricultural",
    "CropLand": "Agricultural",
    "Farm": "Agricultural",
    "Orchard": "Agricultural",
    "PlantNursery": "Agricultural",
    "Vineyard": "Agricultural",
    # Boundaries
    "AdministrativeBoundary": "Boundaries",
    "AdministrativeDivision": "Boundaries",
    "BoundaryMonument": "Boundaries",
    "ConservationArea": "Boundaries",
    "GeopoliticalEntity": "Boundaries",
    # Cultural
    "AerationBasin": "Cultural",
    "Amenity": "Cultural",
    "AmusementPark": "Cultural",
    "AmusementParkAttraction": "Cultural",
    "Amphitheatre": "Cultural",
    "ArcheologicalSite": "Cultural",
    "Aerial": "Cultural",
    "Barn": "Cultural",
    "Bench": "Cultural",
    "Bollard": "Cultural",
    "Borehole": "Cultural",
    "BotanicGarden": "Cultural",
    "Building": "Cultural",
    "BuildingSuperstructure": "Cultural",
    "Cable": "Cultural",
    "Cableway": "Cultural",
    "Cairn": "Cultural",
    "CampSite": "Cultural",
    "Cemetery": "Cultural",
    "Checkpoint": "Cultural",
    "CompactSurface": "Cultural",
    "Conveyor": "Cultural",
    "CoolingTower": "Cultural",
    "Courtyard": "Cultural",
    "CulturalConservationArea": "Cultural",
    "DisposalSite": "Cultural",
    "EducationalAmenity": "Cultural",
    "ElectricPowerStation": "Cultural",
    "ElectricalPowerGenerator": "Cultural",
    "ExtractionMine": "Cultural",
    "Facility": "Cultural",
    "Fairground": "Cultural",
    "Fence": "Cultural",
    "FireHydrant": "Cultural",
    "FiringRange": "Cultural",
    "Flagpole": "Cultural",
    "Fountain": "Cultural",
    "GolfCourse": "Cultural",
    "GolfDrivingRange": "Cultural",
    "GrainStorageStructure": "Cultural",
    "Grandstand": "Cultural",
    "Greenhouse": "Cultural",
    "Hedgerow": "Cultural",
    "Hut": "Cultural",
    "IndustrialPark": "Cultural",
    "Installation": "Cultural",
    "InterestSite": "Cultural",
    "Lookout": "Cultural",
    "Market": "Cultural",
    "MedicalAmenity": "Cultural",
    "Monument": "Cultural",
    "NonBuildingStructure": "Cultural",
    "OverheadObstruction": "Cultural",
    "Park": "Cultural",
    "ParkingGarage": "Cultural",
    "PicnicSite": "Cultural",
    "PicnicTable": "Cultural",
    "PowerSubstation": "Cultural",
    "PublicSquare": "Cultural",
    "Pylon": "Cultural",
    "Racetrack": "Cultural",
    "RadarStation": "Cultural",
    "Ramp": "Cultural",
    "RecyclingSite": "Cultural",
    "Ruins": "Cultural",
    "SaltEvaporator": "Cultural",
    "SettlingPond": "Cultural",
    "SewageTreatmentPlant": "Cultural",
    "Shed": "Cultural",
    "ShoppingComplex": "Cultural",
    "SkiJump": "Cultural",
    "SkiRun": "Cultural",
    "Smokestack": "Cultural",
    "Spa": "Cultural",
    "SportsGround": "Cultural",
    "Stable": "Cultural",
    "Stadium": "Cultural",
    "StorageTank": "Cultural",
    "StreetLamp": "Cultural",
    "SurfaceBunker": "Cultural",
    "SwimmingPool": "Cultural",
    "Tower": "Cultural",
    "TrainingSite": "Cultural",
    "UndergroundDwelling": "Cultural",
    "UtilityCover": "Cultural",
    "VehicleLot": "Cultural",
    "Wall": "Cultural",
    "WaterTower": "Cultural",
    "Waterwork": "Cultural",
    "WindTurbine": "Cultural",
    "Zoo": "Cultural",
    # Elevation
    "GeomorphicExtreme": "Elevation",
    # HydrographicAidsNavigation
    "Reef": "HydrographicAidsNavigation",
    "ShorelineConstruction": "HydrographicAidsNavigation",
    # InlandWater
    "Canal": "Transportation",
    "Dam": "InlandWater",
    "Ditch": "InlandWater",
    "Embankment": "Physiography",
    "FishFarmFacility": "InlandWater",
    "InlandWaterbody": "InlandWater",
    "InundatedLand": "InlandWater",
    "Lock": "InlandWater",
    "Rapids": "InlandWater",
    "River": "InlandWater",
    "Spring": "InlandWater",
    "Waterfall": "InlandWater",
    "Wetland": "InlandWater",
    # MilitaryInstallationsDefensiveStructures
    "Fortification": "MilitaryInstallationsDefensiveStructures",
    "MilitaryInstallation": "MilitaryInstallationsDefensiveStructures",
    # OceanEnvironment
    "TidalWater": "OceanEnvironment",
    # Physiography
    "Glacier": "Physiography",
    "Hill": "Physiography",
    "LandArea": "Physiography",
    "LandMorphologyArea": "Physiography",
    "MountainPass": "Physiography",
    "PermanentSnowIce": "Physiography",
    "RidgeLine": "Physiography",
    "RockFormation": "Physiography",
    "SandDunes": "Physiography",
    "SoilSurfaceRegion": "Physiography",
    "SteepTerrainFace": "Physiography",
    "Volcano": "Physiography",
    # Population
    "BuiltUpArea": "Population",
    "Neighbourhood": "Population",
    "PopulatedPlace": "Population",
    # PortsHarbours
    "FerryStation": "PortsHarbours",
    "Harbour": "PortsHarbours",
    "SmallCraftFacility": "PortsHarbours",
    # Vegetation
    "Forest": "Vegetation",
    "Grassland": "Vegetation",
    "LoggingSite": "Vegetation",
    "Scrubland": "Vegetation",
    "ShrubLand": "Vegetation",
    "Tree": "Vegetation",
    "Tundra": "Vegetation",
    "UnvegetatedLand": "Vegetation",
    # Transportation
    "AircraftHangar": "Transportation",
    "Bridge": "Transportation",
    "BridgePier": "Transportation",
    "ConstructionZone": "Transportation",
    "Crossing": "Transportation",
    "Curb": "Transportation",
    "EntranceExit": "Transportation",
    "FerryCrossing": "Transportation",
    "Gate": "Transportation",
    "LandRoute": "Transportation",
    "LandTransportationWay": "Transportation",
    "MotorVehicleStation": "Transportation",
    "Pipeline": "Transportation",
    "Railway": "Transportation",
    "RailwayYard": "Transportation",
    "RoadInterchange": "Transportation",
    "Stair": "Transportation",
    "TransportationPlatform": "Transportation",
    "TransportationStation": "Transportation",
    "Tunnel": "Transportation",
    "VehicleBarrier": "Transportation",
}


# ============================================================================
# Mapping row data class
# ============================================================================
class MappingRow:
    """One row from Overture_to_DGIF_V3.csv"""
    __slots__ = (
        "no", "theme", "otype", "subtype", "oclass",
        "geometry_type", "description",
        "dgif_class", "dgif_code",
        "dgif_attr1", "dgif_attr1_code", "dgif_val1", "dgif_val1_code",
        "dgif_attr2", "dgif_attr2_code", "dgif_val2", "dgif_val2_code",
    )

    def __init__(self, row: list[str]):
        self.no = row[0]
        self.theme = row[1]
        self.otype = row[2]
        self.subtype = row[3]
        self.oclass = row[4]
        self.geometry_type = row[5]
        self.description = row[6]
        self.dgif_class = row[7] if len(row) > 7 else ""
        self.dgif_code = row[8] if len(row) > 8 else ""
        self.dgif_attr1 = row[9] if len(row) > 9 else ""
        self.dgif_attr1_code = row[10] if len(row) > 10 else ""
        self.dgif_val1 = row[11] if len(row) > 11 else ""
        self.dgif_val1_code = row[12] if len(row) > 12 else ""
        self.dgif_attr2 = row[13] if len(row) > 13 else ""
        self.dgif_attr2_code = row[14] if len(row) > 14 else ""
        self.dgif_val2 = row[15] if len(row) > 15 else ""
        self.dgif_val2_code = row[16] if len(row) > 16 else ""

    @property
    def is_mapped(self) -> bool:
        return bool(self.dgif_class) and self.description != "not in DGIF"


# ============================================================================
# Load mapping CSV
# ============================================================================
def load_mapping(csv_path: str) -> dict[tuple[str, str, str, str], list[MappingRow]]:
    """
    Returns dict keyed by (theme, type, subtype, class) → list[MappingRow].
    """
    mapping: dict[tuple[str, str, str, str], list[MappingRow]] = defaultdict(list)
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader)  # skip header
        for row in reader:
            if not row or not row[0].strip():
                continue
            mr = MappingRow(row)
            if mr.is_mapped:
                mapping[(mr.theme, mr.otype, mr.subtype, mr.oclass)].append(mr)
    return mapping


def find_mapping_rules(
    mapping: dict[tuple[str, str, str, str], list[MappingRow]],
    theme: str,
    otype: str,
    subtype: str | None,
    oclass: str | None,
) -> list[MappingRow] | None:
    """
    Find matching mapping rules with fallback:
    1. Exact match (theme, type, subtype, class)
    2. Match with class only (theme, type, "", class)
    3. Match with subtype only (theme, type, subtype, "")
    4. Generic match (theme, type, "", "")
    """
    sub = subtype or ""
    cls = oclass or ""

    # Exact match
    rules = mapping.get((theme, otype, sub, cls))
    if rules:
        return rules

    # Match with class only (no subtype)
    if sub:
        rules = mapping.get((theme, otype, "", cls))
        if rules:
            return rules

    # Match with subtype only (no class)
    if cls:
        rules = mapping.get((theme, otype, sub, ""))
        if rules:
            return rules

    # Generic match
    rules = mapping.get((theme, otype, "", ""))
    return rules


# ============================================================================
# Build ili2db class metadata from DGIF GeoPackage
# ============================================================================
def build_class_metadata(conn: sqlite3.Connection) -> dict:
    """Build metadata dict for DGIF classes from ili2db system tables."""
    cur = conn.cursor()
    cur.execute("SELECT iliname, sqlname FROM T_ILI2DB_CLASSNAME")
    classname_rows = cur.fetchall()

    cur.execute("SELECT table_name FROM gpkg_contents WHERE data_type IN ('features','attributes')")
    all_tables = {row[0] for row in cur.fetchall()}

    always_provided = {"t_id", "t_basket", "t_lastchange", "t_createdate", "t_user"}

    cur.execute("SELECT table_name, geometry_type_name FROM gpkg_geometry_columns")
    table_geom_type = {row[0]: row[1].upper() for row in cur.fetchall()}

    meta = {}
    for iliname, sqlname in classname_rows:
        if sqlname not in all_tables:
            continue
        parts = iliname.split(".")
        if len(parts) >= 3:
            class_name = parts[-1]
            topic_name = parts[-2]
            col_cur = conn.execute(f'PRAGMA table_info("{sqlname}")')
            columns = set()
            notnull_defaults = {}
            for row in col_cur.fetchall():
                col_name = row[1].lower()
                col_type = (row[2] or "").upper()
                is_notnull = bool(row[3])
                columns.add(col_name)
                if is_notnull and col_name not in always_provided:
                    if "INT" in col_type:
                        notnull_defaults[col_name] = 0
                    elif "DOUBLE" in col_type or "REAL" in col_type or "FLOAT" in col_type:
                        notnull_defaults[col_name] = 0.0
                    elif "BOOL" in col_type:
                        notnull_defaults[col_name] = False
                    else:
                        notnull_defaults[col_name] = "unknown"
            meta[class_name] = {
                "iliname": iliname,
                "sqlname": sqlname,
                "topic": topic_name,
                "columns": columns,
                "notnull_defaults": notnull_defaults,
                "geom_type": table_geom_type.get(sqlname, ""),
            }
    return meta


# ============================================================================
# Ensure dataset and baskets exist
# ============================================================================
def ensure_baskets(conn: sqlite3.Connection, topics_needed: set[str]) -> dict[str, int]:
    """Create a dataset and one basket per DGIF topic."""
    cur = conn.cursor()
    cur.execute("SELECT T_Id FROM T_ILI2DB_DATASET LIMIT 1")
    row = cur.fetchone()
    if row:
        dataset_id = row[0]
    else:
        cur.execute("INSERT INTO T_ILI2DB_DATASET (datasetName) VALUES (?)", ("overture_import",))
        dataset_id = cur.lastrowid

    basket_map = {}
    for topic_ili in sorted(topics_needed):
        cur.execute("SELECT T_Id FROM T_ILI2DB_BASKET WHERE topic=?", (topic_ili,))
        row = cur.fetchone()
        if row:
            basket_map[topic_ili] = row[0]
        else:
            basket_tid = str(uuid.uuid4())
            cur.execute(
                "INSERT INTO T_ILI2DB_BASKET (dataset, topic, T_Ili_Tid, attachmentKey) VALUES (?,?,?,?)",
                (dataset_id, topic_ili, basket_tid, "overture_import")
            )
            basket_map[topic_ili] = cur.lastrowid

    conn.commit()
    return basket_map


# ============================================================================
# GeoPackage WKB helpers
# ============================================================================
def to_gpkg_wkb(geom: ogr.Geometry, srs_id: int = 4326) -> bytes:
    """Convert OGR geometry to GeoPackage binary (GP header + WKB)."""
    if geom is None:
        return None
    wkb = geom.ExportToWkb(ogr.wkbNDR)
    envelope = geom.GetEnvelope()
    flags = 0x02 | 0x01
    header = struct.pack(
        '<2sBBi4d',
        b'GP', 0, flags, srs_id,
        envelope[0], envelope[1], envelope[2], envelope[3],
    )
    return header + wkb


def wkb_to_ogr(wkb_bytes: bytes) -> ogr.Geometry | None:
    """Convert raw WKB bytes to OGR geometry."""
    if wkb_bytes is None:
        return None
    try:
        geom = ogr.CreateGeometryFromWkb(bytes(wkb_bytes))
        return geom
    except Exception:
        return None


# ============================================================================
# Extract Overture feature attributes
# ============================================================================
def get_overture_subtype(row: dict) -> str | None:
    """Extract subtype from an Overture feature row."""
    return row.get("subtype") or row.get("subType") or None


def get_overture_class(row: dict) -> str | None:
    """Extract class from an Overture feature row."""
    return row.get("class") or None


def get_overture_name(row: dict) -> str | None:
    """Extract primary name from Overture names struct.

    Overture names is a struct with 'primary' as the main name.
    DuckDB exports this as a dict or None.
    """
    names = row.get("names")
    if names is None:
        return None
    if isinstance(names, dict):
        primary = names.get("primary")
        if primary:
            return str(primary)
    return None


def get_places_category(row: dict) -> str | None:
    """Extract the primary category from a Places row.

    Overture Places has a 'categories' struct with 'primary' and 'alternate'.
    """
    cats = row.get("categories")
    if cats is None:
        return None
    if isinstance(cats, dict):
        primary = cats.get("primary")
        if primary:
            return str(primary)
    return None


def _extract_nested_primary(raw_value) -> str | None:
    """Extract 'primary' from a nested struct that GDAL may expose as:
    - a dict (Arrow struct → Python dict)
    - a JSON string '{"primary": "xxx", ...}'
    - a list/tuple where first element is the primary value
    """
    if raw_value is None:
        return None
    if isinstance(raw_value, dict):
        v = raw_value.get("primary")
        return str(v) if v else None
    if isinstance(raw_value, str):
        # Try JSON parse
        s = raw_value.strip()
        if s.startswith("{"):
            try:
                import json
                d = json.loads(s)
                v = d.get("primary")
                return str(v) if v else None
            except (json.JSONDecodeError, ValueError):
                pass
        # Maybe it's just a plain string value
        return s if s else None
    if isinstance(raw_value, (list, tuple)) and len(raw_value) > 0:
        return str(raw_value[0]) if raw_value[0] else None
    return None


# ============================================================================
# Foundation metadata for Overture
# ============================================================================
def populate_foundation_metadata(
    conn: sqlite3.Connection,
    basket_tid: int,
    start_tid: int,
) -> int:
    """Insert Foundation metadata records for Overture Maps."""
    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    user = "etl_overture"
    tid = start_tid

    def _base(extra: dict | None = None) -> dict:
        nonlocal tid
        row = {
            "T_Id": tid,
            "T_basket": basket_tid,
            "T_Ili_Tid": str(uuid.uuid4()),
            "T_LastChange": now_iso,
            "T_CreateDate": now_iso,
            "T_User": user,
        }
        if extra:
            row.update(extra)
        tid += 1
        return row

    def _entity(extra: dict | None = None) -> dict:
        row = _base({
            "beginlifespanversion": now_iso,
            "uniqueuniversalentityidentifier": str(uuid.uuid4()),
        })
        if extra:
            row.update(extra)
        return row

    def _insert(table: str, row: dict) -> int:
        cols = ", ".join(row.keys())
        placeholders = ", ".join(["?"] * len(row))
        conn.execute(
            f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})',
            list(row.values()),
        )
        return row["T_Id"]

    print("\n[INFO] Populating Foundation metadata (Overture Maps -> DGIF) ...")

    # SourceInfo
    src_tid = _insert("foundation_sourceinfo", _base({
        "datasetcitation": (
            "Overture Maps Foundation - Open map data from Meta, Microsoft, "
            "Esri, TomTom, and the OpenStreetMap community. "
            "https://overturemaps.org"
        ),
        "sourcedescription": (
            "Overture Maps is a collaborative effort to develop interoperable "
            "open map data. Data covers buildings, transportation, places, "
            "base features, divisions, and addresses globally."
        ),
        "sourceidentifier": "overturemaps.org",
        "typeofsource": "vectorDataset",
        "resourcecontentorigin": "Overture Maps Foundation",
        "scaledenominator": 0,
        "sourcecurrencydatetime": datetime.datetime.now().strftime("%Y"),
    }))
    print(f"  [OK] foundation_sourceinfo          T_Id={src_tid}")

    # Organisation
    org_tid = _insert("foundation_organisation", _entity({
        "organisationdescription": "Overture Maps Foundation (Linux Foundation)",
        "organisationtype": "nonGovernmentalOrganisation",
        "homegeopoliticalentity": "USA",
        "organisationreach": "global",
        "branding": "Overture Maps",
    }))
    print(f"  [OK] foundation_organisation         T_Id={org_tid}")

    # ContactInfo
    contact_tid = _insert("foundation_contactinfo", _base({
        "addresscountry": "USA",
        "addresscity": "San Francisco",
        "addressdeliverypoint": "",
        "addresspostalcode": "",
        "addressadministrativearea": "CA",
        "addresselectronicmail": "info@overturemaps.org",
        "telephonevoice": "",
        "onlineresourcelinkage": "https://overturemaps.org",
    }))
    print(f"  [OK] foundation_contactinfo          T_Id={contact_tid}")

    # OrganisationalUnit
    orgunit_tid = _insert("foundation_organisationalunit", _entity({
        "contactinfo": (
            "Overture Maps Foundation, San Francisco CA, USA; "
            "email: info@overturemaps.org; web: https://overturemaps.org"
        ),
        "mainorganisation": org_tid,
    }))
    print(f"  [OK] foundation_organisationalunit   T_Id={orgunit_tid}")

    # RestrictionInfo
    restr_tid = _insert("foundation_restrictioninfo", _base({
        "commercialcopyrightnotice": "© Overture Maps Foundation",
        "commercialdistribrestrict": "ODbL / CDLA Permissive 2.0",
    }))
    print(f"  [OK] foundation_restrictioninfo      T_Id={restr_tid}")

    # HorizCoordMetadata
    hcoord_tid = _insert("foundation_horizcoordmetadata", _base({
        "geodeticdatum": "worldGeodeticSystem1984",
        "horizaccuracycategory": "Varies by source (OSM, ML-derived, commercial)",
    }))
    print(f"  [OK] foundation_horizcoordmetadata   T_Id={hcoord_tid}")

    # FeatureMetadata
    fmeta_tid = _insert("foundation_featuremetadata", _base({
        "delineationknown": 1,
        "delineationknown_txt": "true",
        "existencecertaintycat": "definite",
        "surveycoveragecategory": "complete",
        "dataqualitystatement": (
            "Overture Maps quality varies by source. Buildings primarily from "
            "Microsoft ML + OSM. Roads from TomTom + OSM. Places from Meta + "
            "Microsoft. Geometric accuracy depends on source imagery resolution."
        ),
    }))
    print(f"  [OK] foundation_featuremetadata      T_Id={fmeta_tid}")

    # FeatureAttMetadata
    fameta_tid = _insert("foundation_featureattmetadata", _base({
        "currencydatetime": datetime.datetime.now().strftime("%Y"),
        "dataqualitystatement": (
            "Attribute quality varies by source. Classification derived from "
            "OSM tags, commercial data (TomTom, Meta), and ML inference."
        ),
    }))
    print(f"  [OK] foundation_featureattmetadata   T_Id={fameta_tid}")

    # NameSpecification
    name_tid = _insert("foundation_namespecification", _base({
        "aname": "Overture Maps",
        "nametype": "official",
        "nameusedescription": "Overture Maps Foundation open data release",
        "referencename": 1,
        "referencename_txt": "true",
    }))
    print(f"  [OK] foundation_namespecification    T_Id={name_tid}")

    conn.commit()
    inserted = tid - start_tid
    print(f"[INFO] Foundation metadata: {inserted} records inserted "
          f"(T_Id {start_tid}..{tid - 1})")
    return tid


# ============================================================================
# Main transform
# ============================================================================
def transform(
    dgif_gpkg_path: str,
    mapping_csv_path: str,
    parquet_inputs: list[tuple[str, str, str]],  # [(theme, type, path), ...]
):
    print("[INFO] Loading mapping table...")
    mapping = load_mapping(mapping_csv_path)
    print(f"[INFO] Loaded {sum(len(v) for v in mapping.values())} mapping rules for "
          f"{len(mapping)} (theme/type/subtype/class) combinations")

    # Open DGIF GeoPackage via sqlite3
    dgif_conn = sqlite3.connect(dgif_gpkg_path)
    dgif_conn.execute("PRAGMA journal_mode=WAL")
    dgif_conn.execute("PRAGMA synchronous=NORMAL")
    dgif_conn.execute("PRAGMA cache_size=-64000")
    dgif_conn.execute("PRAGMA foreign_keys=OFF")

    # Drop rtree triggers (SpatiaLite functions not available in plain sqlite3)
    rtree_triggers = dgif_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' "
        "AND (sql LIKE '%ST_IsEmpty%' OR sql LIKE '%ST_MinX%')"
    ).fetchall()
    if rtree_triggers:
        print(f"[INFO] Dropping {len(rtree_triggers)} rtree triggers...")
        for (tname,) in rtree_triggers:
            dgif_conn.execute(f'DROP TRIGGER IF EXISTS "{tname}"')
        dgif_conn.commit()

    print("[INFO] Building DGIF class metadata from ili2db tables...")
    class_meta = build_class_metadata(dgif_conn)
    print(f"[INFO] Found {len(class_meta)} DGIF classes")

    # Collect which DGIF topics are needed for baskets
    topics_needed = set()
    for key, rules in mapping.items():
        for mr in rules:
            if mr.dgif_class in class_meta:
                meta = class_meta[mr.dgif_class]
                topics_needed.add(f"DGIF_V3.{meta['topic']}")
            elif mr.dgif_class in DGIF_CLASS_TO_TOPIC:
                topics_needed.add(f"DGIF_V3.{DGIF_CLASS_TO_TOPIC[mr.dgif_class]}")
    # Always include Foundation
    topics_needed.add("DGIF_V3.Foundation")

    print(f"[INFO] Creating baskets for {len(topics_needed)} topics...")
    basket_map = ensure_baskets(dgif_conn, topics_needed)

    # T_Id counter — start at 1
    next_tid = 1

    # Statistics
    stats = defaultdict(int)
    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Track spatial extent per table
    table_extents: dict[str, list[float]] = {}
    actual_geom_types: dict[str, set[str]] = defaultdict(set)

    _OGR_TYPE_NAMES = {
        ogr.wkbPoint: "POINT",
        ogr.wkbLineString: "LINESTRING",
        ogr.wkbPolygon: "POLYGON",
        ogr.wkbMultiPoint: "MULTIPOINT",
        ogr.wkbMultiLineString: "MULTILINESTRING",
        ogr.wkbMultiPolygon: "MULTIPOLYGON",
    }

    # ========================================================================
    # Process each data file via GDAL OGR (Parquet / GeoJSON)
    # ========================================================================
    print(f"\n[INFO] Processing {len(parquet_inputs)} data files...")
    print("=" * 60)

    for theme, otype, parquet_path in parquet_inputs:
        print(f"\n  [{theme}/{otype}] Reading {parquet_path}...")

        # Open via GDAL OGR (auto-detects Parquet, GeoJSON, etc.)
        ds = ogr.Open(parquet_path)
        if ds is None:
            print(f"  [ERROR] Cannot open {parquet_path} (GDAL OGR)")
            continue

        lyr = ds.GetLayer(0)
        if lyr is None:
            print(f"  [ERROR] No layer in {parquet_path}")
            ds = None
            continue

        count = lyr.GetFeatureCount()
        print(f"  [{theme}/{otype}] {count:,} features")

        if count == 0:
            ds = None
            continue

        # Discover available field names
        lyr_defn = lyr.GetLayerDefn()
        field_names = set()
        for i in range(lyr_defn.GetFieldCount()):
            field_names.add(lyr_defn.GetFieldDefn(i).GetName().lower())

        # Check for key Overture attribute fields
        has_id = "id" in field_names
        has_subtype = "subtype" in field_names
        has_class = "class" in field_names
        has_names = "names" in field_names
        has_categories = "categories" in field_names

        theme_inserted = 0
        theme_skipped = 0
        theme_no_match = 0

        lyr.ResetReading()
        feat = lyr.GetNextFeature()
        while feat is not None:
            # Extract Overture classification
            subtype = ""
            oclass = ""

            if has_subtype:
                val = feat.GetField("subtype")
                if val:
                    subtype = str(val)

            if has_class:
                val = feat.GetField("class")
                if val:
                    oclass = str(val)

            # For Places, try to extract primary category
            # GDAL may expose nested structs as JSON strings
            if theme == "places" and not oclass and has_categories:
                raw = feat.GetField("categories")
                if raw:
                    cat_primary = _extract_nested_primary(raw)
                    if cat_primary:
                        oclass = cat_primary

            # Find mapping rules
            rules = find_mapping_rules(mapping, theme, otype, subtype, oclass)

            if rules is None:
                theme_no_match += 1
                feat = lyr.GetNextFeature()
                continue

            # Get geometry from OGR
            src_geom = feat.GetGeometryRef()

            # Get Overture ID
            overture_id = ""
            if has_id:
                val = feat.GetField("id")
                if val:
                    overture_id = str(val)
            if not overture_id:
                overture_id = str(uuid.uuid4())

            # Apply each mapping rule
            for mr in rules:
                dgif_class = mr.dgif_class

                if dgif_class not in class_meta:
                    stats["dgif_class_not_found"] += 1
                    continue

                meta = class_meta[dgif_class]
                dgif_table_name = meta["sqlname"]
                dgif_cols = meta["columns"]
                dgif_topic = meta["topic"]

                topic_key = f"DGIF_V3.{dgif_topic}"
                basket_id = basket_map.get(topic_key)
                if basket_id is None:
                    stats["dgif_basket_not_found"] += 1
                    continue

                tid = next_tid
                next_tid += 1

                ili_tid = overture_id
                entity_uuid = overture_id

                # Geometry handling
                dgif_geom_type = meta.get("geom_type", "")
                geom_gpkg_wkb = None

                if src_geom is not None:
                    target_geom = src_geom.Clone()
                    target_geom.FlattenTo2D()

                    src_flat = ogr.GT_Flatten(target_geom.GetGeometryType())

                    # If target expects POINT but source is not → centroid
                    if dgif_geom_type == "POINT" and src_flat != ogr.wkbPoint:
                        target_geom = target_geom.Centroid()

                    if target_geom is not None:
                        geom_gpkg_wkb = to_gpkg_wkb(target_geom, srs_id=4326)

                        written_flat = ogr.GT_Flatten(target_geom.GetGeometryType())
                        written_name = _OGR_TYPE_NAMES.get(written_flat, f"UNKNOWN({written_flat})")
                        actual_geom_types[dgif_table_name].add(written_name)

                        env = target_geom.GetEnvelope()
                        if dgif_table_name not in table_extents:
                            table_extents[dgif_table_name] = [env[0], env[2], env[1], env[3]]
                        else:
                            te = table_extents[dgif_table_name]
                            if env[0] < te[0]: te[0] = env[0]
                            if env[2] < te[1]: te[1] = env[2]
                            if env[1] > te[2]: te[2] = env[1]
                            if env[3] > te[3]: te[3] = env[3]

                # Build INSERT columns
                has_geom_col = "ageometry" in dgif_cols

                insert_cols = [
                    "T_Id", "T_Ili_Tid", "T_basket",
                    "beginlifespanversion", "uniqueuniversalentityidentifier",
                    "T_LastChange", "T_CreateDate", "T_User",
                ]
                insert_vals: list = [
                    tid, ili_tid, basket_id,
                    now_iso, entity_uuid,
                    now_iso, now_iso, "etl_overture",
                ]

                if has_geom_col:
                    insert_cols.insert(3, "ageometry")
                    insert_vals.insert(3, geom_gpkg_wkb)

                # Map DGIF attributes from CSV
                if mr.dgif_attr1 and mr.dgif_val1:
                    attr_lower = mr.dgif_attr1.lower()
                    if attr_lower in dgif_cols:
                        insert_cols.append(mr.dgif_attr1)
                        insert_vals.append(mr.dgif_val1)

                if mr.dgif_attr2 and mr.dgif_val2:
                    attr_lower = mr.dgif_attr2.lower()
                    if attr_lower in dgif_cols:
                        insert_cols.append(mr.dgif_attr2)
                        insert_vals.append(mr.dgif_val2)

                # Fill NOT NULL columns with defaults
                notnull_defs = meta.get("notnull_defaults", {})
                already_set = {c.lower() for c in insert_cols}
                for nn_col, nn_default in notnull_defs.items():
                    if nn_col not in already_set:
                        insert_cols.append(nn_col)
                        insert_vals.append(nn_default)

                col_str = ", ".join(f'"{c}"' for c in insert_cols)
                placeholders = ", ".join(["?"] * len(insert_vals))
                sql = f'INSERT INTO "{dgif_table_name}" ({col_str}) VALUES ({placeholders})'

                try:
                    dgif_conn.execute(sql, insert_vals)
                    theme_inserted += 1
                    stats[f"inserted:{dgif_table_name}"] += 1
                except sqlite3.Error as e:
                    if stats["insert_error"] < 5:
                        print(f"  [DEBUG] Insert error: {e}", file=sys.stderr)
                        print(f"  [DEBUG]   table={dgif_table_name}", file=sys.stderr)
                    stats["insert_error"] += 1
                    theme_skipped += 1

            feat = lyr.GetNextFeature()

        ds = None  # close GDAL dataset

        stats["total_inserted"] += theme_inserted
        stats["total_skipped"] += theme_skipped
        stats["total_no_match"] += theme_no_match

        print(f"    -> Inserted: {theme_inserted:,}  |  Skipped: {theme_skipped:,}  |  No match: {theme_no_match:,}")

    # Commit
    print("\n[INFO] Committing to DGIF GeoPackage...")
    dgif_conn.commit()

    # Foundation metadata
    foundation_basket = basket_map.get("DGIF_V3.Foundation")
    if foundation_basket is None:
        foundation_basket = ensure_baskets(dgif_conn, {"DGIF_V3.Foundation"}).get(
            "DGIF_V3.Foundation"
        )
    if foundation_basket is not None:
        next_tid = populate_foundation_metadata(dgif_conn, foundation_basket, next_tid)

    # Update gpkg_contents extents
    print("[INFO] Updating spatial extents...")
    if table_extents:
        try:
            updated_count = 0
            for tbl, (minx, miny, maxx, maxy) in table_extents.items():
                dgif_conn.execute(
                    "UPDATE gpkg_contents SET min_x=?, min_y=?, max_x=?, max_y=? "
                    "WHERE table_name=?",
                    (minx, miny, maxx, maxy, tbl)
                )
                updated_count += dgif_conn.execute("SELECT changes()").fetchone()[0]
            dgif_conn.commit()
            all_minx = min(e[0] for e in table_extents.values())
            all_miny = min(e[1] for e in table_extents.values())
            all_maxx = max(e[2] for e in table_extents.values())
            all_maxy = max(e[3] for e in table_extents.values())
            print(f"[INFO]   Overall extent: ({all_minx:.6f}, {all_miny:.6f}) - "
                  f"({all_maxx:.6f}, {all_maxy:.6f})")
            print(f"[INFO]   Updated {updated_count} gpkg_contents rows")
        except sqlite3.Error as e:
            print(f"[WARN] Could not update extents: {e}", file=sys.stderr)

    # Null-out extents for empty feature tables
    try:
        nulled = 0
        feat_tables = [
            r[0] for r in dgif_conn.execute(
                "SELECT table_name FROM gpkg_contents WHERE data_type='features'"
            ).fetchall()
        ]
        inserted_set = set(
            k.split(":", 1)[1] for k in stats if k.startswith("inserted:")
        )
        for tbl in feat_tables:
            if tbl not in inserted_set:
                dgif_conn.execute(
                    "UPDATE gpkg_contents SET min_x=NULL, min_y=NULL, "
                    "max_x=NULL, max_y=NULL WHERE table_name=?", (tbl,)
                )
                nulled += 1
        if nulled:
            dgif_conn.commit()
            print(f"[INFO]   Nulled extent for {nulled} empty tables")
    except sqlite3.Error as e:
        print(f"[WARN] Could not null empty extents: {e}", file=sys.stderr)

    # Reconcile gpkg_geometry_columns
    print("[INFO] Reconciling gpkg_geometry_columns with actual geometry types...")
    geom_type_updates = 0
    for tbl, written_types in actual_geom_types.items():
        if not written_types:
            continue
        new_type = next(iter(written_types)) if len(written_types) == 1 else "GEOMETRY"
        row = dgif_conn.execute(
            "SELECT geometry_type_name FROM gpkg_geometry_columns WHERE table_name=?",
            (tbl,)
        ).fetchone()
        if row and row[0] != new_type:
            dgif_conn.execute(
                "UPDATE gpkg_geometry_columns SET geometry_type_name=? WHERE table_name=?",
                (new_type, tbl)
            )
            geom_type_updates += 1
    dgif_conn.commit()
    print(f"[INFO]   Updated geometry_type_name for {geom_type_updates} tables")

    # Populate R-tree spatial indexes
    print("[INFO] Populating R-tree spatial indexes...")
    rtree_total = 0
    rtree_tables = 0
    geom_col_rows = dgif_conn.execute(
        "SELECT table_name, column_name FROM gpkg_geometry_columns"
    ).fetchall()
    for tbl, geom_col in geom_col_rows:
        rtree_name = f"rtree_{tbl}_{geom_col}"
        exists = dgif_conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (rtree_name,)
        ).fetchone()
        if not exists:
            continue
        cnt = dgif_conn.execute(
            f'SELECT COUNT(*) FROM "{tbl}" WHERE "{geom_col}" IS NOT NULL'
        ).fetchone()[0]
        if cnt == 0:
            continue
        dgif_conn.execute(f'DELETE FROM "{rtree_name}"')
        inserted = 0
        for (rowid, blob) in dgif_conn.execute(
            f'SELECT T_Id, "{geom_col}" FROM "{tbl}" WHERE "{geom_col}" IS NOT NULL'
        ):
            if blob is None or len(blob) < 40:
                continue
            flags = blob[3]
            envelope_type = (flags >> 1) & 0x07
            if envelope_type == 0:
                continue
            minx, maxx, miny, maxy = struct.unpack_from('<4d', blob, 8)
            dgif_conn.execute(
                f'INSERT INTO "{rtree_name}" (id, minx, maxx, miny, maxy) '
                f'VALUES (?, ?, ?, ?, ?)',
                (rowid, minx, maxx, miny, maxy)
            )
            inserted += 1
        rtree_total += inserted
        rtree_tables += 1
    dgif_conn.commit()
    print(f"[INFO]   Indexed {rtree_total} geometries across {rtree_tables} tables")

    # Clean up
    dgif_conn.close()

    # Report
    print("\n" + "=" * 60)
    print("  ETL Transform Summary (Overture Maps -> DGIF)")
    print("=" * 60)
    print(f"  Total features inserted : {stats['total_inserted']:,}")
    print(f"  Total features skipped  : {stats['total_skipped']:,}")
    print(f"  No mapping match        : {stats['total_no_match']:,}")
    print(f"  DGIF class not found    : {stats.get('dgif_class_not_found', 0):,}")
    print(f"  DGIF basket not found   : {stats.get('dgif_basket_not_found', 0):,}")
    print(f"  Insert errors           : {stats.get('insert_error', 0):,}")

    print("\n  Features per DGIF table:")
    for k, v in sorted(stats.items()):
        if k.startswith("inserted:"):
            table = k.split(":", 1)[1]
            print(f"    {table:<45} {v:>8,}")

    print("=" * 60)
    return 0 if stats["total_inserted"] > 0 else 1


# ============================================================================
# CLI
# ============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="ETL Transform: Overture Maps data -> DGIF GeoPackage"
    )
    parser.add_argument("--dgif-gpkg", required=True, help="Path to target DGIF GeoPackage")
    parser.add_argument("--mapping", required=True, help="Path to Overture_to_DGIF_V3.csv")
    parser.add_argument(
        "--parquet", action="append", required=True,
        help="theme/type=path pairs, e.g. buildings/building=C:/tmp/data.parquet "
             "(also accepts .geojson, .json)"
    )
    args = parser.parse_args()

    # Validate paths
    if not Path(args.dgif_gpkg).exists():
        print(f"[FATAL] DGIF GPKG not found: {args.dgif_gpkg}", file=sys.stderr)
        sys.exit(1)
    if not Path(args.mapping).exists():
        print(f"[FATAL] Mapping CSV not found: {args.mapping}", file=sys.stderr)
        sys.exit(1)

    # Parse --parquet arguments: "theme/type=path"
    parquet_inputs = []
    for entry in args.parquet:
        if "=" not in entry:
            print(f"[FATAL] Invalid --parquet format: {entry}", file=sys.stderr)
            print("  Expected: theme/type=path/to/file.parquet (or .geojson)", file=sys.stderr)
            sys.exit(1)
        key, path = entry.split("=", 1)
        if "/" not in key:
            print(f"[FATAL] Invalid --parquet key: {key}", file=sys.stderr)
            print("  Expected: theme/type", file=sys.stderr)
            sys.exit(1)
        theme, otype = key.split("/", 1)
        if not Path(path).exists():
            print(f"[FATAL] Data file not found: {path}", file=sys.stderr)
            sys.exit(1)
        parquet_inputs.append((theme, otype, path))

    print(f"[INFO] Data inputs: {len(parquet_inputs)}")
    for theme, otype, path in parquet_inputs:
        print(f"  {theme}/{otype} = {path}")

    rc = transform(args.dgif_gpkg, args.mapping, parquet_inputs)
    sys.exit(rc)


if __name__ == "__main__":
    main()
