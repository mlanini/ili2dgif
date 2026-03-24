#!/usr/bin/env python3
"""
ETL Transform & Load: swissTLM3D → DGIF GeoPackage

Reads features from a temporary swissTLM3D GeoPackage (imported via ili2gpkg),
applies the mapping table (swissTLM3D_to_DGIF_V3.csv), reprojects from
LV95 (EPSG:2056) to WGS84 (EPSG:4326), and inserts features into a
DGIF-schema GeoPackage.

The DGIF GeoPackage uses --smart2Inheritance, so each concrete class table
(e.g. cultural_building) contains ALL inherited columns from Entity and
FeatureEntity (including ageometry, beginlifespanversion, etc.).
This means a single INSERT per feature into the concrete table is sufficient.

Usage:
    python etl_swisstlm3d_transform.py \\
        --tlm-gpkg  C:/tmp/dgif/swisstlm3d_temp.gpkg \\
        --dgif-gpkg output/DGIF_swissTLM3D.gpkg \\
        --mapping   models/swissTLM3D_to_DGIF_V3.csv
"""

import argparse
import csv
import datetime
import sqlite3
import sys
import uuid
from collections import defaultdict
from pathlib import Path

try:
    from osgeo import ogr, osr, gdal
    gdal.UseExceptions()
except ImportError:
    print("[FATAL] GDAL/OGR Python bindings not found. Install via QGIS or pip.", file=sys.stderr)
    sys.exit(1)

# ============================================================================
# DGIF class → DGIF topic mapping
# Derived from DGIF_V3.ili topic boundaries + class line numbers.
# With --nameByTopic the ili2gpkg table name is "Topic.Class"
# ============================================================================
DGIF_CLASS_TO_TOPIC = {
    # Foundation
    "GeneralLocation": "Foundation",
    # AeronauticalFacility
    "Aerodrome": "AeronauticalFacility",
    "Heliport": "AeronauticalFacility",
    "Runway": "AeronauticalFacility",
    "Taxiway": "AeronauticalFacility",
    # Agricultural
    "AllotmentArea": "Agricultural",
    "Orchard": "Agricultural",
    "Vineyard": "Agricultural",
    # Boundaries
    "AdministrativeDivision": "Boundaries",
    "BoundaryMonument": "Boundaries",
    "ConservationArea": "Boundaries",
    # Cultural (large topic — lines 1082‥2678)
    "AerationBasin": "Cultural",
    "Amenity": "Cultural",
    "AmusementPark": "Cultural",
    "Amphitheatre": "Cultural",
    "ArcheologicalSite": "Cultural",
    "Aerial": "Cultural",
    "Bench": "Cultural",
    "Borehole": "Cultural",
    "BotanicGarden": "Cultural",
    "Bridge": "Transportation",  # line 6621 → Transportation
    "Building": "Cultural",
    "BuildingOverhang": "Cultural",
    "Cable": "Cultural",
    "Cableway": "Cultural",
    "Cairn": "Cultural",
    "CampSite": "Cultural",
    "Cemetery": "Cultural",
    "Checkpoint": "Cultural",
    "CompactSurface": "Cultural",
    "ConstructionZone": "Transportation",  # line 6833 → Transportation
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
    "FiringRange": "Cultural",
    "Flagpole": "Cultural",
    "Fountain": "Cultural",
    "GolfCourse": "Cultural",
    "GolfDrivingRange": "Cultural",
    "Installation": "Cultural",
    "InterestSite": "Cultural",
    "Lookout": "Cultural",
    "Market": "Cultural",
    "MedicalAmenity": "Cultural",
    "Monument": "Cultural",
    "NonBuildingStructure": "Cultural",
    "OverheadObstruction": "Cultural",
    "Park": "Cultural",
    "PicnicSite": "Cultural",
    "PowerSubstation": "Cultural",
    "PublicSquare": "Cultural",
    "Racetrack": "Cultural",
    "Ramp": "Cultural",
    "RecyclingSite": "Cultural",
    "Ruins": "Cultural",
    "SaltEvaporator": "Cultural",
    "SettlingPond": "Cultural",
    "SewageTreatmentPlant": "Cultural",
    "SkiJump": "Cultural",
    "SkiRun": "Cultural",
    "Smokestack": "Cultural",
    "SportsGround": "Cultural",
    "StorageTank": "Cultural",
    "SwimmingPool": "Cultural",
    "Tower": "Cultural",
    "TrainingSite": "Cultural",
    "UndergroundDwelling": "Cultural",
    "VehicleLot": "Cultural",
    "Wall": "Cultural",
    "Waterwork": "Cultural",
    "WindTurbine": "Cultural",
    "Zoo": "Cultural",
    # Elevation
    "GeomorphicExtreme": "Elevation",
    # HydrographicAidsNavigation
    "ShorelineConstruction": "HydrographicAidsNavigation",
    # InlandWater
    "Canal": "Transportation",   # line 6758 → Transportation
    "Dam": "InlandWater",
    "Ditch": "InlandWater",
    "Embankment": "Physiography",  # line 6036
    "InlandWaterbody": "InlandWater",
    "InundatedLand": "InlandWater",
    "Lock": "InlandWater",
    "River": "InlandWater",
    "Spring": "InlandWater",
    "Waterfall": "InlandWater",
    # MilitaryInstallationsDefensiveStructures
    "Fortification": "MilitaryInstallationsDefensiveStructures",
    # OceanEnvironment
    "Harbour": "PortsHarbours",  # line 6493
    "Island": "Physiography",    # line 6315
    # Physiography
    "Glacier": "Physiography",
    "Hill": "Physiography",
    "LandArea": "Physiography",
    "LandMorphologyArea": "Physiography",
    "MountainPass": "Physiography",
    "PermanentSnowIce": "Physiography",
    "RockFormation": "Physiography",
    "SoilSurfaceRegion": "Physiography",
    # Population
    "PopulatedPlace": "Population",
    "Neighbourhood": "Population",
    # Vegetation
    "Forest": "Vegetation",
    "Scrubland": "Vegetation",
    "ShrubLand": "Vegetation",
    "Tree": "Vegetation",
    # Transportation
    "FerryCrossing": "Transportation",
    "LandRoute": "Transportation",
    "LandTransportationWay": "Transportation",
    "Pipeline": "Transportation",
    "Railway": "Transportation",
    "RailwayYard": "Transportation",
    "RoadInterchange": "Transportation",
    "TransportationPlatform": "Transportation",
    "TransportationStation": "Transportation",
    "Tunnel": "Transportation",
    "VehicleBarrier": "Transportation",
    # PortsHarbours
    "Checkpoint_SU004": "Cultural",  # Checkpoint is in Cultural; SU004 alias
    # HydrographicAidsNavigation
    "CaveMouth": "Physiography",  # DB029 not in Hydro — reassign
}

# Fallback: if a class is not found above, try to discover it dynamically
# from the GeoPackage table list.


# ============================================================================
# Mapping row data class
# ============================================================================
class MappingRow:
    """One row from swissTLM3D_to_DGIF_V3.csv"""
    __slots__ = (
        "no", "tlm_topic", "tlm_class", "tlm_attr", "tlm_value",
        "geometry_type", "description",
        "dgif_class", "dgif_code",
        "dgif_attr1", "dgif_attr1_code", "dgif_val1", "dgif_val1_code",
        "dgif_attr2", "dgif_attr2_code", "dgif_val2", "dgif_val2_code",
    )

    def __init__(self, row: list[str]):
        self.no = row[0]
        self.tlm_topic = row[1]
        self.tlm_class = row[2]
        self.tlm_attr = row[3]
        self.tlm_value = row[4]
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
        """True if this row has a valid DGIF target (not 'not in DGIF')."""
        return bool(self.dgif_class) and self.description != "not in DGIF"


# ============================================================================
# Load mapping CSV
# ============================================================================
def load_mapping(csv_path: str) -> dict[tuple[str, str], list[MappingRow]]:
    """
    Returns dict keyed by (TLM_class, Objektart_value) → list[MappingRow].
    """
    mapping: dict[tuple[str, str], list[MappingRow]] = defaultdict(list)
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)  # skip header
        for row in reader:
            if not row or not row[0].strip():
                continue
            mr = MappingRow(row)
            if mr.is_mapped:
                mapping[(mr.tlm_class, mr.tlm_value)].append(mr)
    return mapping


# ============================================================================
# Discover TLM source tables in the GeoPackage
# ============================================================================
def discover_tlm_tables(gpkg_path: str) -> dict[str, str]:
    """
    Returns dict of TLM class name → actual GeoPackage table name.
    Uses the ili2db metadata table T_ILI2DB_CLASSNAME to resolve
    INTERLIS qualified names (e.g. 'swissTLM3D_ili2_V2_4.TLM_STRASSEN.TLM_STRASSE')
    to the actual SQL table name (e.g. 'tlm_strassen_tlm_strasse').
    The returned key is the short class name (e.g. 'TLM_STRASSE').
    """
    import sqlite3

    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()

    # Build set of actual feature table names for filtering
    cur.execute("SELECT table_name FROM gpkg_contents WHERE data_type='features'")
    feature_tables = {row[0] for row in cur.fetchall()}

    # Use ili2db metadata to get the INTERLIS class name → SQL table mapping
    tables = {}
    cur.execute("SELECT iliname, sqlname FROM T_ILI2DB_CLASSNAME")
    for iliname, sqlname in cur.fetchall():
        if sqlname not in feature_tables:
            continue
        # iliname: 'swissTLM3D_ili2_V2_4.TLM_STRASSEN.TLM_STRASSE'
        # Extract last part as class name: 'TLM_STRASSE'
        parts = iliname.split(".")
        if len(parts) >= 3:
            class_name = parts[-1]  # e.g. 'TLM_STRASSE'
            tables[class_name] = sqlname

    conn.close()
    return tables


# ============================================================================
# Discover DGIF target tables in the GeoPackage
# ============================================================================
def discover_dgif_tables(gpkg_path: str) -> dict[str, str]:
    """
    Returns dict of DGIF class name → actual GeoPackage table name.
    Uses the ili2db metadata table T_ILI2DB_CLASSNAME to resolve
    INTERLIS qualified names (e.g. 'DGIF_V3.Cultural.Building')
    to the actual SQL table name (e.g. 'cultural_building').
    The returned key is the short class name (e.g. 'Building').
    """
    import sqlite3

    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()

    # Build set of actual table names for filtering (features + attributes)
    cur.execute("SELECT table_name FROM gpkg_contents WHERE data_type IN ('features','attributes')")
    feature_tables = {row[0] for row in cur.fetchall()}

    # Use ili2db metadata to get the INTERLIS class name → SQL table mapping
    tables = {}
    cur.execute("SELECT iliname, sqlname FROM T_ILI2DB_CLASSNAME")
    for iliname, sqlname in cur.fetchall():
        if sqlname not in feature_tables:
            continue
        # iliname: 'DGIF_V3.Cultural.Building'
        # Extract last part as class name: 'Building'
        parts = iliname.split(".")
        if len(parts) >= 3:
            class_name = parts[-1]  # e.g. 'Building'
            tables[class_name] = sqlname

    conn.close()
    return tables


# ============================================================================
# Get column names for a table
# ============================================================================
def get_column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    """Return set of column names (lowercase) for a table."""
    cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
    return {row[1].lower() for row in cursor.fetchall()}


# ============================================================================
# Coordinate transformer
# ============================================================================
def create_transformer() -> osr.CoordinateTransformation:
    """LV95 (EPSG:2056) → WGS84 (EPSG:4326)"""
    src = osr.SpatialReference()
    src.ImportFromEPSG(2056)
    dst = osr.SpatialReference()
    dst.ImportFromEPSG(4326)
    # Ensure axis order is lon/lat for OGR
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return osr.CoordinateTransformation(src, dst)


# ============================================================================
# Geometry helpers
# ============================================================================
def reproject_geometry(geom: ogr.Geometry, transform: osr.CoordinateTransformation) -> ogr.Geometry:
    """Reproject an OGR geometry, flattening 3D → 2D (DGIF uses Coord2)."""
    if geom is None:
        return None
    clone = geom.Clone()
    clone.FlattenTo2D()
    clone.Transform(transform)
    return clone


def extract_centroid_coord2(geom: ogr.Geometry, transform: osr.CoordinateTransformation) -> ogr.Geometry:
    """For polygon/line features that must map to Point DGIF classes, extract centroid.

    Note: Only used when a source geometry is non-point and the DGIF target
    column is registered as POINT.  Most DGIF classes now declare the correct
    geometry type (POLYGON / LINESTRING) so this is rarely needed.
    """
    if geom is None:
        return None
    clone = geom.Clone()
    clone.FlattenTo2D()
    clone.Transform(transform)
    centroid = clone.Centroid()
    return centroid


# ============================================================================
# Build GPKG-compatible WKB (little-endian header with SRS ID)
# ============================================================================
def to_gpkg_wkb(geom: ogr.Geometry, srs_id: int = 4326) -> bytes:
    """
    Convert OGR geometry to GeoPackage binary (GP header + WKB).
    See http://www.geopackage.org/spec/#gpb_format
    """
    if geom is None:
        return None

    wkb = geom.ExportToWkb(ogr.wkbNDR)  # little-endian WKB
    envelope = geom.GetEnvelope()  # (minX, maxX, minY, maxY)

    import struct
    # GP header: magic 'GP', version 0, flags, srs_id, envelope
    # flags byte: envelope type 1 (minX,maxX,minY,maxY) = 0b00000010 = 0x02
    # byte order: little-endian (0x01)
    flags = 0x02 | 0x01  # envelope type 1 + little-endian
    header = struct.pack(
        '<2sBBi4d',
        b'GP',          # magic
        0,              # version
        flags,          # flags
        srs_id,         # srs_id
        envelope[0],    # minX
        envelope[1],    # maxX
        envelope[2],    # minY
        envelope[3],    # maxY
    )
    return header + wkb


# ============================================================================
# Build ili2db class metadata from DGIF GeoPackage
# ============================================================================
def build_class_metadata(conn: sqlite3.Connection) -> dict:
    """
    Build metadata dict for DGIF classes from ili2db system tables.
    Returns dict keyed by short class name (e.g. 'Building') with:
      - iliname: fully qualified INTERLIS name (e.g. 'DGIF_V3.Cultural.Building')
      - sqlname: SQL table name (e.g. 'cultural_building')
      - topic:   INTERLIS topic name (e.g. 'Cultural')
      - columns: set of column names (lowercase)
      - notnull_defaults: dict of lowercase col name -> default value for
        domain-specific NOT NULL columns (excludes T_Id, T_basket, T_LastChange,
        T_CreateDate, T_User which are always provided)
    """
    cur = conn.cursor()

    # Get all class name mappings
    cur.execute("SELECT iliname, sqlname FROM T_ILI2DB_CLASSNAME")
    classname_rows = cur.fetchall()

    # Get all table names from gpkg_contents (features + attributes)
    cur.execute("SELECT table_name FROM gpkg_contents WHERE data_type IN ('features','attributes')")
    all_tables = {row[0] for row in cur.fetchall()}

    # Columns always provided by the ETL code
    always_provided = {"t_id", "t_basket", "t_lastchange", "t_createdate", "t_user"}

    # Build a lookup of registered geometry type per table from gpkg_geometry_columns
    cur.execute("SELECT table_name, geometry_type_name FROM gpkg_geometry_columns")
    table_geom_type = {row[0]: row[1].upper() for row in cur.fetchall()}

    meta = {}
    for iliname, sqlname in classname_rows:
        if sqlname not in all_tables:
            continue
        parts = iliname.split(".")
        if len(parts) >= 3:
            class_name = parts[-1]   # e.g. 'Building'
            topic_name = parts[-2]   # e.g. 'Cultural'
            # Get column info (name, type, notnull, default)
            col_cur = conn.execute(f'PRAGMA table_info("{sqlname}")')
            columns = set()
            notnull_defaults = {}
            for row in col_cur.fetchall():
                col_name = row[1].lower()
                col_type = (row[2] or "").upper()
                is_notnull = bool(row[3])
                columns.add(col_name)
                if is_notnull and col_name not in always_provided:
                    # Determine a sensible default based on column type
                    if "INT" in col_type:
                        notnull_defaults[col_name] = 0
                    elif "DOUBLE" in col_type or "REAL" in col_type or "FLOAT" in col_type:
                        notnull_defaults[col_name] = 0.0
                    elif "BOOL" in col_type:
                        notnull_defaults[col_name] = False
                    else:
                        # TEXT / VARCHAR — use 'unknown'
                        notnull_defaults[col_name] = "unknown"
            meta[class_name] = {
                "iliname": iliname,
                "sqlname": sqlname,
                "topic": topic_name,
                "columns": columns,
                "notnull_defaults": notnull_defaults,
                "geom_type": table_geom_type.get(sqlname, ""),  # e.g. "POLYGON", "LINESTRING", "POINT"
            }
    return meta


# ============================================================================
# Ensure dataset and baskets exist
# ============================================================================
def ensure_baskets(conn: sqlite3.Connection, topics_needed: set[str]) -> dict[str, int]:
    """
    Create a dataset and one basket per DGIF topic.
    Returns dict of topic_iliname -> basket T_Id.
    """
    cur = conn.cursor()

    # Check for existing dataset
    cur.execute("SELECT T_Id FROM T_ILI2DB_DATASET LIMIT 1")
    row = cur.fetchone()
    if row:
        dataset_id = row[0]
    else:
        cur.execute("INSERT INTO T_ILI2DB_DATASET (datasetName) VALUES (?)", ("swissTLM3D_import",))
        dataset_id = cur.lastrowid

    # Create baskets for each topic
    basket_map = {}
    for topic_ili in sorted(topics_needed):
        # topic_ili e.g. 'DGIF_V3.Cultural'
        cur.execute("SELECT T_Id FROM T_ILI2DB_BASKET WHERE topic=?", (topic_ili,))
        row = cur.fetchone()
        if row:
            basket_map[topic_ili] = row[0]
        else:
            basket_tid = str(uuid.uuid4())
            cur.execute(
                "INSERT INTO T_ILI2DB_BASKET (dataset, topic, T_Ili_Tid, attachmentKey) VALUES (?,?,?,?)",
                (dataset_id, topic_ili, basket_tid, "swissTLM3D_import")
            )
            basket_map[topic_ili] = cur.lastrowid

    conn.commit()
    return basket_map


# ============================================================================
# Populate Foundation metadata from geocat.ch (swissTLM3D)
# ============================================================================
# Metadata source: geocat.ch ISO 19115/19139 record
#   https://www.geocat.ch/geonetwork/srv/api/records/73856ca2-f21d-4cc9-90f6-f3e8375555df
# The values below are extracted from the official swisstopo metadata for
# swissTLM3D and mapped to the DGIF Foundation topic classes.
# ============================================================================

_GEOCAT_FILE_ID = "73856ca2-f21d-4cc9-90f6-f3e8375555df"
_GEOCAT_URL = (
    "https://www.geocat.ch/geonetwork/srv/api/records/"
    f"{_GEOCAT_FILE_ID}/formatters/xml?approved=true"
)

# Static metadata values (from geocat.ch record for swissTLM3D)
_META = {
    # SourceInfo
    "datasetcitation": (
        "swissTLM3D - The large-scale topographic landscape model of Switzerland. "
        f"geocat.ch fileIdentifier: {_GEOCAT_FILE_ID}"
    ),
    "sourcedescription": (
        "The large-scale topographic landscape model of Switzerland covering the "
        "entire territory of Switzerland and the Principality of Liechtenstein. "
        "swissTLM3D contains the natural and artificial objects of the landscape. "
        "It is the most detailed and accurate 3D vector dataset of swisstopo."
    ),
    "sourceidentifier": _GEOCAT_FILE_ID,
    "typeofsource": "vectorDataset",
    "resourcecontentorigin": "swissTLM3D",
    "scaledenominator": 5000,
    "sourcecurrencydatetime": "2024",

    # Organisation
    "organisationdescription": "Federal Office of Topography swisstopo",
    "organisationtype": "government",
    "homegeopoliticalentity": "CHE",
    "organisationreach": "national",
    "branding": "swisstopo",

    # ContactInfo
    "addresscountry": "CHE",
    "addresscity": "Wabern",
    "addressdeliverypoint": "Seftigenstrasse 264",
    "addresspostalcode": "3084",
    "addressadministrativearea": "BE",
    "addresselectronicmail": "geodata@swisstopo.ch",
    "telephonevoice": "+41 58 469 01 11",
    "onlineresourcelinkage": "https://www.swisstopo.admin.ch",

    # RestrictionInfo (OGD / open data since 2021)
    "commercialcopyrightnotice": "© swisstopo",
    "commercialdistribrestrict": "openData (OGD)",

    # HorizCoordMetadata (data reprojected to WGS84 in DGIF)
    "geodeticdatum": "worldGeodeticSystem1984",
    "horizaccuracycategory": (
        "0.2-1.5 m (well-defined features) / 1-3 m (not clearly defined features)"
    ),

    # FeatureMetadata
    "delineationknown": 1,  # BOOLEAN as integer
    "existencecertaintycat": "definite",
    "surveycoveragecategory": "complete",
    "dataqualitystatement": (
        "Geometric accuracy: well-defined objects 0.2-1.5 m, not clearly defined "
        "objects 1-3 m. Update cycle: approx. 6 years for the full territory. "
        "Source: official mensuration, aerial imagery, stereo plotting."
    ),

    # FeatureAttMetadata
    "att_currencydatetime": "2024",
    "att_dataqualitystatement": (
        "Attribute quality controlled against official registers and field "
        "verification. Thematic accuracy > 95% for main categories."
    ),
}


def populate_foundation_metadata(
    conn: sqlite3.Connection,
    basket_tid: int,
    start_tid: int,
) -> int:
    """Insert Foundation metadata records derived from geocat.ch.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open connection to the DGIF GeoPackage.
    basket_tid : int
        T_Id of the Foundation basket (from ``ensure_baskets``).
    start_tid : int
        First available T_Id (to avoid collisions with existing data).

    Returns
    -------
    int
        The next available T_Id after all inserts.
    """
    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    user = "etl_swisstlm3d"
    tid = start_tid

    def _ili_tid() -> str:
        return str(uuid.uuid4())

    # Helper: common ili2db bookkeeping columns
    def _base(extra: dict | None = None) -> dict:
        nonlocal tid
        row = {
            "T_Id": tid,
            "T_basket": basket_tid,
            "T_Ili_Tid": _ili_tid(),
            "T_LastChange": now_iso,
            "T_CreateDate": now_iso,
            "T_User": user,
        }
        if extra:
            row.update(extra)
        tid += 1
        return row

    # Helper: Entity columns (for Organisation, OrganisationalUnit, …)
    def _entity(extra: dict | None = None) -> dict:
        row = _base({
            "beginlifespanversion": now_iso,
            "uniqueuniversalentityidentifier": _ili_tid(),
        })
        if extra:
            row.update(extra)
        return row

    # Helper: generic INSERT
    def _insert(table: str, row: dict) -> int:
        cols = ", ".join(row.keys())
        placeholders = ", ".join(["?"] * len(row))
        conn.execute(
            f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})',
            list(row.values()),
        )
        return row["T_Id"]

    m = _META  # shortcut

    print("\n[INFO] Populating Foundation metadata (geocat.ch -> DGIF) ...")

    # 1) SourceInfo
    src_tid = _insert("foundation_sourceinfo", _base({
        "datasetcitation": m["datasetcitation"],
        "sourcedescription": m["sourcedescription"],
        "sourceidentifier": m["sourceidentifier"],
        "typeofsource": m["typeofsource"],
        "resourcecontentorigin": m["resourcecontentorigin"],
        "scaledenominator": m["scaledenominator"],
        "sourcecurrencydatetime": m["sourcecurrencydatetime"],
    }))
    print(f"  [OK] foundation_sourceinfo          T_Id={src_tid}")

    # 2) Organisation (EXTENDS ActorEntity EXTENDS Entity)
    org_tid = _insert("foundation_organisation", _entity({
        "organisationdescription": m["organisationdescription"],
        "organisationtype": m["organisationtype"],
        "homegeopoliticalentity": m["homegeopoliticalentity"],
        "organisationreach": m["organisationreach"],
        "branding": m["branding"],
    }))
    print(f"  [OK] foundation_organisation         T_Id={org_tid}")

    # 3) ContactInfo
    contact_tid = _insert("foundation_contactinfo", _base({
        "addresscountry": m["addresscountry"],
        "addresscity": m["addresscity"],
        "addressdeliverypoint": m["addressdeliverypoint"],
        "addresspostalcode": m["addresspostalcode"],
        "addressadministrativearea": m["addressadministrativearea"],
        "addresselectronicmail": m["addresselectronicmail"],
        "telephonevoice": m["telephonevoice"],
        "onlineresourcelinkage": m["onlineresourcelinkage"],
    }))
    print(f"  [OK] foundation_contactinfo          T_Id={contact_tid}")

    # 4) OrganisationalUnit (EXTENDS ActorEntity EXTENDS Entity)
    #    contactInfo is MANDATORY TEXT*1024 — we store structured contact summary
    #    mainOrganisation is FK → Organisation
    orgunit_tid = _insert("foundation_organisationalunit", _entity({
        "contactinfo": (
            f"{m['addressdeliverypoint']}, {m['addresspostalcode']} "
            f"{m['addresscity']}, {m['addresscountry']}; "
            f"email: {m['addresselectronicmail']}; "
            f"tel: {m['telephonevoice']}; "
            f"web: {m['onlineresourcelinkage']}"
        ),
        "mainorganisation": org_tid,
    }))
    print(f"  [OK] foundation_organisationalunit   T_Id={orgunit_tid}")

    # 5) RestrictionInfo
    restr_tid = _insert("foundation_restrictioninfo", _base({
        "commercialcopyrightnotice": m["commercialcopyrightnotice"],
        "commercialdistribrestrict": m["commercialdistribrestrict"],
    }))
    print(f"  [OK] foundation_restrictioninfo      T_Id={restr_tid}")

    # 6) HorizCoordMetadata
    hcoord_tid = _insert("foundation_horizcoordmetadata", _base({
        "geodeticdatum": m["geodeticdatum"],
        "horizaccuracycategory": m["horizaccuracycategory"],
    }))
    print(f"  [OK] foundation_horizcoordmetadata   T_Id={hcoord_tid}")

    # 7) FeatureMetadata
    fmeta_tid = _insert("foundation_featuremetadata", _base({
        "delineationknown": m["delineationknown"],
        "delineationknown_txt": "true",
        "existencecertaintycat": m["existencecertaintycat"],
        "surveycoveragecategory": m["surveycoveragecategory"],
        "dataqualitystatement": m["dataqualitystatement"],
    }))
    print(f"  [OK] foundation_featuremetadata      T_Id={fmeta_tid}")

    # 8) FeatureAttMetadata
    fameta_tid = _insert("foundation_featureattmetadata", _base({
        "currencydatetime": m["att_currencydatetime"],
        "dataqualitystatement": m["att_dataqualitystatement"],
    }))
    print(f"  [OK] foundation_featureattmetadata   T_Id={fameta_tid}")

    # 9) NameSpecification (dataset name)
    name_tid = _insert("foundation_namespecification", _base({
        "aname": "swissTLM3D",
        "nametype": "official",
        "nameusedescription": "Official swisstopo product name",
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
    tlm_gpkg_path: str,
    dgif_gpkg_path: str,
    mapping_csv_path: str,
):
    print("[INFO] Loading mapping table...")
    mapping = load_mapping(mapping_csv_path)
    print(f"[INFO] Loaded {sum(len(v) for v in mapping.values())} mapping rules for "
          f"{len(mapping)} (class, Objektart) pairs")

    print("[INFO] Discovering TLM tables...")
    tlm_tables = discover_tlm_tables(tlm_gpkg_path)
    print(f"[INFO] Found {len(tlm_tables)} TLM tables")

    # Open DGIF GeoPackage via sqlite3
    dgif_conn = sqlite3.connect(dgif_gpkg_path)
    dgif_conn.execute("PRAGMA journal_mode=WAL")
    dgif_conn.execute("PRAGMA synchronous=NORMAL")
    dgif_conn.execute("PRAGMA cache_size=-64000")  # 64 MB
    dgif_conn.execute("PRAGMA foreign_keys=OFF")    # defer FK checks for performance

    # Drop rtree triggers that reference ST_IsEmpty / ST_MinX etc.
    # These SpatiaLite functions are not available in plain sqlite3.
    # We will rebuild the rtree index after all inserts.
    rtree_triggers = dgif_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' "
        "AND (sql LIKE '%ST_IsEmpty%' OR sql LIKE '%ST_MinX%')"
    ).fetchall()
    if rtree_triggers:
        print(f"[INFO] Dropping {len(rtree_triggers)} rtree triggers (SpatiaLite not available)...")
        for (tname,) in rtree_triggers:
            dgif_conn.execute(f'DROP TRIGGER IF EXISTS "{tname}"')
        dgif_conn.commit()
        print(f"[INFO]   Dropped: {[t[0] for t in rtree_triggers]}")

    print("[INFO] Building DGIF class metadata from ili2db tables...")
    class_meta = build_class_metadata(dgif_conn)
    print(f"[INFO] Found {len(class_meta)} DGIF classes")

    # Coordinate transformer
    transform_ct = create_transformer()

    # Open TLM as OGR read-only
    tlm_ds = ogr.Open(tlm_gpkg_path, 0)
    if tlm_ds is None:
        print("[FATAL] Cannot open TLM GeoPackage", file=sys.stderr)
        sys.exit(1)

    # Collect which DGIF topics are needed for baskets
    topics_needed = set()
    for (_, _), rules in mapping.items():
        for mr in rules:
            if mr.dgif_class in class_meta:
                meta = class_meta[mr.dgif_class]
                topics_needed.add(f"DGIF_V3.{meta['topic']}")

    print(f"[INFO] Creating baskets for {len(topics_needed)} topics...")
    basket_map = ensure_baskets(dgif_conn, topics_needed)
    print(f"[INFO] Baskets: {basket_map}")

    # T_Id counter — start at 1 (tables are empty after schemaimport)
    next_tid = 1

    # Statistics
    stats = defaultdict(int)
    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Track spatial extent **per table** for gpkg_contents update.
    table_extents: dict[str, list[float]] = {}  # table -> [minX, minY, maxX, maxY]

    # Track actual WKB geometry types written per DGIF table.
    # OGR geometry type name for the *flat* type (2D), e.g. "POINT", "LINESTRING", "POLYGON".
    # Used after all inserts to reconcile gpkg_geometry_columns.
    _OGR_TYPE_NAMES = {
        ogr.wkbPoint: "POINT",
        ogr.wkbLineString: "LINESTRING",
        ogr.wkbPolygon: "POLYGON",
        ogr.wkbMultiPoint: "MULTIPOINT",
        ogr.wkbMultiLineString: "MULTILINESTRING",
        ogr.wkbMultiPolygon: "MULTIPOLYGON",
    }
    actual_geom_types: dict[str, set[str]] = defaultdict(set)  # table -> set of type names

    # Collect unique TLM classes referenced in mapping
    tlm_classes_needed = set()
    for (tlm_cls, _) in mapping.keys():
        tlm_classes_needed.add(tlm_cls)

    print(f"\n[INFO] Processing {len(tlm_classes_needed)} TLM classes...")
    print("=" * 60)

    for tlm_class_name in sorted(tlm_classes_needed):
        # Find the actual table name in TLM GeoPackage
        if tlm_class_name not in tlm_tables:
            print(f"  [SKIP] TLM class '{tlm_class_name}' not found in GeoPackage")
            stats["tlm_class_not_found"] += 1
            continue

        tlm_table = tlm_tables[tlm_class_name]
        tlm_layer = tlm_ds.GetLayerByName(tlm_table)
        if tlm_layer is None:
            print(f"  [SKIP] Cannot open TLM layer: {tlm_table}")
            stats["tlm_layer_error"] += 1
            continue

        feature_count = tlm_layer.GetFeatureCount()
        print(f"\n  [{tlm_class_name}] ({tlm_table}) -- {feature_count} features")

        # Collect all Objektart values mapped for this class
        objektart_map: dict[str, list[MappingRow]] = {}
        for (cls, val), rules in mapping.items():
            if cls == tlm_class_name:
                objektart_map[val] = rules

        # Determine field indices once
        layer_defn = tlm_layer.GetLayerDefn()
        field_names = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]
        field_names_lower = [n.lower() for n in field_names]

        has_objektart = "objektart" in field_names_lower
        has_name = "name" in field_names_lower
        has_t_ili_tid = "t_ili_tid" in field_names_lower
        has_datum_erstellung = "datum_erstellung" in field_names_lower

        objektart_idx = field_names_lower.index("objektart") if has_objektart else -1
        name_idx = field_names_lower.index("name") if has_name else -1
        t_ili_tid_idx = field_names_lower.index("t_ili_tid") if has_t_ili_tid else -1
        datum_erst_idx = field_names_lower.index("datum_erstellung") if has_datum_erstellung else -1

        class_inserted = 0
        class_skipped = 0
        class_no_match = 0

        # Iterate features
        tlm_layer.ResetReading()
        feat: ogr.Feature
        for feat in tlm_layer:
            # Get Objektart value
            if has_objektart and objektart_idx >= 0:
                objektart_val = feat.GetFieldAsString(objektart_idx)
                # ili2gpkg stores enum values with the topic prefix stripped;
                # try both raw value and stripped
                objektart_val_clean = objektart_val.split(".")[-1] if "." in objektart_val else objektart_val
            else:
                # Classes without Objektart (e.g. TLM_BODENBEDECKUNG sometimes)
                objektart_val = ""
                objektart_val_clean = ""

            # Find mapping rules
            rules = objektart_map.get(objektart_val_clean)
            if rules is None:
                rules = objektart_map.get(objektart_val)
            if rules is None:
                class_no_match += 1
                continue

            # Get source geometry
            src_geom = feat.GetGeometryRef()

            # Get source attributes
            src_name = feat.GetFieldAsString(name_idx) if has_name and name_idx >= 0 else None
            src_tid = feat.GetFieldAsString(t_ili_tid_idx) if has_t_ili_tid and t_ili_tid_idx >= 0 else None
            src_datum = feat.GetFieldAsString(datum_erst_idx) if has_datum_erstellung and datum_erst_idx >= 0 else None

            # Apply each mapping rule (usually 1, but could be multiple)
            for mr in rules:
                dgif_class = mr.dgif_class

                # Resolve DGIF class metadata
                if dgif_class not in class_meta:
                    stats["dgif_class_not_found"] += 1
                    continue

                meta = class_meta[dgif_class]
                dgif_table_name = meta["sqlname"]
                dgif_cols = meta["columns"]
                dgif_iliname = meta["iliname"]   # e.g. 'DGIF_V3.Cultural.Building'
                dgif_topic = meta["topic"]       # e.g. 'Cultural'

                # Resolve basket
                topic_key = f"DGIF_V3.{dgif_topic}"
                basket_id = basket_map.get(topic_key)
                if basket_id is None:
                    stats["dgif_basket_not_found"] += 1
                    continue

                # Assign T_Id for this feature
                tid = next_tid
                next_tid += 1

                # Generate identifiers
                ili_tid = src_tid if src_tid else str(uuid.uuid4())
                entity_uuid = ili_tid
                # Normalise date to ISO 8601 (YYYY-MM-DD); source may use
                # YYYY/MM/DD or other separators.
                if src_datum:
                    begin_date = src_datum.replace("/", "-")
                else:
                    begin_date = now_iso

                # --- Geometry ---
                # With --smart2Inheritance each concrete class table has its
                # own ageometry column.  The registered geometry type in
                # gpkg_geometry_columns (POINT, LINESTRING, POLYGON) reflects
                # the OCL constraint from the INTERLIS model.
                #
                # Strategy:
                #   • If the DGIF target expects POINT but the source is
                #     Line/Polygon → extract centroid (rare edge case).
                #   • Otherwise → reproject the source geometry as-is.
                dgif_geom_type = meta.get("geom_type", "")  # e.g. "POLYGON"
                geom_wkb = None
                if src_geom is not None:
                    src_flat = ogr.GT_Flatten(src_geom.GetGeometryType())
                    if dgif_geom_type == "POINT" and src_flat != ogr.wkbPoint:
                        # Target is POINT but source is not → centroid
                        target_geom = extract_centroid_coord2(src_geom, transform_ct)
                    else:
                        # Keep original geometry (reproject + flatten 3D→2D)
                        target_geom = reproject_geometry(src_geom, transform_ct)
                    if target_geom is not None:
                        geom_wkb = to_gpkg_wkb(target_geom, srs_id=4326)
                        # Record the actual WKB type written
                        written_flat = ogr.GT_Flatten(target_geom.GetGeometryType())
                        written_name = _OGR_TYPE_NAMES.get(written_flat, f"UNKNOWN({written_flat})")
                        actual_geom_types[dgif_table_name].add(written_name)
                        # Track per-table extent via envelope
                        env = target_geom.GetEnvelope()  # (minX, maxX, minY, maxY)
                        if dgif_table_name not in table_extents:
                            table_extents[dgif_table_name] = [env[0], env[2], env[1], env[3]]
                        else:
                            te = table_extents[dgif_table_name]
                            if env[0] < te[0]: te[0] = env[0]
                            if env[2] < te[1]: te[1] = env[2]
                            if env[1] > te[2]: te[2] = env[1]
                            if env[3] > te[3]: te[3] = env[3]

                # --- Single-table insert (--smart2Inheritance) ---
                # With --smart2Inheritance, each concrete class table contains
                # ALL inherited columns (Entity + FeatureEntity + domain attrs).
                # No separate foundation_entity / foundation_featureentity insert.
                has_geom_col = "ageometry" in dgif_cols

                # If no geometry was extracted leave ageometry as NULL.
                # (Inserting a fake default point at 0,0 would place features
                # off the map and pollute the data.)

                insert_cols = [
                    "T_Id", "T_Ili_Tid", "T_basket",
                    "beginlifespanversion", "uniqueuniversalentityidentifier",
                    "T_LastChange", "T_CreateDate", "T_User",
                ]
                insert_vals: list = [
                    tid, ili_tid, basket_id,
                    begin_date, entity_uuid,
                    now_iso, now_iso, "etl_swisstlm3d",
                ]

                # Add geometry column only for FeatureEntity subclasses
                if has_geom_col:
                    insert_cols.insert(3, "ageometry")
                    insert_vals.insert(3, geom_wkb)

                # Map DGIF-specific attributes from CSV
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

                # Fill remaining NOT NULL columns with defaults
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
                    class_inserted += 1
                    stats[f"inserted:{dgif_table_name}"] += 1
                except sqlite3.Error as e:
                    if stats["insert_error"] < 5:
                        print(f"  [DEBUG] Insert error: {e}", file=sys.stderr)
                        print(f"  [DEBUG]   table={dgif_table_name}", file=sys.stderr)
                    stats["insert_error"] += 1
                    class_skipped += 1

        stats["total_inserted"] += class_inserted
        stats["total_skipped"] += class_skipped
        stats["total_no_match"] += class_no_match

        print(f"    -> Inserted: {class_inserted}  |  Skipped: {class_skipped}  |  No match: {class_no_match}")

    # Commit
    print("\n[INFO] Committing to DGIF GeoPackage...")
    dgif_conn.commit()

    # ---- Foundation metadata (geocat.ch → DGIF) ----
    foundation_basket = basket_map.get("DGIF_V3.Foundation")
    if foundation_basket is None:
        # Ensure Foundation basket exists even when no GeneralLocation features
        foundation_basket = ensure_baskets(dgif_conn, {"DGIF_V3.Foundation"}).get(
            "DGIF_V3.Foundation"
        )
    if foundation_basket is not None:
        next_tid = populate_foundation_metadata(
            dgif_conn, foundation_basket, next_tid
        )
    else:
        print("[WARN] Foundation basket not found — skipping metadata population.")

    # Update gpkg_contents extent **per table** (not global)
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
                updated_count += dgif_conn.execute(
                    "SELECT changes()"
                ).fetchone()[0]
            dgif_conn.commit()
            # Compute overall extent for log
            all_minx = min(e[0] for e in table_extents.values())
            all_miny = min(e[1] for e in table_extents.values())
            all_maxx = max(e[2] for e in table_extents.values())
            all_maxy = max(e[3] for e in table_extents.values())
            print(f"[INFO]   Overall extent: ({all_minx:.6f}, {all_miny:.6f}) - "
                  f"({all_maxx:.6f}, {all_maxy:.6f})")
            print(f"[INFO]   Updated {updated_count} gpkg_contents rows")
        except sqlite3.Error as e:
            print(f"[WARN] Could not update extents: {e}", file=sys.stderr)
    else:
        print("[INFO]   No geometry inserted, skipping extent update.")

    # Null-out extents for empty feature tables (ili2gpkg sets default
    # (-180,-90)-(180,90) at schema import which pollutes overall extent).
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

    # Reconcile gpkg_geometry_columns: update geometry_type_name to match
    # the actual WKB types written.  When a table received a single type
    # (e.g. only LINESTRING) use that; when mixed types were written use
    # "GEOMETRY" so QGIS accepts all of them.
    print("[INFO] Reconciling gpkg_geometry_columns with actual geometry types...")
    geom_type_updates = 0
    for tbl, written_types in actual_geom_types.items():
        if not written_types:
            continue
        if len(written_types) == 1:
            new_type = next(iter(written_types))
        else:
            new_type = "GEOMETRY"
        # Check current registered type
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
            if geom_type_updates <= 10:
                print(f"  [INFO]   {tbl}: {row[0]} -> {new_type}")
    if geom_type_updates > 10:
        print(f"  [INFO]   ... and {geom_type_updates - 10} more")
    dgif_conn.commit()
    print(f"[INFO]   Updated geometry_type_name for {geom_type_updates} tables")

    # ---------------------------------------------------------------
    # Populate R-tree spatial indexes.
    # ili2gpkg creates rtree_<table>_ageometry virtual tables but only
    # adds a DELETE trigger; INSERTs done via raw SQL bypass the index.
    # We rebuild each populated table's R-tree from the GP binary
    # envelope stored in the geometry blob.
    # ---------------------------------------------------------------
    print("[INFO] Populating R-tree spatial indexes...")
    import struct as _struct
    rtree_total = 0
    rtree_tables = 0
    geom_col_rows = dgif_conn.execute(
        "SELECT table_name, column_name FROM gpkg_geometry_columns"
    ).fetchall()
    for tbl, geom_col in geom_col_rows:
        rtree_name = f"rtree_{tbl}_{geom_col}"
        # Check the rtree table exists
        exists = dgif_conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (rtree_name,)
        ).fetchone()
        if not exists:
            continue
        # Only process tables that have data
        cnt = dgif_conn.execute(
            f'SELECT COUNT(*) FROM "{tbl}" WHERE "{geom_col}" IS NOT NULL'
        ).fetchone()[0]
        if cnt == 0:
            continue
        # Clear any stale entries
        dgif_conn.execute(f'DELETE FROM "{rtree_name}"')
        # Insert from geometry blobs: parse GP header envelope
        inserted = 0
        for (rowid, blob) in dgif_conn.execute(
            f'SELECT T_Id, "{geom_col}" FROM "{tbl}" WHERE "{geom_col}" IS NOT NULL'
        ):
            if blob is None or len(blob) < 40:
                continue
            flags = blob[3]
            envelope_type = (flags >> 1) & 0x07
            if envelope_type == 0:
                # No envelope in GP header — skip (should not happen with our writer)
                continue
            minx, maxx, miny, maxy = _struct.unpack_from('<4d', blob, 8)
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
    tlm_ds = None

    # Report
    print("\n" + "=" * 60)
    print("  ETL Transform Summary")
    print("=" * 60)
    print(f"  Total features inserted : {stats['total_inserted']}")
    print(f"  Total features skipped  : {stats['total_skipped']}")
    print(f"  No Objektart match      : {stats['total_no_match']}")
    print(f"  TLM classes not found   : {stats.get('tlm_class_not_found', 0)}")
    print(f"  DGIF class not found    : {stats.get('dgif_class_not_found', 0)}")
    print(f"  DGIF basket not found   : {stats.get('dgif_basket_not_found', 0)}")
    print(f"  Insert errors           : {stats.get('insert_error', 0)}")

    print("\n  Features per DGIF table:")
    for k, v in sorted(stats.items()):
        if k.startswith("inserted:"):
            table = k.split(":", 1)[1]
            print(f"    {table:<45} {v:>8}")

    print("=" * 60)
    return 0 if stats["total_inserted"] > 0 else 1


# ============================================================================
# CLI
# ============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="ETL Transform: swissTLM3D GeoPackage -> DGIF GeoPackage"
    )
    parser.add_argument("--tlm-gpkg", required=True, help="Path to temporary swissTLM3D GeoPackage")
    parser.add_argument("--dgif-gpkg", required=True, help="Path to target DGIF GeoPackage")
    parser.add_argument("--mapping", required=True, help="Path to swissTLM3D_to_DGIF_V3.csv")
    args = parser.parse_args()

    # Validate paths
    for label, path in [("TLM GPKG", args.tlm_gpkg), ("Mapping CSV", args.mapping)]:
        if not Path(path).exists():
            print(f"[FATAL] {label} not found: {path}", file=sys.stderr)
            sys.exit(1)

    if not Path(args.dgif_gpkg).exists():
        print(f"[FATAL] DGIF GPKG not found: {args.dgif_gpkg}", file=sys.stderr)
        sys.exit(1)

    rc = transform(args.tlm_gpkg, args.dgif_gpkg, args.mapping)
    sys.exit(rc)


if __name__ == "__main__":
    main()
