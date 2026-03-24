#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_overture_dgif_v3.py
=========================
Generates the mapping table Overture Maps → DGIF V3.0.

For every Overture theme/type/subtype/class combination, finds the best
matching DGIF 3.0 class (with optional attribute/value) and writes a CSV
following the same structure as swissTLM3D_to_DGIF_V3.csv.

Input:
  - models/DGIF_V3.ili                     (DGIF 3.0 INTERLIS model)

Output:
  - models/Overture_to_DGIF_V3.csv

Overture schema reference:
  https://docs.overturemaps.org/schema/reference/

Author: Automated DGIF pipeline
"""

import csv
import re
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
ILI_DGIF = BASE / "models" / "DGIF_V3.ili"
CSV_OUT = BASE / "models" / "Overture_to_DGIF_V3.csv"

# Header columns
HEADER = [
    "NO",
    "Overture Theme",
    "Overture Type",
    "Overture Subtype",
    "Overture Class",
    "Geometry",
    "Mapping Description",
    "DGIF Feature Alpha",
    "DGIF Feature 531",
    "DGIF Attribute Alpha",
    "DGIF Attribute 531",
    "DGIF AttributeValue Alpha",
    "DGIF Value 531",
    "DGIF Attribute Alpha",
    "DGIF Attribute 531",
    "DGIF AttributeValue Alpha",
    "DGIF Value 531",
]


def extract_dgif_classes(ili_path: Path) -> set:
    """Return set of class names found in the .ili file."""
    classes = set()
    pat = re.compile(r"^\s*CLASS\s+(\w+)\s")
    with open(ili_path, encoding="utf-8") as f:
        for line in f:
            m = pat.match(line)
            if m:
                classes.add(m.group(1))
    return classes


# ══════════════════════════════════════════════════════════════════════════════
# MASTER MAPPING TABLE
# ══════════════════════════════════════════════════════════════════════════════
# Structure per row:
#   (theme, type, subtype, class,
#    geometry,
#    mapping_description,
#    dgif_class, dgif_531,
#    attr1, attr1_531, val1, val1_531,
#    attr2, attr2_531, val2, val2_531)
#
# mapping_description: OK = exact match, Generalization = approximate,
#                      not in DGIF = no equivalent

# ── 1. BUILDINGS ─────────────────────────────────────────────────────────────

BUILDINGS = [
    # ── building by subtype (when class is null/unknown) ──
    ("buildings", "building", "agricultural", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "agriculture", "2", "", "", "", ""),
    ("buildings", "building", "civic", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "publicAdministration", "808", "", "", "", ""),
    ("buildings", "building", "commercial", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "commerce", "440", "", "", "", ""),
    ("buildings", "building", "education", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "education", "850", "", "", "", ""),
    ("buildings", "building", "entertainment", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "culturalArtsEntertainment", "890", "", "", "", ""),
    ("buildings", "building", "industrial", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "manufacturing", "99", "", "", "", ""),
    ("buildings", "building", "medical", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "humanHealthActivities", "860", "", "", "", ""),
    ("buildings", "building", "military", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "government", "811", "", "", "", ""),
    ("buildings", "building", "outbuilding", "", "Polygon",
     "Generalization", "Building", "AL013", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "religious", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "religiousActivities", "930", "", "", "", ""),
    ("buildings", "building", "residential", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "service", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "utilities", "350", "", "", "", ""),
    ("buildings", "building", "transportation", "", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "transport", "480", "", "", "", ""),

    # ── building by class ──
    ("buildings", "building", "", "agricultural", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "agriculture", "2", "", "", "", ""),
    ("buildings", "building", "", "allotment_house", "Polygon",
     "Generalization", "Hut", "AL099", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "apartments", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "barn", "Polygon",
     "OK", "Barn", "AL020", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "beach_hut", "Polygon",
     "Generalization", "Hut", "AL099", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "boathouse", "Polygon",
     "Generalization", "Building", "AL013", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "bridge_structure", "Polygon",
     "Generalization", "Bridge", "AQ040", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "bungalow", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "bunker", "Polygon",
     "OK", "SurfaceBunker", "AH060", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "cabin", "Polygon",
     "Generalization", "Hut", "AL099", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "carport", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "motorVehicleParking", "535", "", "", "", ""),
    ("buildings", "building", "", "cathedral", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "placeOfWorship", "931", "", "", "", ""),
    ("buildings", "building", "", "chapel", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "placeOfWorship", "931", "", "", "", ""),
    ("buildings", "building", "", "church", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "placeOfWorship", "931", "", "", "", ""),
    ("buildings", "building", "", "civic", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "publicAdministration", "808", "", "", "", ""),
    ("buildings", "building", "", "college", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "higherEducation", "855", "", "", "", ""),
    ("buildings", "building", "", "commercial", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "commerce", "440", "", "", "", ""),
    ("buildings", "building", "", "cowshed", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "raisingOfAnimals", "9", "", "", "", ""),
    ("buildings", "building", "", "detached", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "digester", "Polygon",
     "Generalization", "StorageTank", "AM070", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "dormitory", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "dormitory", "556", "", "", "", ""),
    ("buildings", "building", "", "dwelling_house", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "factory", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "manufacturing", "99", "", "", "", ""),
    ("buildings", "building", "", "farm", "Polygon",
     "Generalization", "Farm", "EA010", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "farm_auxiliary", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "agriculture", "2", "", "", "", ""),
    ("buildings", "building", "", "fire_station", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "firefighting", "845", "", "", "", ""),
    ("buildings", "building", "", "garage", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "motorVehicleParking", "535", "", "", "", ""),
    ("buildings", "building", "", "garages", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "motorVehicleParking", "535", "", "", "", ""),
    ("buildings", "building", "", "ger", "Polygon",
     "Generalization", "Hut", "AL099", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "glasshouse", "Polygon",
     "OK", "Greenhouse", "AJ050", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "government", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "government", "811", "", "", "", ""),
    ("buildings", "building", "", "grandstand", "Polygon",
     "OK", "Grandstand", "AK061", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "greenhouse", "Polygon",
     "OK", "Greenhouse", "AJ050", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "guardhouse", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "lawEnforcement", "841", "", "", "", ""),
    ("buildings", "building", "", "hangar", "Polygon",
     "OK", "AircraftHangar", "GB230", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "hospital", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "inPatientCare", "861", "", "", "", ""),
    ("buildings", "building", "", "hotel", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "hotel", "551", "", "", "", ""),
    ("buildings", "building", "", "house", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "houseboat", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "hut", "Polygon",
     "OK", "Hut", "AL099", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "industrial", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "manufacturing", "99", "", "", "", ""),
    ("buildings", "building", "", "kindergarten", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "dayCare", "885", "", "", "", ""),
    ("buildings", "building", "", "kiosk", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "retailSale", "460", "", "", "", ""),
    ("buildings", "building", "", "library", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "library", "902", "", "", "", ""),
    ("buildings", "building", "", "manufacture", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "manufacturing", "99", "", "", "", ""),
    ("buildings", "building", "", "military", "Polygon",
     "Generalization", "MilitaryInstallation", "SU001", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "monastery", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "religiousActivities", "930", "", "", "", ""),
    ("buildings", "building", "", "mosque", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "placeOfWorship", "931", "", "", "", ""),
    ("buildings", "building", "", "office", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "officeAdministration", "801", "", "", "", ""),
    ("buildings", "building", "", "outbuilding", "Polygon",
     "Generalization", "Building", "AL013", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "parking", "Polygon",
     "OK", "ParkingGarage", "AQ141", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "pavilion", "Polygon",
     "Generalization", "Building", "AL013", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "post_office", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "postalActivities", "540", "", "", "", ""),
    ("buildings", "building", "", "presbytery", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "religiousActivities", "930", "", "", "", ""),
    ("buildings", "building", "", "public", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "publicAdministration", "808", "", "", "", ""),
    ("buildings", "building", "", "religious", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "religiousActivities", "930", "", "", "", ""),
    ("buildings", "building", "", "residential", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "retail", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "retailSale", "460", "", "", "", ""),
    ("buildings", "building", "", "roof", "Polygon",
     "Generalization", "Building", "AL013", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "school", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "education", "850", "", "", "", ""),
    ("buildings", "building", "", "semi", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "semidetached_house", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "service", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "utilities", "350", "", "", "", ""),
    ("buildings", "building", "", "shed", "Polygon",
     "OK", "Shed", "AL210", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "shrine", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "placeOfWorship", "931", "", "", "", ""),
    ("buildings", "building", "", "silo", "Polygon",
     "OK", "GrainStorageStructure", "AM020", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "slurry_tank", "Polygon",
     "Generalization", "StorageTank", "AM070", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "sports_centre", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "sportsCentre", "912", "", "", "", ""),
    ("buildings", "building", "", "sports_hall", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "sportsCentre", "912", "", "", "", ""),
    ("buildings", "building", "", "stable", "Polygon",
     "OK", "Stable", "AL175", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "stadium", "Polygon",
     "OK", "Stadium", "AK160", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "static_caravan", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "stilt_house", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "storage_tank", "Polygon",
     "OK", "StorageTank", "AM070", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "sty", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "raisingOfAnimals", "9", "", "", "", ""),
    ("buildings", "building", "", "supermarket", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "retailSale", "460", "", "", "", ""),
    ("buildings", "building", "", "synagogue", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "placeOfWorship", "931", "", "", "", ""),
    ("buildings", "building", "", "temple", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "placeOfWorship", "931", "", "", "", ""),
    ("buildings", "building", "", "terrace", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "toilets", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "restroom", "382", "", "", "", ""),
    ("buildings", "building", "", "train_station", "Polygon",
     "OK", "TransportationStation", "AQ125", "meansTransportation", "TME", "railway", "13", "", "", "", ""),
    ("buildings", "building", "", "transformer_tower", "Polygon",
     "OK", "PowerSubstation", "AD030", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "transportation", "Polygon",
     "Generalization", "TransportationStation", "AQ125", "", "", "", "", "", "", "", ""),
    ("buildings", "building", "", "trullo", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "residence", "563", "", "", "", ""),
    ("buildings", "building", "", "university", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "higherEducation", "855", "", "", "", ""),
    ("buildings", "building", "", "warehouse", "Polygon",
     "OK", "Building", "AL013", "featureFunction", "FFN", "warehousingStorage", "530", "", "", "", ""),
    ("buildings", "building", "", "wayside_shrine", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "placeOfWorship", "931", "", "", "", ""),

    # ── building_part (LOD detail — mapped generically to Building) ──
    ("buildings", "building_part", "", "", "Polygon",
     "Generalization", "BuildingSuperstructure", "AL018", "", "", "", "", "", "", "", ""),
]

# ── 2. TRANSPORTATION ────────────────────────────────────────────────────────

TRANSPORTATION = [
    # ── segment subtype=road ──
    ("transportation", "segment", "road", "motorway", "Line",
     "OK", "LandTransportationWay", "AP030", "waySignificance", "WSI", "primaryWay", "1", "", "", "", ""),
    ("transportation", "segment", "road", "trunk", "Line",
     "OK", "LandTransportationWay", "AP030", "waySignificance", "WSI", "primaryWay", "2", "", "", "", ""),
    ("transportation", "segment", "road", "primary", "Line",
     "OK", "LandTransportationWay", "AP030", "waySignificance", "WSI", "primaryWay", "3", "", "", "", ""),
    ("transportation", "segment", "road", "secondary", "Line",
     "OK", "LandTransportationWay", "AP030", "waySignificance", "WSI", "secondaryWay", "4", "", "", "", ""),
    ("transportation", "segment", "road", "tertiary", "Line",
     "OK", "LandTransportationWay", "AP030", "waySignificance", "WSI", "tertiaryWay", "5", "", "", "", ""),
    ("transportation", "segment", "road", "residential", "Line",
     "Generalization", "LandTransportationWay", "AP030", "waySignificance", "WSI", "localWay", "6", "", "", "", ""),
    ("transportation", "segment", "road", "living_street", "Line",
     "Generalization", "LandTransportationWay", "AP030", "waySignificance", "WSI", "localWay", "6", "", "", "", ""),
    ("transportation", "segment", "road", "unclassified", "Line",
     "Generalization", "LandTransportationWay", "AP030", "waySignificance", "WSI", "noInformation", "-999999", "", "", "", ""),
    ("transportation", "segment", "road", "service", "Line",
     "Generalization", "LandTransportationWay", "AP030", "waySignificance", "WSI", "localWay", "6", "", "", "", ""),
    ("transportation", "segment", "road", "pedestrian", "Line",
     "OK", "LandTransportationWay", "AP030", "waySignificance", "WSI", "localWay", "6", "meansTransportation", "MTR", "pedestrian", "4"),
    ("transportation", "segment", "road", "footway", "Line",
     "OK", "LandTransportationWay", "AP030", "meansTransportation", "MTR", "pedestrian", "4", "", "", "", ""),
    ("transportation", "segment", "road", "steps", "Line",
     "OK", "Stair", "AQ170", "", "", "", "", "", "", "", ""),
    ("transportation", "segment", "road", "path", "Line",
     "Generalization", "LandRoute", "AP050", "", "", "", "", "", "", "", ""),
    ("transportation", "segment", "road", "track", "Line",
     "Generalization", "LandRoute", "AP050", "", "", "", "", "", "", "", ""),
    ("transportation", "segment", "road", "cycleway", "Line",
     "Generalization", "LandTransportationWay", "AP030", "meansTransportation", "MTR", "bicycle", "5", "", "", "", ""),
    ("transportation", "segment", "road", "bridleway", "Line",
     "Generalization", "LandRoute", "AP050", "", "", "", "", "", "", "", ""),
    ("transportation", "segment", "road", "unknown", "Line",
     "Generalization", "LandTransportationWay", "AP030", "waySignificance", "WSI", "noInformation", "-999999", "", "", "", ""),

    # ── segment subtype=rail ──
    ("transportation", "segment", "rail", "standard_gauge", "Line",
     "OK", "Railway", "AN010", "railwayClass", "RWC", "mainLine", "1", "", "", "", ""),
    ("transportation", "segment", "rail", "narrow_gauge", "Line",
     "OK", "Railway", "AN010", "railwayClass", "RWC", "branchLine", "2", "", "", "", ""),
    ("transportation", "segment", "rail", "light_rail", "Line",
     "Generalization", "Railway", "AN010", "railwayUse", "RWU", "railRapidTransit", "3", "", "", "", ""),
    ("transportation", "segment", "rail", "subway", "Line",
     "OK", "Railway", "AN010", "railwayUse", "RWU", "undergroundRailway", "5", "", "", "", ""),
    ("transportation", "segment", "rail", "tram", "Line",
     "OK", "Railway", "AN010", "railwayUse", "RWU", "tramway", "12", "", "", "", ""),
    ("transportation", "segment", "rail", "monorail", "Line",
     "OK", "Railway", "AN010", "railwayUse", "RWU", "railRapidTransit", "25", "", "", "", ""),
    ("transportation", "segment", "rail", "funicular", "Line",
     "Generalization", "Railway", "AN010", "railwayUse", "RWU", "funicular", "15", "", "", "", ""),
    ("transportation", "segment", "rail", "unknown", "Line",
     "Generalization", "Railway", "AN010", "railwayUse", "RWU", "noInformation", "-999999", "", "", "", ""),

    # ── connector (routing node, no direct DGIF equivalent) ──
    ("transportation", "connector", "", "", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
]

# ── 3. BASE — INFRASTRUCTURE ────────────────────────────────────────────────

BASE_INFRASTRUCTURE = [
    # ── aerialway ──
    ("base", "infrastructure", "aerialway", "aerialway_station", "Point",
     "OK", "TransportationStation", "AQ125", "meansTransportation", "TME", "cableCar", "14", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "cable_car", "Line",
     "OK", "Cableway", "AT041", "cablewayType", "CAT", "aerialTramway", "5", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "chair_lift", "Line",
     "OK", "Cableway", "AT041", "cablewayType", "CAT", "chairLift", "2", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "drag_lift", "Line",
     "Generalization", "Cableway", "AT041", "cablewayType", "CAT", "teeBarLift", "7", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "gondola", "Line",
     "OK", "Cableway", "AT041", "cablewayType", "CAT", "gondolaLift", "6", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "goods", "Line",
     "Generalization", "Cableway", "AT041", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "j-bar", "Line",
     "Generalization", "Cableway", "AT041", "cablewayType", "CAT", "teeBarLift", "7", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "magic_carpet", "Line",
     "Generalization", "Cableway", "AT041", "cablewayType", "CAT", "skiTow", "3", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "mixed_lift", "Line",
     "Generalization", "Cableway", "AT041", "cablewayType", "CAT", "gondolaLift", "6", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "platter", "Line",
     "Generalization", "Cableway", "AT041", "cablewayType", "CAT", "teeBarLift", "7", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "rope_tow", "Line",
     "Generalization", "Cableway", "AT041", "cablewayType", "CAT", "teeBarLift", "7", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "t-bar", "Line",
     "OK", "Cableway", "AT041", "cablewayType", "CAT", "teeBarLift", "7", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "zip_line", "Line",
     "Generalization", "Cableway", "AT041", "cablewayType", "CAT", "other", "999", "", "", "", ""),
    ("base", "infrastructure", "aerialway", "pylon", "Point",
     "OK", "Pylon", "AT042", "", "", "", "", "", "", "", ""),

    # ── airport ──
    ("base", "infrastructure", "airport", "airport", "Mixed",
     "OK", "Aerodrome", "GB001", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "international_airport", "Mixed",
     "OK", "Aerodrome", "GB001", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "municipal_airport", "Mixed",
     "Generalization", "Aerodrome", "GB001", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "private_airport", "Mixed",
     "Generalization", "Aerodrome", "GB001", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "regional_airport", "Mixed",
     "Generalization", "Aerodrome", "GB001", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "military_airport", "Mixed",
     "OK", "Aerodrome", "GB001", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "seaplane_airport", "Mixed",
     "Generalization", "Aerodrome", "GB001", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "airstrip", "Mixed",
     "Generalization", "Aerodrome", "GB001", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "airport_gate", "Point",
     "OK", "Gate", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "apron", "Polygon",
     "OK", "Apron", "GB015", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "helipad", "Mixed",
     "OK", "Helipad", "GB030", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "heliport", "Mixed",
     "OK", "Heliport", "GB035", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "launchpad", "Point",
     "Generalization", "LaunchPad", "GB250", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "runway", "Mixed",
     "OK", "Runway", "GB055", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "stopway", "Line",
     "OK", "Stopway", "GB070", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "taxilane", "Line",
     "OK", "Taxiway", "GB075", "taxiwayType", "TXP", "apronTaxiway", "8", "", "", "", ""),
    ("base", "infrastructure", "airport", "taxiway", "Line",
     "OK", "Taxiway", "GB075", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "airport", "terminal", "Polygon",
     "OK", "TransportationStation", "AQ125", "meansTransportation", "TME", "aircraft", "12", "", "", "", ""),

    # ── barrier ──
    ("base", "infrastructure", "barrier", "barrier", "Mixed",
     "Generalization", "VehicleBarrier", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "bollard", "Point",
     "OK", "Bollard", "AL070", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "bump_gate", "Point",
     "Generalization", "Gate", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "bus_trap", "Point",
     "Generalization", "VehicleBarrier", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "cable_barrier", "Line",
     "Generalization", "Fence", "AL070", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "cattle_grid", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "chain", "Line",
     "Generalization", "VehicleBarrier", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "city_wall", "Line",
     "OK", "Wall", "AL260", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "cycle_barrier", "Point",
     "Generalization", "VehicleBarrier", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "fence", "Line",
     "OK", "Fence", "AL070", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "full-height_turnstile", "Point",
     "Generalization", "Gate", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "gate", "Point",
     "OK", "Gate", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "guard_rail", "Line",
     "Generalization", "Fence", "AL070", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "hampshire_gate", "Point",
     "Generalization", "Gate", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "handrail", "Line",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "hedge", "Line",
     "OK", "Hedgerow", "AL170", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "height_restrictor", "Point",
     "Generalization", "VehicleBarrier", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "jersey_barrier", "Line",
     "Generalization", "VehicleBarrier", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "kerb", "Line",
     "OK", "Curb", "AP010", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "kissing_gate", "Point",
     "Generalization", "Gate", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "lift_gate", "Point",
     "OK", "Gate", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "retaining_wall", "Line",
     "Generalization", "Wall", "AL260", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "sally_port", "Point",
     "Generalization", "Gate", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "stile", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "swing_gate", "Point",
     "Generalization", "Gate", "AP040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "barrier", "wall", "Line",
     "OK", "Wall", "AL260", "", "", "", "", "", "", "", ""),

    # ── bridge ──
    ("base", "infrastructure", "bridge", "bridge", "Mixed",
     "OK", "Bridge", "AQ040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "bridge", "bridge_support", "Point",
     "Generalization", "BridgePier", "AQ045", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "bridge", "boardwalk", "Line",
     "Generalization", "Bridge", "AQ040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "bridge", "cantilever", "Mixed",
     "Generalization", "Bridge", "AQ040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "bridge", "covered", "Mixed",
     "Generalization", "Bridge", "AQ040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "bridge", "movable", "Mixed",
     "OK", "Bridge", "AQ040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "bridge", "trestle", "Mixed",
     "Generalization", "Bridge", "AQ040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "bridge", "viaduct", "Mixed",
     "OK", "Bridge", "AQ040", "", "", "", "", "", "", "", ""),

    # ── communication ──
    ("base", "infrastructure", "communication", "communication_line", "Line",
     "OK", "Cable", "AT005", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "communication", "communication_pole", "Point",
     "Generalization", "Pylon", "AT042", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "communication", "communication_tower", "Point",
     "OK", "Tower", "AL241", "featureFunction", "FFN", "telecommunications", "610", "", "", "", ""),
    ("base", "infrastructure", "communication", "mobile_phone_tower", "Point",
     "Generalization", "Tower", "AL241", "featureFunction", "FFN", "telecommunications", "610", "", "", "", ""),

    # ── power ──
    ("base", "infrastructure", "power", "cable", "Line",
     "OK", "Cable", "AT005", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "power", "cable_distribution", "Point",
     "Generalization", "PowerSubstation", "AD030", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "power", "generator", "Point",
     "OK", "ElectricalPowerGenerator", "AD020", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "power", "minor_line", "Line",
     "Generalization", "Cable", "AT005", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "power", "plant", "Polygon",
     "OK", "ElectricPowerStation", "AD010", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "power", "power_line", "Line",
     "OK", "Cable", "AT005", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "power", "power_pole", "Point",
     "Generalization", "Pylon", "AT042", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "power", "power_tower", "Point",
     "OK", "Pylon", "AT042", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "power", "substation", "Polygon",
     "OK", "PowerSubstation", "AD030", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "power", "switch", "Point",
     "Generalization", "PowerSubstation", "AD030", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "power", "transformer", "Point",
     "Generalization", "PowerSubstation", "AD030", "", "", "", "", "", "", "", ""),

    # ── tower ──
    ("base", "infrastructure", "tower", "bell_tower", "Point",
     "OK", "Tower", "AL241", "featureFunction", "FFN", "religiousActivities", "930", "", "", "", ""),
    ("base", "infrastructure", "tower", "observation", "Point",
     "OK", "Lookout", "AL210", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "tower", "watchtower", "Point",
     "OK", "Lookout", "AL210", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "tower", "water_tower", "Point",
     "OK", "WaterTower", "BH220", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "tower", "cooling", "Point",
     "OK", "CoolingTower", "AF040", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "tower", "lighting", "Point",
     "Generalization", "Tower", "AL241", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "tower", "radar", "Point",
     "OK", "RadarStation", "AT045", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "tower", "siren", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "tower", "gasometer", "Polygon",
     "Generalization", "StorageTank", "AM070", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "tower", "silo", "Point",
     "Generalization", "GrainStorageStructure", "AM020", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "tower", "storage_tank", "Mixed",
     "OK", "StorageTank", "AM070", "", "", "", "", "", "", "", ""),

    # ── transit ──
    ("base", "infrastructure", "transit", "bus_station", "Point",
     "OK", "TransportationStation", "AQ125", "meansTransportation", "TME", "bus", "2", "", "", "", ""),
    ("base", "infrastructure", "transit", "bus_stop", "Point",
     "Generalization", "TransportationStation", "AQ125", "meansTransportation", "TME", "bus", "2", "", "", "", ""),
    ("base", "infrastructure", "transit", "bus_route", "Line",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transit", "ferry_terminal", "Point",
     "OK", "FerryStation", "AQ080", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transit", "platform", "Polygon",
     "OK", "TransportationPlatform", "AQ125", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transit", "railway_halt", "Point",
     "Generalization", "TransportationStation", "AQ125", "meansTransportation", "TME", "railway", "13", "", "", "", ""),
    ("base", "infrastructure", "transit", "railway_station", "Point",
     "OK", "TransportationStation", "AQ125", "meansTransportation", "TME", "railway", "13", "", "", "", ""),
    ("base", "infrastructure", "transit", "stop_position", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transit", "subway_station", "Point",
     "OK", "TransportationStation", "AQ125", "meansTransportation", "TME", "metro", "7", "", "", "", ""),
    ("base", "infrastructure", "transit", "charging_station", "Point",
     "OK", "MotorVehicleStation", "AQ170", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transit", "toll_booth", "Point",
     "Generalization", "Checkpoint", "AH025", "", "", "", "", "", "", "", ""),

    # ── transportation ──
    ("base", "infrastructure", "transportation", "crossing", "Point",
     "OK", "Crossing", "AP020", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transportation", "motorway_junction", "Point",
     "OK", "RoadInterchange", "AQ111", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transportation", "parking", "Polygon",
     "OK", "VehicleLot", "AQ140", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transportation", "parking_entrance", "Point",
     "Generalization", "EntranceExit", "AQ090", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transportation", "parking_space", "Polygon",
     "Generalization", "VehicleLot", "AQ140", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transportation", "pedestrian_crossing", "Point",
     "Generalization", "Crossing", "AP020", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "transportation", "bicycle_parking", "Point",
     "OK", "VehicleLot", "AQ140", "meansTransportation", "TME", "bicycle", "4", "", "", "", ""),
    ("base", "infrastructure", "transportation", "bicycle_rental", "Point",
     "Generalization", "Facility", "AL010", "featureFunction", "FFN", "rentalOfGoods", "480", "", "", "", ""),
    ("base", "infrastructure", "transportation", "motorcycle_parking", "Point",
     "OK", "VehicleLot", "AQ140", "meansTransportation", "TME", "motorcycle", "9", "", "", "", ""),

    # ── pedestrian ──
    ("base", "infrastructure", "pedestrian", "bench", "Point",
     "OK", "Bench", "AL015", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "pedestrian", "drinking_water", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "pedestrian", "information", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "pedestrian", "picnic_table", "Point",
     "OK", "PicnicTable", "AK121", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "pedestrian", "street_lamp", "Point",
     "OK", "StreetLamp", "AL200", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "pedestrian", "toilets", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "restroom", "382", "", "", "", ""),
    ("base", "infrastructure", "pedestrian", "viewpoint", "Point",
     "OK", "Lookout", "AL210", "", "", "", "", "", "", "", ""),

    # ── pier / quay ──
    ("base", "infrastructure", "pier", "pier", "Line",
     "OK", "ShorelineConstruction", "BB081", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "quay", "quay", "Line",
     "OK", "ShorelineConstruction", "BB081", "", "", "", "", "", "", "", ""),

    # ── recreation ──
    ("base", "infrastructure", "recreation", "camp_site", "Polygon",
     "OK", "CampSite", "AK060", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "recreation", "roller_coaster", "Line",
     "Generalization", "AmusementParkAttraction", "AK030", "", "", "", "", "", "", "", ""),

    # ── waste_management ──
    ("base", "infrastructure", "waste_management", "recycling", "Point",
     "OK", "RecyclingSite", "AB050", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "waste_management", "waste_basket", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "waste_management", "waste_disposal", "Point",
     "Generalization", "DisposalSite", "AB010", "", "", "", "", "", "", "", ""),

    # ── water infrastructure ──
    ("base", "infrastructure", "water", "dam", "Mixed",
     "OK", "Dam", "BI020", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "water", "ditch", "Line",
     "OK", "Ditch", "BH030", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "water", "drain", "Line",
     "Generalization", "Ditch", "BH030", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "water", "weir", "Mixed",
     "OK", "Dam", "BI020", "damType", "DWT", "weir", "5", "", "", "", ""),
    ("base", "infrastructure", "water", "reservoir_covered", "Polygon",
     "Generalization", "InlandWaterbody", "BH082", "", "", "", "", "", "", "", ""),

    # ── utility ──
    ("base", "infrastructure", "utility", "fire_hydrant", "Point",
     "OK", "FireHydrant", "AL020", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "utility", "hose", "Line",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "utility", "manhole", "Point",
     "Generalization", "UtilityCover", "AQ150", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "utility", "pipeline", "Line",
     "OK", "Pipeline", "AQ113", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "utility", "street_cabinet", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "utility", "utility_pole", "Point",
     "Generalization", "Pylon", "AT042", "", "", "", "", "", "", "", ""),

    # ── emergency ──
    ("base", "infrastructure", "emergency", "atm", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "emergency", "post_box", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "infrastructure", "emergency", "vending_machine", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
]

# ── 4. BASE — WATER ──────────────────────────────────────────────────────────

BASE_WATER = [
    # ── inland water (subtype != ocean) ──
    ("base", "water", "lake", "", "Polygon",
     "OK", "InlandWaterbody", "BH082", "", "", "", "", "", "", "", ""),
    ("base", "water", "pond", "", "Polygon",
     "Generalization", "InlandWaterbody", "BH082", "", "", "", "", "", "", "", ""),
    ("base", "water", "reservoir", "", "Polygon",
     "OK", "InlandWaterbody", "BH082", "", "", "", "", "", "", "", ""),
    ("base", "water", "river", "", "Mixed",
     "OK", "River", "BH140", "", "", "", "", "", "", "", ""),
    ("base", "water", "stream", "", "Line",
     "Generalization", "River", "BH140", "", "", "", "", "", "", "", ""),
    ("base", "water", "canal", "", "Mixed",
     "OK", "Canal", "BH020", "", "", "", "", "", "", "", ""),
    ("base", "water", "drain", "", "Line",
     "OK", "Ditch", "BH030", "", "", "", "", "", "", "", ""),
    ("base", "water", "ditch", "", "Line",
     "OK", "Ditch", "BH030", "", "", "", "", "", "", "", ""),
    ("base", "water", "spring", "", "Point",
     "OK", "Spring", "BH170", "", "", "", "", "", "", "", ""),
    ("base", "water", "waterfall", "", "Mixed",
     "OK", "Waterfall", "BH180", "", "", "", "", "", "", "", ""),
    ("base", "water", "wetland", "", "Polygon",
     "OK", "Wetland", "BH090", "", "", "", "", "", "", "", ""),
    ("base", "water", "wastewater", "", "Polygon",
     "Generalization", "SewageTreatmentPlant", "AB000", "", "", "", "", "", "", "", ""),
    ("base", "water", "rapids", "", "Mixed",
     "OK", "Rapids", "BH120", "", "", "", "", "", "", "", ""),
    ("base", "water", "ocean", "", "Polygon",
     "Generalization", "TidalWater", "BA040", "", "", "", "", "", "", "", ""),
    # ── physical water features ──
    ("base", "water", "physical", "cape", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "water", "physical", "bay", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "water", "physical", "strait", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
    ("base", "water", "physical", "reef", "Polygon",
     "OK", "Reef", "BD120", "", "", "", "", "", "", "", ""),
]

# ── 5. BASE — LAND ───────────────────────────────────────────────────────────

BASE_LAND = [
    ("base", "land", "physical", "cliff", "Line",
     "Generalization", "SteepTerrainFace", "DB110", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "dune", "Polygon",
     "OK", "SandDunes", "BJ040", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "glacier", "Polygon",
     "OK", "Glacier", "BJ030", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "island", "Polygon",
     "OK", "Island", "BA030", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "peninsula", "Polygon",
     "Generalization", "LandArea", "DA010", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "rock", "Point",
     "Generalization", "RockFormation", "DB160", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "sand", "Polygon",
     "Generalization", "SoilSurfaceRegion", "DA010", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "scree", "Polygon",
     "Generalization", "RockFormation", "DB160", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "wetland", "Polygon",
     "OK", "Wetland", "BH090", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "volcano", "Point",
     "OK", "Volcano", "DB180", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "valley", "Point",
     "Generalization", "LandMorphologyArea", "DA020", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "mountain_range", "Point",
     "Generalization", "Hill", "DB070", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "peak", "Point",
     "OK", "GeomorphicExtreme", "CA010", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "ridge", "Line",
     "Generalization", "RidgeLine", "DB150", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "saddle", "Point",
     "OK", "MountainPass", "DB080", "", "", "", "", "", "", "", ""),
    ("base", "land", "physical", "land", "Polygon",
     "Generalization", "LandArea", "DA010", "", "", "", "", "", "", "", ""),
]

# ── 6. BASE — LAND USE ───────────────────────────────────────────────────────

BASE_LAND_USE = [
    ("base", "land_use", "agriculture", "allotments", "Polygon",
     "OK", "AllotmentArea", "AJ110", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "agriculture", "animal_keeping", "Polygon",
     "Generalization", "Farm", "EA010", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "agriculture", "aquaculture", "Polygon",
     "Generalization", "FishFarmFacility", "BH050", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "agriculture", "farmland", "Polygon",
     "OK", "CropLand", "EA020", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "agriculture", "farmyard", "Polygon",
     "Generalization", "Farm", "EA010", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "agriculture", "greenhouse_horticulture", "Polygon",
     "OK", "Greenhouse", "AJ050", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "agriculture", "meadow", "Polygon",
     "OK", "Grassland", "EB010", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "agriculture", "orchard", "Polygon",
     "OK", "Orchard", "EA050", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "agriculture", "plant_nursery", "Polygon",
     "OK", "PlantNursery", "AJ055", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "agriculture", "vineyard", "Polygon",
     "OK", "Vineyard", "EA060", "", "", "", "", "", "", "", ""),

    ("base", "land_use", "developed", "cemetery", "Polygon",
     "OK", "Cemetery", "AL030", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "commercial", "Polygon",
     "Generalization", "ShoppingComplex", "AG040", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "construction", "Polygon",
     "OK", "ConstructionZone", "AL023", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "education", "Polygon",
     "OK", "EducationalAmenity", "AL019", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "garages", "Polygon",
     "Generalization", "VehicleLot", "AQ140", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "grave_yard", "Polygon",
     "OK", "Cemetery", "AL030", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "hospital", "Polygon",
     "Generalization", "MedicalAmenity", "AL026", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "industrial", "Polygon",
     "OK", "IndustrialPark", "AL020", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "institutional", "Polygon",
     "Generalization", "Facility", "AL010", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "landfill", "Polygon",
     "OK", "DisposalSite", "AB010", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "residential", "Polygon",
     "Generalization", "BuiltUpArea", "AL020", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "developed", "retail", "Polygon",
     "Generalization", "ShoppingComplex", "AG040", "", "", "", "", "", "", "", ""),

    ("base", "land_use", "military", "barracks", "Polygon",
     "Generalization", "MilitaryInstallation", "SU001", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "military", "base", "Polygon",
     "OK", "MilitaryInstallation", "SU001", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "military", "danger_area", "Polygon",
     "Generalization", "FiringRange", "FA015", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "military", "military", "Polygon",
     "OK", "MilitaryInstallation", "SU001", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "military", "naval_base", "Polygon",
     "Generalization", "MilitaryInstallation", "SU001", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "military", "training_area", "Polygon",
     "OK", "TrainingSite", "FA015", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "military", "trench", "Polygon",
     "Generalization", "Fortification", "AH055", "", "", "", "", "", "", "", ""),

    ("base", "land_use", "recreation", "beach_resort", "Polygon",
     "Generalization", "Facility", "AL010", "featureFunction", "FFN", "recreation", "921", "", "", "", ""),
    ("base", "land_use", "recreation", "dog_park", "Polygon",
     "Generalization", "Park", "AK120", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "recreation", "garden", "Polygon",
     "Generalization", "BotanicGarden", "EA030", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "recreation", "grass", "Polygon",
     "OK", "Grassland", "EB010", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "recreation", "park", "Polygon",
     "OK", "Park", "AK120", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "recreation", "playground", "Polygon",
     "Generalization", "SportsGround", "AK040", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "recreation", "recreation_ground", "Polygon",
     "OK", "SportsGround", "AK040", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "recreation", "village_green", "Polygon",
     "Generalization", "Park", "AK120", "", "", "", "", "", "", "", ""),

    ("base", "land_use", "protected", "national_park", "Polygon",
     "OK", "ConservationArea", "FA003", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "protected", "nature_reserve", "Polygon",
     "OK", "ConservationArea", "FA003", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "protected", "protected", "Polygon",
     "Generalization", "ConservationArea", "FA003", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "protected", "state_park", "Polygon",
     "Generalization", "ConservationArea", "FA003", "", "", "", "", "", "", "", ""),

    ("base", "land_use", "resourceExtraction", "logging", "Polygon",
     "OK", "LoggingSite", "BJ080", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "resourceExtraction", "peat_cutting", "Polygon",
     "Generalization", "ExtractionMine", "AA010", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "resourceExtraction", "quarry", "Polygon",
     "OK", "ExtractionMine", "AA010", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "resourceExtraction", "salt_pond", "Polygon",
     "OK", "SaltEvaporator", "BH155", "", "", "", "", "", "", "", ""),

    ("base", "land_use", "golf", "golf_course", "Polygon",
     "OK", "GolfCourse", "AK100", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "golf", "driving_range", "Polygon",
     "OK", "GolfDrivingRange", "AK101", "", "", "", "", "", "", "", ""),

    ("base", "land_use", "managed", "forest", "Polygon",
     "OK", "Forest", "EC015", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "managed", "flowerbed", "Polygon",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),

    ("base", "land_use", "sports", "pitch", "Polygon",
     "OK", "SportsGround", "AK040", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "sports", "stadium", "Polygon",
     "OK", "Stadium", "AK160", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "sports", "track", "Polygon",
     "Generalization", "Racetrack", "AK130", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "sports", "ski_jump", "Polygon",
     "OK", "SkiJump", "AK150", "", "", "", "", "", "", "", ""),

    ("base", "land_use", "entertainment", "theme_park", "Polygon",
     "OK", "AmusementPark", "AK030", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "entertainment", "water_park", "Polygon",
     "OK", "SwimmingPool", "AK170", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "entertainment", "zoo", "Polygon",
     "OK", "Zoo", "AK180", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "entertainment", "marina", "Polygon",
     "OK", "SmallCraftFacility", "BB009", "", "", "", "", "", "", "", ""),

    # ── education / school related ──
    ("base", "land_use", "education", "college", "Polygon",
     "OK", "EducationalAmenity", "AL019", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "education", "kindergarten", "Polygon",
     "Generalization", "EducationalAmenity", "AL019", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "education", "school", "Polygon",
     "OK", "EducationalAmenity", "AL019", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "education", "university", "Polygon",
     "OK", "EducationalAmenity", "AL019", "", "", "", "", "", "", "", ""),

    # ── transport related ──
    ("base", "land_use", "transportation", "airfield", "Polygon",
     "OK", "Aerodrome", "GB001", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "transportation", "highway", "Polygon",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", ""),
    ("base", "land_use", "transportation", "railway", "Polygon",
     "Generalization", "RailwayYard", "AN060", "", "", "", "", "", "", "", ""),

    # ── religious ──
    ("base", "land_use", "social", "religious", "Polygon",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "religiousActivities", "930", "", "", "", ""),
]

# ── 7. BASE — LAND COVER ─────────────────────────────────────────────────────

BASE_LAND_COVER = [
    ("base", "land_cover", "", "forest", "Polygon",
     "OK", "Forest", "EC015", "", "", "", "", "", "", "", ""),
    ("base", "land_cover", "", "shrub", "Polygon",
     "OK", "ShrubLand", "EB020", "", "", "", "", "", "", "", ""),
    ("base", "land_cover", "", "grass", "Polygon",
     "OK", "Grassland", "EB010", "", "", "", "", "", "", "", ""),
    ("base", "land_cover", "", "barren", "Polygon",
     "Generalization", "UnvegetatedLand", "EE020", "", "", "", "", "", "", "", ""),
    ("base", "land_cover", "", "wetland", "Polygon",
     "OK", "Wetland", "BH090", "", "", "", "", "", "", "", ""),
    ("base", "land_cover", "", "moss", "Polygon",
     "Generalization", "Tundra", "BJ110", "", "", "", "", "", "", "", ""),
    ("base", "land_cover", "", "mangrove", "Polygon",
     "Generalization", "Forest", "EC015", "", "", "", "", "", "", "", ""),
    ("base", "land_cover", "", "snow", "Polygon",
     "OK", "PermanentSnowIce", "BJ100", "", "", "", "", "", "", "", ""),
    ("base", "land_cover", "", "ice", "Polygon",
     "Generalization", "PermanentSnowIce", "BJ100", "", "", "", "", "", "", "", ""),
    ("base", "land_cover", "", "urban", "Polygon",
     "Generalization", "BuiltUpArea", "AL020", "", "", "", "", "", "", "", ""),
    ("base", "land_cover", "", "crop", "Polygon",
     "OK", "CropLand", "EA020", "", "", "", "", "", "", "", ""),
]

# ── 8. PLACES ─────────────────────────────────────────────────────────────────
# Maps the ~100 most common Overture Places basic_category values to DGIF
# following the same pattern as the OSM amenity mapping (Building + FFN).

PLACES = [
    # ── Accommodation ──
    ("places", "place", "accommodation", "hotel", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "hotel", "551", "", "", "", ""),
    ("places", "place", "accommodation", "motel", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "motel", "553", "", "", "", ""),
    ("places", "place", "accommodation", "hostel", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "hostel", "555", "", "", "", ""),
    ("places", "place", "accommodation", "guest_house", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "guestHouse", "554", "", "", "", ""),
    ("places", "place", "accommodation", "camp_site", "Point",
     "OK", "CampSite", "AK060", "", "", "", "", "", "", "", ""),
    ("places", "place", "accommodation", "resort", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "hotel", "551", "", "", "", ""),
    ("places", "place", "accommodation", "vacation_rental", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "vacationCottage", "557", "", "", "", ""),

    # ── Eat and Drink ──
    ("places", "place", "eat_and_drink", "restaurant", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "restaurant", "572", "", "", "", ""),
    ("places", "place", "eat_and_drink", "cafe", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "foodService", "570", "", "", "", ""),
    ("places", "place", "eat_and_drink", "bar", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "bar", "573", "", "", "", ""),
    ("places", "place", "eat_and_drink", "pub", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "bar", "573", "", "", "", ""),
    ("places", "place", "eat_and_drink", "fast_food", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "foodService", "570", "", "", "", ""),
    ("places", "place", "eat_and_drink", "bakery", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "foodService", "570", "", "", "", ""),
    ("places", "place", "eat_and_drink", "ice_cream_shop", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "foodService", "570", "", "", "", ""),
    ("places", "place", "eat_and_drink", "nightclub", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "nightClub", "895", "", "", "", ""),
    ("places", "place", "eat_and_drink", "brewery", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "brewing", "123", "", "", "", ""),
    ("places", "place", "eat_and_drink", "winery", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "winery", "122", "", "", "", ""),

    # ── Shopping / Retail ──
    ("places", "place", "shopping", "supermarket", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "retailSale", "460", "", "", "", ""),
    ("places", "place", "shopping", "convenience_store", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "retailSale", "460", "", "", "", ""),
    ("places", "place", "shopping", "department_store", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "nonSpecializedStore", "465", "", "", "", ""),
    ("places", "place", "shopping", "shopping_mall", "Point",
     "Generalization", "ShoppingComplex", "AG040", "", "", "", "", "", "", "", ""),
    ("places", "place", "shopping", "market", "Point",
     "OK", "Market", "AG030", "", "", "", "", "", "", "", ""),
    ("places", "place", "shopping", "pharmacy", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "pharmacy", "477", "", "", "", ""),
    ("places", "place", "shopping", "bookstore", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "specializedStore", "464", "", "", "", ""),
    ("places", "place", "shopping", "hardware_store", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "specializedStore", "464", "", "", "", ""),
    ("places", "place", "shopping", "clothing_store", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "specializedStore", "464", "", "", "", ""),
    ("places", "place", "shopping", "electronics_store", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "specializedStore", "464", "", "", "", ""),
    ("places", "place", "shopping", "pet_store", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "petShop", "478", "", "", "", ""),
    ("places", "place", "shopping", "furniture_store", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "specializedStore", "464", "", "", "", ""),
    ("places", "place", "shopping", "gas_station", "Point",
     "OK", "MotorVehicleStation", "AQ170", "", "", "", "", "", "", "", ""),

    # ── Health and Medical ──
    ("places", "place", "health_and_medical", "hospital", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "inPatientCare", "861", "", "", "", ""),
    ("places", "place", "health_and_medical", "clinic", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "outPatientCare", "862", "", "", "", ""),
    ("places", "place", "health_and_medical", "dentist", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "humanHealthActivities", "860", "", "", "", ""),
    ("places", "place", "health_and_medical", "doctor", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "humanHealthActivities", "860", "", "", "", ""),
    ("places", "place", "health_and_medical", "veterinarian", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "veterinary", "757", "", "", "", ""),
    ("places", "place", "health_and_medical", "nursing_home", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "residentialCare", "875", "", "", "", ""),

    # ── Education ──
    ("places", "place", "education", "school", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "education", "850", "", "", "", ""),
    ("places", "place", "education", "university", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "higherEducation", "855", "", "", "", ""),
    ("places", "place", "education", "college", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "higherEducation", "855", "", "", "", ""),
    ("places", "place", "education", "kindergarten", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "dayCare", "885", "", "", "", ""),
    ("places", "place", "education", "library", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "library", "902", "", "", "", ""),
    ("places", "place", "education", "driving_school", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "vocationalEducation", "857", "", "", "", ""),

    # ── Financial Services ──
    ("places", "place", "financial_service", "bank", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "financialServices", "640", "", "", "", ""),
    ("places", "place", "financial_service", "insurance", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "insurance", "651", "", "", "", ""),
    ("places", "place", "financial_service", "accountant", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "accounting", "696", "", "", "", ""),

    # ── Public Service and Government ──
    ("places", "place", "public_service", "police", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "lawEnforcement", "841", "", "", "", ""),
    ("places", "place", "public_service", "fire_station", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "firefighting", "845", "", "", "", ""),
    ("places", "place", "public_service", "post_office", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "postalActivities", "540", "", "", "", ""),
    ("places", "place", "public_service", "courthouse", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "judicialActivities", "840", "", "", "", ""),
    ("places", "place", "public_service", "embassy", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "embassy", "827", "", "", "", ""),
    ("places", "place", "public_service", "townhall", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "localGovernment", "812", "", "", "", ""),
    ("places", "place", "public_service", "prison", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "imprisonment", "843", "", "", "", ""),
    ("places", "place", "public_service", "government_office", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "government", "811", "", "", "", ""),

    # ── Arts and Entertainment ──
    ("places", "place", "arts_and_entertainment", "museum", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "museum", "905", "", "", "", ""),
    ("places", "place", "arts_and_entertainment", "theatre", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "theatre", "891", "", "", "", ""),
    ("places", "place", "arts_and_entertainment", "cinema", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "cinema", "594", "", "", "", ""),
    ("places", "place", "arts_and_entertainment", "casino", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "gambling", "909", "", "", "", ""),
    ("places", "place", "arts_and_entertainment", "aquarium", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "aquarium", "906", "", "", "", ""),
    ("places", "place", "arts_and_entertainment", "amusement_park", "Point",
     "OK", "AmusementPark", "AK030", "", "", "", "", "", "", "", ""),
    ("places", "place", "arts_and_entertainment", "zoo", "Point",
     "OK", "Zoo", "AK180", "", "", "", "", "", "", "", ""),
    ("places", "place", "arts_and_entertainment", "community_centre", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "communityCentre", "893", "", "", "", ""),

    # ── Active Life / Sports ──
    ("places", "place", "active_life", "sports_centre", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "sportsCentre", "912", "", "", "", ""),
    ("places", "place", "active_life", "swimming_pool", "Point",
     "OK", "SwimmingPool", "AK170", "", "", "", "", "", "", "", ""),
    ("places", "place", "active_life", "gym", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "sportsCentre", "912", "", "", "", ""),
    ("places", "place", "active_life", "golf_course", "Point",
     "OK", "GolfCourse", "AK100", "", "", "", "", "", "", "", ""),
    ("places", "place", "active_life", "stadium", "Point",
     "OK", "Stadium", "AK160", "", "", "", "", "", "", "", ""),

    # ── Automotive ──
    ("places", "place", "automotive", "car_dealer", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "motorVehicleRental", "761", "", "", "", ""),
    ("places", "place", "automotive", "car_wash", "Point",
     "Generalization", "Building", "AL013", "featureFunction", "FFN", "motorVehicleRepair", "343", "", "", "", ""),
    ("places", "place", "automotive", "car_repair", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "vehicleMaintenance", "486", "", "", "", ""),
    ("places", "place", "automotive", "car_rental", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "motorVehicleRental", "761", "", "", "", ""),
    ("places", "place", "automotive", "parking", "Point",
     "OK", "VehicleLot", "AQ140", "", "", "", "", "", "", "", ""),

    # ── Beauty and Spa ──
    ("places", "place", "beauty_and_spa", "hairdresser", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "beautyTreatment", "962", "", "", "", ""),
    ("places", "place", "beauty_and_spa", "spa", "Point",
     "Generalization", "Spa", "AK110", "", "", "", "", "", "", "", ""),

    # ── Professional Services ──
    ("places", "place", "professional_services", "lawyer", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "legalActivities", "691", "", "", "", ""),
    ("places", "place", "professional_services", "real_estate_agent", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "realEstateActivities", "680", "", "", "", ""),
    ("places", "place", "professional_services", "travel_agency", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "travelAgency", "775", "", "", "", ""),

    # ── Religious ──
    ("places", "place", "religious", "place_of_worship", "Point",
     "OK", "Building", "AL013", "featureFunction", "FFN", "placeOfWorship", "931", "", "", "", ""),

    # ── Attractions ──
    ("places", "place", "attractions_and_activities", "monument", "Point",
     "OK", "Monument", "AL130", "", "", "", "", "", "", "", ""),
    ("places", "place", "attractions_and_activities", "historic_site", "Point",
     "Generalization", "ArcheologicalSite", "AL012", "", "", "", "", "", "", "", ""),
    ("places", "place", "attractions_and_activities", "viewpoint", "Point",
     "OK", "Lookout", "AL210", "", "", "", "", "", "", "", ""),
    ("places", "place", "attractions_and_activities", "park", "Point",
     "OK", "Park", "AK120", "", "", "", "", "", "", "", ""),
    ("places", "place", "attractions_and_activities", "botanical_garden", "Point",
     "OK", "BotanicGarden", "EA030", "", "", "", "", "", "", "", ""),
]

# ── 9. DIVISIONS ──────────────────────────────────────────────────────────────

DIVISIONS = [
    ("divisions", "division", "country", "", "Point",
     "OK", "GeopoliticalEntity", "GE010", "", "", "", "", "", "", "", ""),
    ("divisions", "division", "dependency", "", "Point",
     "Generalization", "GeopoliticalEntity", "GE010", "", "", "", "", "", "", "", ""),
    ("divisions", "division", "macroregion", "", "Point",
     "Generalization", "AdministrativeDivision", "FA000", "", "", "", "", "", "", "", ""),
    ("divisions", "division", "region", "", "Point",
     "OK", "AdministrativeDivision", "FA000", "", "", "", "", "", "", "", ""),
    ("divisions", "division", "macrocounty", "", "Point",
     "Generalization", "AdministrativeDivision", "FA000", "", "", "", "", "", "", "", ""),
    ("divisions", "division", "county", "", "Point",
     "OK", "AdministrativeDivision", "FA000", "", "", "", "", "", "", "", ""),
    ("divisions", "division", "locality", "", "Point",
     "OK", "PopulatedPlace", "AL020", "", "", "", "", "", "", "", ""),
    ("divisions", "division", "borough", "", "Point",
     "Generalization", "Neighbourhood", "AL024", "", "", "", "", "", "", "", ""),
    ("divisions", "division", "neighborhood", "", "Point",
     "OK", "Neighbourhood", "AL024", "", "", "", "", "", "", "", ""),
    ("divisions", "division", "microhood", "", "Point",
     "Generalization", "Neighbourhood", "AL024", "", "", "", "", "", "", "", ""),

    # ── division_area (polygon admin boundaries) ──
    ("divisions", "division_area", "country", "", "Polygon",
     "OK", "GeopoliticalEntity", "GE010", "", "", "", "", "", "", "", ""),
    ("divisions", "division_area", "region", "", "Polygon",
     "OK", "AdministrativeDivision", "FA000", "", "", "", "", "", "", "", ""),
    ("divisions", "division_area", "county", "", "Polygon",
     "OK", "AdministrativeDivision", "FA000", "", "", "", "", "", "", "", ""),
    ("divisions", "division_area", "locality", "", "Polygon",
     "Generalization", "PopulatedPlace", "AL020", "", "", "", "", "", "", "", ""),

    # ── division_boundary (admin boundary lines) ──
    ("divisions", "division_boundary", "", "", "Line",
     "OK", "AdministrativeBoundary", "FA001", "", "", "", "", "", "", "", ""),
]

# ── 10. ADDRESSES ─────────────────────────────────────────────────────────────

ADDRESSES = [
    ("addresses", "address", "", "", "Point",
     "not in DGIF", "", "", "", "", "", "", "", "", "", ""),
]


# ══════════════════════════════════════════════════════════════════════════════
# Assembly and validation
# ══════════════════════════════════════════════════════════════════════════════

ALL_MAPPINGS = (
    BUILDINGS
    + TRANSPORTATION
    + BASE_INFRASTRUCTURE
    + BASE_WATER
    + BASE_LAND
    + BASE_LAND_USE
    + BASE_LAND_COVER
    + PLACES
    + DIVISIONS
    + ADDRESSES
)


def validate_mappings(dgif_classes: set):
    """Check that all mapped DGIF classes exist in the .ili model."""
    issues = []
    for i, row in enumerate(ALL_MAPPINGS, 1):
        dgif_cls = row[6]
        if dgif_cls and dgif_cls not in dgif_classes:
            issues.append(f"Row {i}: DGIF class '{dgif_cls}' not in model [{row[0]}/{row[1]}/{row[2]}/{row[3]}]")
    return issues


def write_csv():
    """Write the mapping CSV."""
    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(HEADER)
        for i, row in enumerate(ALL_MAPPINGS, 1):
            csv_row = [str(i)] + list(row)
            writer.writerow(csv_row)
    return len(ALL_MAPPINGS)


def main():
    print("build_overture_dgif_v3.py")
    print("=" * 60)

    # Extract DGIF classes
    dgif_classes = extract_dgif_classes(ILI_DGIF)
    print(f"DGIF V3 classes in model: {len(dgif_classes)}")
    print(f"Mapping rows to write:    {len(ALL_MAPPINGS)}")

    # Validate
    issues = validate_mappings(dgif_classes)
    if issues:
        print(f"\n[WARNING] {len(issues)} validation issues:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("\n[OK] All DGIF classes validated against model")

    # Statistics
    from collections import Counter
    themes = Counter(r[0] for r in ALL_MAPPINGS)
    mapped = sum(1 for r in ALL_MAPPINGS if r[6])
    not_dgif = sum(1 for r in ALL_MAPPINGS if r[5] == "not in DGIF")
    ok = sum(1 for r in ALL_MAPPINGS if r[5] == "OK")
    gen = sum(1 for r in ALL_MAPPINGS if r[5] == "Generalization")

    print(f"\nStats:")
    print(f"  Mapped to DGIF:      {mapped}")
    print(f"  Not in DGIF:         {not_dgif}")
    print(f"  Exact (OK):          {ok}")
    print(f"  Generalization:      {gen}")
    print(f"\n  By theme:")
    for theme, cnt in sorted(themes.items()):
        print(f"    {theme}: {cnt}")

    # Write CSV
    n = write_csv()
    print(f"\n[OK] Written {n} rows to {CSV_OUT}")


if __name__ == "__main__":
    main()
