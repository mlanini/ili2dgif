# ili2dgif

**An implementation of the Defence Geospatial Information Framework (DGIF) 3.0 using INTERLIS 2.4**

This aims at setting up an end-to-end, fully automated pipeline that takes the DGIF 3.0 UML model — maintained by the [Defence Geospatial Information Working Group (DGIWG)](https://dgiwg.org/) — and produces a standards-compliant Swiss geospatial data stack:

1. **INTERLIS 2.4 model** (`models/DGIF_V3.ili`) — 673 classes, 21 topics, 0 compiler errors
2. **GeoPackage** (`output/DGIF_V3.gpkg`) — OGC/DGIWG-conformant schema in WGS84
3. **INTERLIS XML catalogues** (`models/DGFCD_*.xml`, `models/DGRWI_*.xml`) — DGFCD + DGRWI concept dictionaries
4. **Mapping tables** — OSM ↔ DGIF V3 (1,657 rows) and swissTLM3D ↔ DGIF V3 (215 rows)
5. **ETL pipeline** — populates the DGIF GeoPackage with real-world
   [swissTLM3D](https://www.swisstopo.admin.ch/en/landscape-model-swisstlm3d)
   data from swisstopo, reprojected from LV95 to WGS84

All scripts are pure Python. The only external runtime dependency is Java (for
the INTERLIS toolchain: ili2c, ili2gpkg, ilivalidator).

## Overview

This project implements an **automated pipeline** to transform the UML model of the **Defence Geospatial Information Framework (DGIF) 3.0** — exported as an XMI file from Enterprise Architect — into a data model compliant with the Swiss standard **INTERLIS 2.4 / eCH-0031**, generate a **GeoPackage** conforming to the DGIWG profile, and update the **mapping tables** between OpenStreetMap / swissTLM3D and DGIF from version 2.0 to version 3.0.

---

## Normative references

| # | Document | Edition | Date |
|---|----------|---------|------|
| DGIWG 200-3 | [Defence Geospatial Information Framework (DGIF) — Overview](https://dgiwg.org/documents/dgiwg-standards/200) | 3.0 | 2024-07-19 |
| DGIWG 205-3 | [Defence Geospatial Information Model (DGIM)](https://dgiwg.org/documents/dgiwg-standards/200) | 3.0 | 2024-07-19 |
| DGIWG 206-3 | [Defence Geospatial Feature Concept Dictionary (DGFCD)](https://dgiwg.org/documents/dgiwg-standards/200) | 3.0 | 2024-07-19 |
| DGIWG 207-3 | [Defence Geospatial Real World Object Index (DGRWI)](https://dgiwg.org/documents/dgiwg-standards/200) | 3.0 | 2024-07-19 |
| DGIWG 208-3 | [Defence Geospatial Encoding Specification — Part 1: GML](https://dgiwg.org/documents/dgiwg-standards/200) | 3.0 | 2024-07-25 |
| DGIWG 126 | [DGIWG GeoPackage Profile](https://dgiwg.org/documents/dgiwg-standards/200) | 1.1 | 2025-05-02 |
| DGIWG 200-3-BL2025-1 | [DGIF Normative Content — Baseline 2025-1](https://dgiwg.org/documents/dgiwg-standards/200) (this project) | 3.0 | 2025-1 |

> **Source:** [DGIWG Standards — Series 200](https://dgiwg.org/documents/dgiwg-standards/200)

---

## What is INTERLIS?

**INTERLIS** is a Swiss federal standard (SN 612030 / eCH-0031) for describing and exchanging geospatial data models and transfer datasets. It was developed by the Swiss Federal Directorate of Cadastral Surveying and has been legally mandated for official geodata in Switzerland since 2008 [Geospatial Information Act](https://www.fedlex.admin.ch/eli/cc/2008/388/en).

### Key characteristics

| Aspect | Description |
|--------|-------------|
| **Model-driven approach** | Data models are described in a formal, platform-independent language (`.ili` files) before any implementation |
| **Separation of concerns** | The conceptual model is strictly separated from the transfer format and the physical database schema |
| **Compiler-verifiable** | Models can be validated by a compiler (`ili2c`) before data is ever produced — catching errors early |
| **Transfer format** | The standardised transfer format (INTERLIS 2 / XTF) is a well-defined XML schema generated from the model |
| **Tool ecosystem** | A rich open-source toolchain exists: `ili2c` (compiler), `ili2db` (database import/export), `ili2gpkg`, `ilivalidator`, etc. |
| **Multi-language support** | Models can carry multilingual metadata (DE, FR, IT, EN) |

### Advantages over pure UML / GML / Shapefile

1. **Formal semantics** — Unlike UML, INTERLIS models have precise, machine-readable transfer rules
2. **Validation** — Data can be validated against the model at any point (`ilivalidator`)
3. **Interoperability** — One model generates multiple outputs: GeoPackage, PostGIS, Oracle Spatial, GML, XTF
4. **Legal compliance** — Mandatory for Swiss NSDI (NGDI) and federal geodata catalogues
5. **Versioning** — Built-in model versioning and dependency management via model repositories

### Why INTERLIS in this project?

The Defence Geospatial Information Model 3.0 (673 classes, 64 associations) is transformed into INTERLIS 2.4 to:
- Leverage the Swiss geospatial infrastructure and model repositories
- Enable automatic generation of GeoPackage, PostGIS and XTF transfer files
- Validate the data model formally with `ili2c` (0 errors achieved)
- Align with the Swiss eCH-0031 v2.1.0 standard for geospatial data modelling

> **Reference:** [INTERLIS website](https://www.interlis.ch) ·
> [eCH-0031](https://www.ech.ch/de/ech/ech-0031) ·
> [ili2db tools](https://github.com/claeis/ili2db)

---

## Pipeline — Step by step

### Step 1 — INTERLIS XML Catalogues

**Script:** `scripts/extract_dgfcd_dgrwi_catalogs.py` (423 lines)

Extracts the DGFCD and DGRWI concept dictionaries from the XMI file and serialises
them into **7 XML catalogues** conforming to the INTERLIS `CatalogueObjects_V2` format:

| Catalogue | Content |
|-----------|---------|
| `models/DGFCD_FeatureConcepts.xml` | Feature Concepts (geospatial object classes) |
| `models/DGFCD_AttributeConcepts.xml` | Feature Concept attributes |
| `models/DGFCD_AttributeDataTypes.xml` | Attribute data types |
| `models/DGFCD_AttributeValueConcepts.xml` | Permitted attribute values |
| `models/DGFCD_RoleConcepts.xml` | Association roles |
| `models/DGFCD_UnitsOfMeasure.xml` | Units of measure |
| `models/DGRWI_RealWorldObjects.xml` | Real-world objects mapped to Feature Concepts |

**Run:**
```bash
python scripts/extract_dgfcd_dgrwi_catalogs.py
```

---

### Step 2 — INTERLIS 2.4 Model

**Script:** `scripts/generate_ili_model.py` (1,135 lines)

Transforms the UML/XMI model into an `.ili` file compliant with INTERLIS 2.4 and eCH-0031 v2.1.0.

**UML → INTERLIS transformation rules:**

| UML Element | INTERLIS Element |
|-------------|------------------|
| Package DGIM | `MODEL DGIF_V3` |
| Thematic sub-packages | `TOPIC` (21 topics) |
| `uml:Class` | `CLASS` with OID (673 classes) |
| `ownedAttribute` | `ATTRIBUTE` with cardinality |
| `uml:Generalization` | `EXTENDS` |
| `uml:Association` | `ASSOCIATION` (64 associations) |
| `uml:Enumeration` | Inline `DOMAIN` enumerations |
| `uml:DataType` | `STRUCTURE` |
| AttributeDataTypes | Mapping to INTERLIS base types |

**Key features:**

- **Topological sorting** of classes to respect `EXTENDS` dependencies
- **Cross-topic references** handled with `EXTERNAL`
- **9 geometry attributes** mapped to native types:
  - `POINT` → `GeometryCHLV95_V2.Coord2`
  - `LINESTRING` → `GeometryCHLV95_V2.Line`
  - `POLYGON` → `GeometryCHLV95_V2.Surface`
- **Validation:** 0 ili2c errors

**Run:**
```bash
python scripts/generate_ili_model.py
```

**Validate:**
```bash
java -jar ressources/ili2c-5.6.8/ili2c.jar --check models/DGIF_V3.ili
```

---

### Step 3 — GeoPackage

**Script:** `scripts/generate_gpkg.py` (Python)

Generates a GeoPackage conforming to the DGIWG profile (STD-08-006) using `ili2gpkg 5.5.1`.

**Input:** `models/DGIF_V3.ili`

**Inheritance strategy:** `--noSmartMapping` (one table per class) because:
- `smart1Inheritance` → "too many columns" error on the Entity base class (600+ subclasses)
- `smart2Inheritance` → "duplicate column T_Id" bug with self-referential associations

**Key options:**

| Option | Reason |
|--------|--------|
| `--defaultSrsCode 4326` | WGS84 as per DGIWG profile |
| `--noSmartMapping` | NewClass for every class |
| `--nameByTopic` | `Topic.Class` names to avoid conflicts |
| `--gpkgMultiGeomPerTable` | Multi-geometry support |
| `--createGeomIdx` | Spatial indices (DGIWG requirement) |
| `--strokeArcs` | Arcs → line segments for compatibility |
| `--createEnumTabs` | Lookup tables for enumerations |

**Result:** `output/DGIF_V3.gpkg` — 22.5 MB, **1,457 tables** (9 with geometry, 1,448 attribute-only), SRID 4326.

**Run:**
```bash
python scripts/generate_gpkg.py
```

---

### Step 4 — OSM ↔ DGIF V3 Mapping Table

**Script:** `scripts/build_osm_dgif_v3.py` (~480 lines)

Updates the mapping table between OpenStreetMap tags and DGIF classes from version 2.0 to version 3.0, based on three sources:

1. **V2 CSV** (`models/OSM_to_DGIF_V2.csv`) — 1,610 rows, 26 OSM categories
2. **INTERLIS V3 model** (`models/DGIF_V3.ili`) — 673 classes in 21 topics
3. **OSM wiki Map_features** — current state of OSM tags

**Operations performed:**

#### a) DGIF class renames V2 → V3

| V2 Class | V3 Class | Rows affected |
|----------|----------|---------------|
| `MemorialMonument` | `Monument` | 9 |
| `Route` | `LandRoute` | 11 |
| `CaravanPark` | `CampSite` | 2 |
| `ArchaeologicalSite` | `ArcheologicalSite` | 1 |

#### b) Upgraded mappings previously marked "not in DGIF"

| OSM Tag | New DGIF Class | Mapping Type |
|---------|----------------|--------------|
| `amenity=bicycle_rental` | `Facility` (AL010) | Generalization |
| `leisure=summer_camp` | `CampSite` (AK060) | Generalization |

#### c) 47 new OSM tags added

New categories and tags added based on the OSM wiki (as of 2025/2026):

- **aeroway:** `spaceport`, `aircraft_crossing`
- **amenity:** `boat_rental`, `vehicle_inspection`, `conference_centre`, `events_venue`, `music_venue`, `parcel_locker`
- **boundary:** `forest`, `hazard`, `low_emission_zone`
- **highway:** `busway`, `via_ferrata`
- **historic:** `aqueduct`, `bomb_crater`
- **landuse:** `education`, `logging`, `aquaculture`, `animal_keeping`, `depot`, `greenery`, `winter_sports`
- **leisure:** `disc_golf_course`, `escape_game`, `fitness_centre`, `fitness_station`
- **military:** `academy`, `base`
- **natural:** `bare_rock`
- **power:** `connection`, `switchgear`
- **railway:** `proposed`, `tram_level_crossing`, `wash`
- **shop:** `health_food`, `cannabis`
- **telecom:** `exchange`, `data_centre` *(new OSM category)*
- **tourism:** `camp_pitch`
- **water:** `river`, `lake`, `reservoir`, `pond`, `canal`, `lagoon` *(new OSM primary key)*
- **waterway:** `tidal_channel`, `pressurised`

#### d) Final statistics

| Metric | V2 | V3 | Delta |
|--------|----|----|-------|
| Total rows | 1,610 | 1,657 | +47 |
| OK mappings | ~704 | 709 | +5 |
| Generalisations | ~725 | 729 | +4 |
| not in DGIF | ~225 | 218 | −7 |
| V3 classes covered | — | 158 / 673 | — |

The 515 unmapped V3 classes are predominantly specialist (aviation, maritime navigation, metadata, instrument procedures) with no direct equivalent in OSM tags.

**Run:**
```bash
python scripts/build_osm_dgif_v3.py
```

---

### Step 5 — swissTLM3D ↔ DGIF V3 Mapping Table

**Script:** `scripts/build_swisstlm3d_dgif_v3.py`

**Input:** swissTLM3D INTERLIS model (`models/swissTLM3D_ili2_V2_4.ili`) and `models/DGIF_V3.ili`.

**Output:** `models/swissTLM3D_to_DGIF_V3.csv` — 215 rows, 93 distinct DGIF classes, 0 errors.

**Approach:**

The swissTLM3D model (INTERLIS 2.3, LV95 coordinates) is organised into 7 Topics with ~25 concrete classes. Each class uses an enumerative `Objektart` attribute to distinguish subtypes. The script maps every `TLM_Class + Objektart` combination to the corresponding DGIF V3 class with optional attribute/value pairs.

#### Coverage by Topic

| TLM Topic | Classes | Total Objektart | OK | Generalisation | not in DGIF |
|-----------|---------|------------------|----|----------------|-------------|
| TLM_AREALE | 4 | 40 | 18 | 21 | 0 |
| TLM_BAUTEN | 10 | 48 | 22 | 24 | 1 |
| TLM_BB | 2 | 16 | 8 | 7 | 0 |
| TLM_EO | 1 | 10 | 5 | 5 | 0 |
| TLM_GEWAESSER | 2 | 9 | 5 | 4 | 0 |
| TLM_NAMEN | 5 | 25 | 7 | 18 | 0 |
| TLM_OEV | 4 | 18 | 12 | 6 | 0 |
| TLM_STRASSEN | 4 | 49 | 6 | 37 | 6 |
| **Total** | **32** | **215** | **83** | **125** | **7** |

#### CSV format

Identical to OSM_to_DGIF_V3.csv, with adapted columns:

| Column | Description |
|--------|-------------|
| `NO` | Sequential number |
| `TLM Topic` | INTERLIS topic (e.g. `TLM_AREALE`) |
| `TLM Feature Class` | swissTLM3D class (e.g. `TLM_FREIZEITAREAL`) |
| `TLM Attribute (Objektart)` | Always `Objektart` |
| `TLM Attribute Value` | Enumerative value (e.g. `Campingplatzareal`) |
| `Geometry` | `Point` / `Line` / `Polygon` / empty |
| `Mapping Description` | `OK` / `Generalization` / `not in DGIF` |
| `DGIF Feature Alpha` | DGIF class name (e.g. `CampSite`) |
| `DGIF Feature 531` | FACC code (e.g. `AK060`) |
| Columns 10–17 | Up to 2 DGIF attribute/value pairs |

#### Unmapped elements (7)

| TLM Class | Objektart | Reason |
|-----------|-----------|--------|
| TLM_GEBAEUDE_FOOTPRINT | Lueftungsschacht | No DGIF equivalent |
| TLM_STRASSE | Klettersteig | Via ferrata not modelled |
| TLM_STRASSENINFO | Erschliessung | Internal infrastructure node |
| TLM_STRASSENINFO | MISTRA_Zusatzknoten | CH-specific MISTRA node |
| TLM_STRASSENINFO | Standardknoten | Topological node |
| TLM_STRASSENINFO | Zahlstelle | Toll station |
| TLM_STRASSENINFO | Namen | Toponym label |

**Run:**
```bash
python scripts/build_swisstlm3d_dgif_v3.py
```

---

### Step 6 — ETL Pipeline: swissTLM3D XTF → DGIF GeoPackage

**Scripts:**
- `scripts/etl_swisstlm3d_to_dgif.py` — Python orchestrator
- `scripts/etl_swisstlm3d_transform.py` — Python transform & load

**Input:**
- swissTLM3D XTF archive from [data.geo.admin.ch](https://data.geo.admin.ch/ch.swisstopo.swisstlm3d/)
- `models/DGIF_V3.ili` — DGIF INTERLIS model
- `dgiwg_docs/swissTLM3D_to_DGIF_V3.csv` — mapping table (215 rules)

**Output:** `output/DGIF_swissTLM3D.gpkg` — DGIF-conformant GeoPackage populated with swissTLM3D data in WGS84 (EPSG:4326).

**Architecture:**

The pipeline runs in 6 phases:

| Phase | Tool | Description |
|-------|------|-------------|
| 1 — Download | Python | Downloads the swissTLM3D XTF ZIP archive (~3.6 GB) |
| 2 — Extract | Python | Extracts the 8 `.xtf` files from the ZIP (~28 GB uncompressed) |
| 2b — Validate | ilivalidator | Validates the data against the INTERLIS model (`--modeldir`, `--logtime`); generates text log and XTF error log. Non-blocking: pipeline continues on validation errors (official swisstopo data may contain minor model deviations). Skippable with `--skip-validation` |
| 3 — Schema | ili2gpkg | Creates an empty DGIF GeoPackage via `--schemaimport` with `models/DGIF_V3.ili` (same options as Step 3: `--noSmartMapping`, `--nameByTopic`, SRID 4326) |
| 4 — Import | ili2gpkg | Imports each XTF file into a temporary swissTLM3D GeoPackage (`--import`, SRID 2056, `--nameByTopic`) |
| 5 — Transform | Python/OGR | Reads the TLM GeoPackage, applies the mapping CSV, reprojects LV95→WGS84, and inserts features into the DGIF GeoPackage |

**Attribute mapping:**

| TLM source | DGIF target | Notes |
|------------|-------------|-------|
| `OID` (UUID) | `uniqueUniversalEntityIdentifier` | Mandatory in Entity base class |
| `Datum_Erstellung` | `beginLifespanVersion` | Mandatory in Entity base class |
| `Name` | — | Stored via `T_Ili_Tid` for traceability |
| `Objektart` enum value | DGIF class + attribute/value | Per CSV mapping rules |

**Coordinate reprojection:**

All geometries are reprojected from LV95 (EPSG:2056) to WGS84 (EPSG:4326) and
flattened from 3D to 2D (DGIF uses `Coord2`).

**Discarded features:**

Seven `Objektart` values marked as "not in DGIF" in the mapping table are silently
discarded (see Step 5 — Unmapped elements).

**Run:**
```bash
python scripts/etl_swisstlm3d_to_dgif.py

# To skip re-downloading:
python scripts/etl_swisstlm3d_to_dgif.py --skip-download

# To skip download, extraction, and validation:
python scripts/etl_swisstlm3d_to_dgif.py --skip-download --skip-extract --skip-validation
```

---

## Prerequisites

| Component | Version | Path |
|-----------|---------|------|
| Python | 3.12+ | System Python or QGIS-bundled Python |
| Java (JRE) | ≥ 11 | In system `PATH` |
| ili2c | 5.6.8 | `ressources/ili2c-5.6.8/ili2c.jar` |
| ili2gpkg | 5.5.1 | `ressources/ili2gpkg-5.5.1/ili2gpkg-5.5.1.jar` |
| ilivalidator | 1.15.0 | `ressources/ilivalidator-1.15.0/ilivalidator-1.15.0.jar` |
| GDAL/OGR | 3.10+ | Bundled with QGIS or installed separately |

> **Note:** Steps 1–5 use only the Python standard library. Step 6 additionally
> requires GDAL/OGR Python bindings (e.g. from a QGIS installation or `pip install GDAL`).
> All scripts are pure Python — no PowerShell required.
>
> **Java tools:** Download [ili2gpkg](https://github.com/claeis/ili2db/releases),
> [ili2c](https://github.com/claeis/ili2c/releases), and
> [ilivalidator](https://github.com/claeis/ilivalidator/releases)
> and extract them into `ressources/`. These directories are excluded from git
> via `.gitignore`.

---

## Running the full pipeline

```bash
# Step 1 — XML Catalogues
python scripts/extract_dgfcd_dgrwi_catalogs.py

# Step 2 — INTERLIS Model
python scripts/generate_ili_model.py

# Step 2b — Validation
java -jar ressources/ili2c-5.6.8/ili2c.jar --check models/DGIF_V3.ili

# Step 3 — GeoPackage
python scripts/generate_gpkg.py

# Step 4 — OSM↔DGIF V3 Table
python scripts/build_osm_dgif_v3.py

# Step 5 — swissTLM3D↔DGIF V3 Table
python scripts/build_swisstlm3d_dgif_v3.py

# Step 6 — ETL: swissTLM3D XTF → DGIF GeoPackage
python scripts/etl_swisstlm3d_to_dgif.py
```

---

## OSM ↔ DGIF CSV format

The CSV (`;` delimiter) has the following structure:

| Column | Description |
|--------|-------------|
| `NO` | Sequential number |
| `OSM Feature Class` | Category + geometry (e.g. `amenity_Point`) |
| `OSM MCE FieldName` | OSM tag key (e.g. `amenity`) |
| `OSM Attribute Value` | OSM tag value (e.g. `hospital`) |
| `OSM Attribute definition` | Description from the OSM wiki |
| `Mapping Description` | `OK` / `Generalization` / `not in DGIF` |
| `DGIF Feature Alpha` | DGIF class name (e.g. `Building`) |
| `DGIF Feature 531` | 5-character FACC code (e.g. `AL013`) |
| `DGIF Attribute Alpha` | DGIF attribute name (e.g. `featureFunction`) |
| `DGIF Attribute 531` | FACC attribute code (e.g. `FFN`) |
| `DGIF AttributeValue Alpha` | Attribute value (e.g. `hospital`) |
| `DGIF Value 531` | FACC value code (e.g. `830`) |
| Columns 13–16 | Optional second attribute/value pair |

---

## Licence

This project is released under the [MIT Licence](LICENSE).
