# Snowflake Data Loader for HAPI FHIR

This module contains Python-based tools for loading FHIR resources from a HAPI FHIR server (running in SPCS) into Snowflake tables.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Snowflake AI Data Cloud                         │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  Snowpark Container Services (SPCS)                            │ │
│  │  ┌──────────────────────┐                                      │ │
│  │  │   HAPI FHIR Server   │◄─── Java JPA Server (../Dockerfile)  │ │
│  │  │   + PostgreSQL       │                                      │ │
│  │  └──────────┬───────────┘                                      │ │
│  └─────────────┼──────────────────────────────────────────────────┘ │
│                │ REST API (internal SPCS network)                   │
│                ▼                                                    │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │  Snowflake Stored Procedures (Python)                          ││
│  │  ┌───────────────────────┐  ┌───────────────────────────────┐  ││
│  │  │ LOAD_FHIR_RESOURCE    │  │ Streams & Dynamic Tables      │  ││
│  │  │ (load_fhir_resource/) │──│ (claim_update/)               │  ││
│  │  └───────────────────────┘  └───────────────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────────┘│
│                │                                                    │
│                ▼                                                    │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │  Snowflake Tables                                              ││
│  │  ┌─────────────────────┐  ┌─────────────────────┐              ││
│  │  │ RAW_FHIR_RESOURCES  │  │ FLAT_FHIR_VIEW      │              ││
│  │  │ (JSON)              │──│ (Columnar)          │              ││
│  │  └─────────────────────┘  └─────────────────────┘              ││
│  └─────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
```

## Components

### load_fhir_resource/
The main Snowflake stored procedure for extracting FHIR resources from the HAPI server and loading them into Snowflake.

**Features:**
- High water mark pattern using `_lastUpdated` for incremental sync
- Pagination support for large result sets
- Sync history and watermark tracking tables
- Event table integration for log correlation

**Usage:**
```sql
CALL LOAD_FHIR_RESOURCE('Patient', 'RAW_FHIR_PATIENTS', 1000);
CALL LOAD_FHIR_RESOURCE('Claim', 'RAW_FHIR_CLAIMS', 1000);
CALL LOAD_FHIR_RESOURCE('Observation', 'RAW_FHIR_OBSERVATIONS', 1000);
```

### load_test_data/
Local utility to load the `fhir.test.data.r4` package into the HAPI FHIR server.

**Usage:**
```bash
cd load_test_data
python load_fhir_test_data.py --connection DEMO8498-christen
```

### claim_update/
Stream-based processing for flattening and transforming raw FHIR JSON into normalized tables.

**Features:**
- Snowflake Streams for CDC
- Dynamic JSON flattening with PIVOT
- Handles nested arrays and complex FHIR structures

### flat_view.py
Utility to auto-generate a flattened view from raw FHIR JSON data.

**Usage:**
```bash
python load_fhir_resource/flat_view.py RAW_FHIR_RESOURCES RESOURCE FLAT_FHIR_VIEW
```

## Setup

### Prerequisites
1. HAPI FHIR server running in SPCS (see parent directory)
2. External access integration for FHIR server connectivity
3. Snowflake CLI configured with connection

### Initial Setup
Run the SQL in `Setup.sql` to create:
- Database and schema (`HL7.RAW`)
- Warehouse (`HL7_FHIR_WH`)
- Network rule and external access integration

### Deploy Stored Procedure
```bash
cd snowflake-data-loader
snow snowpark deploy
```

## Configuration

### snowflake.yml
Defines the stored procedure deployment configuration:
- Handler: `app.main`
- External access integration: `HAPI_FHIR_ACCESS_INTEGRATION`
- Target: `HL7.RAW.LOAD_FHIR_RESOURCE`

### Environment Variables
- `SNOWFLAKE_CONNECTION_NAME`: Snowflake CLI connection to use

## Related Documentation

- [FUTURE_ENHANCEMENTS.md](./FUTURE_ENHANCEMENTS.md) - Roadmap for SMART on FHIR, write operations, and Epic integration
- Parent [README.md](../README.md) - HAPI FHIR JPA Server documentation
