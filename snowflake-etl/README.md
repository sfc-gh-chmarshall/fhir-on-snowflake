# Snowflake ETL for HAPI FHIR

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
│  │             │                                                  │ │
│  │             │ Internal SPCS DNS                                │ │
│  │             │ (fhir-server.<pool>.svc.spcs.internal:8080)       │ │
│  │             │                                                  │ │
│  │             ▼                                                  │ │
│  │  ┌───────────────────────┐                                     │ │
│  │  │ LOAD_FHIR_RESOURCE    │  Snowflake Stored Procedure         │ │
│  │  │ (load_fhir_resource/) │                                     │ │
│  │  └───────────────────────┘                                     │ │
│  └────────────────────────────────────────────────────────────────┘ │
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

## Quick Start

### Option 1: Run from Snowflake (Stored Procedure)

Deploy and call the stored procedure directly in Snowflake:

```sql
-- Load Patient resources into a table
CALL HL7.RAW.LOAD_FHIR_RESOURCE('Patient', 'RAW_FHIR_PATIENTS');
```

See [Deploying to Snowflake](#deploying-to-snowflake) for setup instructions.

### Option 2: Run Locally (CLI)

Run the loader from your local machine against an external FHIR endpoint:

```bash
cd snowflake-etl
python cli.py Patient RAW_FHIR_PATIENTS --connection my-connection
```

See [Running Locally](#running-locally) for setup instructions.

---

## Running from Snowflake

### Procedure Signature
```sql
LOAD_FHIR_RESOURCE(
    resource_type STRING DEFAULT 'Claim',
    target_table STRING DEFAULT 'RAW_FHIR_RESOURCES',
    event_table STRING DEFAULT 'SNOWFLAKE.TELEMETRY.EVENTS',
    fhir_base_url STRING DEFAULT 'http://fhir-server:8080/fhir'
)
RETURNS STRING
```

### Examples
```sql
-- Load with defaults (Claim resources)
CALL HL7.RAW.LOAD_FHIR_RESOURCE();

-- Load specific resource type
CALL HL7.RAW.LOAD_FHIR_RESOURCE('Patient', 'RAW_FHIR_PATIENTS');

-- Load Observations with custom event table
CALL HL7.RAW.LOAD_FHIR_RESOURCE('Observation', 'RAW_FHIR_OBSERVATIONS', 'MY_DB.MY_SCHEMA.MY_EVENT_TABLE');

-- Load from a different FHIR server
CALL HL7.RAW.LOAD_FHIR_RESOURCE('Patient', 'RAW_FHIR_PATIENTS', 'SNOWFLAKE.TELEMETRY.EVENTS', 'http://other-fhir-server:8080/fhir');
```

### Return Value
Returns a JSON object with sync status:
```json
{
  "status": "success",
  "count": 150,
  "run_id": "abc-123-def",
  "session_id": "12345678",
  "events_query": "SELECT * FROM SNOWFLAKE.TELEMETRY.EVENTS WHERE ...",
  "message": "Loaded 150 Patient resources into RAW_FHIR_PATIENTS"
}
```

---

## Running Locally

The `cli.py` script lets you run the FHIR loader from your local machine, useful for development and testing against external FHIR endpoints.

### Local Setup

1. Create and activate the conda environment:
   ```bash
   cd snowflake-etl
   conda env create -f environment.local.yml
   conda activate fhir-on-snowflake
   ```

2. Ensure you have a Snowflake connection configured in `~/.snowflake/connections.toml`

### CLI Usage
```bash
python cli.py <resource_type> <target_table> [options]
```

### CLI Options
| Option | Default | Description |
|--------|---------|-------------|
| `--connection` | `default` | Snowflake connection name from connections.toml |
| `--fhir-url` | External SPCS endpoint | FHIR server base URL |
| `--event-table` | `SNOWFLAKE.TELEMETRY.EVENTS` | Event table for log correlation |

### CLI Examples
```bash
# Load Patient resources using default connection
python cli.py Patient RAW_FHIR_PATIENTS

# Load using a specific connection
python cli.py Patient RAW_FHIR_PATIENTS --connection my-prod-connection

# Load from a custom FHIR endpoint
python cli.py Claim RAW_FHIR_CLAIMS --fhir-url https://hapi.fhir.org/baseR4

# Load with all options
python cli.py Observation RAW_FHIR_OBS \
  --connection my-connection \
  --fhir-url https://<endpoint>.snowflakecomputing.app/fhir \
  --event-table MY_DB.LOGS.EVENTS
```

---

## Deploying to Snowflake

### Prerequisites
1. HAPI FHIR server running in SPCS (see parent directory)
2. Snowflake CLI installed and configured

> **Note:** No external access integration is required. The stored procedure communicates with the FHIR server using internal SPCS DNS names.

### Initial Setup
Run the SQL in `Setup.sql` to create the required objects:
```bash
snow sql -f Setup.sql
```

This creates:
- Database and schema (`HL7.RAW`)
- Warehouse (`HL7_FHIR_WH`)

### Deploy the Stored Procedure
```bash
cd snowflake-etl

# Build the artifacts
snow snowpark build

# Deploy to Snowflake
snow snowpark deploy
```

To update an existing deployment:
```bash
snow snowpark deploy --replace
```

---

## Components

### load_fhir_resource/
The Snowflake stored procedure source code. This directory is packaged and deployed to Snowflake.

**Features:**
- High water mark pattern using `_lastUpdated` for incremental sync
- Pagination support for large result sets
- Automatic creation of tracking tables
- Event table integration for log correlation

### cli.py
Local CLI wrapper for development and testing. NOT deployed to Snowflake.

### load_test_data/
Utility to load the `fhir.test.data.r4` package into the HAPI FHIR server.

```bash
cd load_test_data
python load_fhir_test_data.py --connection my-connection
```

### flat_view.py
Utility to auto-generate a flattened view from raw FHIR JSON data.

```bash
python load_fhir_resource/flat_view.py RAW_FHIR_RESOURCES RESOURCE FLAT_FHIR_VIEW
```

---

## Configuration

### snowflake.yml
Defines the stored procedure deployment:
- **Handler:** `app.main`
- **Target:** `HL7.RAW.LOAD_FHIR_RESOURCE`
- **Stage:** `HL7.RAW.deployments`

### environment.local.yml
Conda environment for local CLI development. Not used for Snowflake deployment.

### Tracking Tables
The procedure automatically creates these tables on first run:
- `HL7.RAW.FHIR_SYNC_WATERMARKS` - Tracks last sync timestamp per resource type
- `HL7.RAW.FHIR_SYNC_HISTORY` - Audit log of all sync runs

---

## Related Documentation

- [FUTURE_ENHANCEMENTS.md](../FUTURE_ENHANCEMENTS.md) - Roadmap for SMART on FHIR, write operations, and Epic integration
- Parent [README.md](../README.md) - HAPI FHIR JPA Server documentation
