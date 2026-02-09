# FHIR Test Data Loader

Loads the [fhir.test.data.r4](https://www.fhir.org/packages/fhir.test.data.r4/) package into a local HAPI FHIR server for development and testing.

## Prerequisites

- Local HAPI FHIR server running (default: `http://localhost:8080/fhir`)
- Python 3.8+

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Load test data to local server
python load_fhir_test_data.py

# Custom server URL
python load_fhir_test_data.py --server-url http://localhost:8080/fhir

# Persist downloaded data for reuse
python load_fhir_test_data.py --data-dir ./fhir_data --skip-download
```

## Data Source

The test data is from the FHIR Foundation's test data package containing de-identified clinical records in FHIR R4 format.
