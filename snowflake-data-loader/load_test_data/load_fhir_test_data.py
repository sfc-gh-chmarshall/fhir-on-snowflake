#!/usr/bin/env python3
"""
Local FHIR Test Data Loader

Downloads and loads the fhir.test.data.r4 package into a HAPI FHIR server
running in Snowpark Container Services (SPCS).

Authentication:
    Uses PAT from password field in ~/.snowflake/connections.toml

Usage:
    python load_fhir_test_data.py --connection DEMO8498-christen
"""

import argparse
import json
import os
import sys
import tarfile
import tempfile
import time
from pathlib import Path
from urllib.request import urlretrieve

import requests
import toml

FHIR_TEST_DATA_URL = "https://www.fhir.org/packages/fhir.test.data.r4/0.2.1/package.tgz"
DEFAULT_SERVER_URL = "https://azbookd-sfsenorthamerica-demo8498.snowflakecomputing.app/fhir"

RESOURCE_LOAD_ORDER = [
    "Organization",
    "Location", 
    "Practitioner",
    "PractitionerRole",
    "Patient",
    "RelatedPerson",
    "Device",
    "Substance",
    "Medication",
    "Encounter",
    "EpisodeOfCare",
    "Condition",
    "Procedure",
    "Observation",
    "DiagnosticReport",
    "Specimen",
    "ImagingStudy",
    "MedicationRequest",
    "MedicationAdministration",
    "MedicationDispense",
    "MedicationStatement",
    "Immunization",
    "CarePlan",
    "CareTeam",
    "Goal",
    "NutritionOrder",
    "ServiceRequest",
    "Coverage",
    "Claim",
    "ClaimResponse",
    "ExplanationOfBenefit",
    "AllergyIntolerance",
    "AdverseEvent",
    "FamilyMemberHistory",
    "DocumentReference",
    "Composition",
    "Bundle",
]


def get_pat_from_connection(connection_name: str) -> str:
    """Read PAT from password field in Snowflake CLI connections.toml."""
    connections_path = Path.home() / ".snowflake" / "connections.toml"
    
    if not connections_path.exists():
        raise FileNotFoundError(f"Snowflake connections file not found: {connections_path}")
    
    with open(connections_path) as f:
        connections = toml.load(f)
    
    if connection_name not in connections:
        available = list(connections.keys())
        raise ValueError(f"Connection '{connection_name}' not found. Available: {available}")
    
    config = connections[connection_name]
    pat = config.get("password")
    
    if not pat:
        raise ValueError(f"Connection '{connection_name}' has no password/PAT configured")
    
    return pat


def get_authenticated_session(connection_name: str, server_url: str) -> requests.Session:
    session = requests.Session()
    
    if server_url.startswith("https://") and "snowflakecomputing.app" in server_url:
        print(f"Reading PAT from connection '{connection_name}'...")
        pat = get_pat_from_connection(connection_name)
        session.headers.update({
            "Authorization": f'Snowflake Token="{pat}"',
        })
        print("  PAT loaded successfully")
    
    return session


def download_test_data(dest_dir: str) -> str:
    package_path = os.path.join(dest_dir, "fhir_test_data.tgz")
    print(f"Downloading FHIR test data from {FHIR_TEST_DATA_URL}...")
    urlretrieve(FHIR_TEST_DATA_URL, package_path)
    print(f"Downloaded to {package_path}")
    return package_path


def extract_package(package_path: str, dest_dir: str) -> str:
    extract_dir = os.path.join(dest_dir, "package")
    print(f"Extracting package to {extract_dir}...")
    with tarfile.open(package_path, "r:gz") as tar:
        tar.extractall(dest_dir)
    print(f"Extracted {len(os.listdir(extract_dir))} files")
    return extract_dir


def send_resource_to_fhir(server_url: str, resource: dict, session: requests.Session, verbose: bool = False) -> tuple[bool, str]:
    resource_type = resource.get("resourceType")
    resource_id = resource.get("id")
    
    headers = {"Content-Type": "application/fhir+json"}
    
    if resource_type == "Bundle":
        url = server_url
        response = session.post(url, json=resource, headers=headers)
    else:
        url = f"{server_url}/{resource_type}"
        if resource_id:
            url = f"{server_url}/{resource_type}/{resource_id}"
            response = session.put(url, json=resource, headers=headers)
        else:
            response = session.post(url, json=resource, headers=headers)
    
    if verbose:
        print(f"\n  URL: {url}")
        print(f"  Status: {response.status_code}")
        print(f"  Response: {response.text[:500]}")
    
    if response.status_code in (200, 201):
        resp_json = response.json() if response.text else {}
        returned_id = resp_json.get("id", resource_id)
        return True, f"{resource_type}/{returned_id}"
    else:
        return False, f"{resource_type}/{resource_id}: {response.status_code} - {response.text[:200]}"


def get_resource_type_from_filename(filename: str) -> str:
    """Extract resource type from filename like 'Patient-123.json'."""
    name = filename.replace(".json", "")
    if "-" in name:
        return name.split("-")[0]
    return name


def sort_files_by_dependency(json_files: list) -> list:
    """Sort files so dependencies are loaded first."""
    def sort_key(filepath):
        resource_type = get_resource_type_from_filename(filepath.name)
        try:
            return RESOURCE_LOAD_ORDER.index(resource_type)
        except ValueError:
            return len(RESOURCE_LOAD_ORDER)
    
    return sorted(json_files, key=sort_key)


def load_json_files(package_dir: str, server_url: str, session: requests.Session, verbose: bool = False, limit: int = None) -> dict:
    stats = {"success": 0, "failed": 0, "skipped": 0, "errors": []}
    
    json_files = list(Path(package_dir).glob("*.json"))
    json_files = sort_files_by_dependency(json_files)
    if limit:
        json_files = json_files[:limit]
    total_files = len(json_files)
    print(f"Found {total_files} JSON files to process")
    
    for i, json_file in enumerate(json_files, 1):
        if json_file.name in ("package.json", ".index.json"):
            stats["skipped"] += 1
            continue
        
        print(f"[{i}/{total_files}] Processing {json_file.name}...", end=" ")
        
        try:
            with open(json_file, "r") as f:
                resource = json.load(f)
            
            success, msg = send_resource_to_fhir(server_url, resource, session, verbose)
            if success:
                stats["success"] += 1
                print(f"OK ({msg})")
            else:
                stats["failed"] += 1
                stats["errors"].append(msg)
                print(f"FAILED")
                
        except json.JSONDecodeError as e:
            stats["failed"] += 1
            stats["errors"].append(f"{json_file.name}: Invalid JSON - {e}")
            print(f"INVALID JSON")
        except Exception as e:
            stats["failed"] += 1
            stats["errors"].append(f"{json_file.name}: {e}")
            print(f"ERROR: {e}")
    
    return stats


def check_server_available(server_url: str, session: requests.Session) -> bool:
    try:
        response = session.get(f"{server_url}/metadata", timeout=10)
        if response.status_code != 200:
            print(f"Server returned status {response.status_code}: {response.text[:500]}")
            return False
        content_type = response.headers.get("content-type", "")
        if "html" in content_type.lower():
            print("ERROR: Server returned HTML (likely authentication failed)")
            print("Response preview:", response.text[:300])
            return False
        return True
    except requests.RequestException as e:
        print(f"Connection error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Load FHIR test data into SPCS HAPI FHIR server")
    parser.add_argument("--server-url", default=DEFAULT_SERVER_URL, help=f"FHIR server URL (default: {DEFAULT_SERVER_URL})")
    parser.add_argument("--data-dir", help="Directory to store downloaded data (default: temp directory)")
    parser.add_argument("--skip-download", action="store_true", help="Skip download if data already exists in data-dir")
    parser.add_argument("--connection", default="default", help="Snowflake CLI connection name from connections.toml")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed request/response info")
    parser.add_argument("--limit", type=int, help="Limit number of files to process (for testing)")
    args = parser.parse_args()
    
    print("=" * 60)
    print("FHIR Test Data Loader")
    print("=" * 60)
    print(f"Server URL: {args.server_url}")
    print(f"Connection: {args.connection}")
    
    session = get_authenticated_session(args.connection, args.server_url)
    
    print("\nChecking FHIR server availability...")
    if not check_server_available(args.server_url, session):
        print(f"ERROR: FHIR server at {args.server_url} is not available.")
        sys.exit(1)
    print("Server is available!")
    
    if args.data_dir:
        data_dir = args.data_dir
        os.makedirs(data_dir, exist_ok=True)
    else:
        data_dir = tempfile.mkdtemp(prefix="fhir_test_data_")
    
    print(f"Data directory: {data_dir}")
    
    package_dir = os.path.join(data_dir, "package")
    
    if args.skip_download and os.path.exists(package_dir):
        print("Skipping download, using existing data...")
    else:
        package_path = download_test_data(data_dir)
        package_dir = extract_package(package_path, data_dir)
    
    print("\n" + "=" * 60)
    print("Loading resources into FHIR server...")
    print("=" * 60)
    
    start_time = time.time()
    stats = load_json_files(package_dir, args.server_url, session, args.verbose, args.limit)
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Success: {stats['success']}")
    print(f"Failed:  {stats['failed']}")
    print(f"Skipped: {stats['skipped']}")
    print(f"Time:    {elapsed:.1f} seconds")
    
    if stats["errors"]:
        print(f"\nFirst 5 errors:")
        for err in stats["errors"][:5]:
            print(f"  - {err[:100]}")
    
    if stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
