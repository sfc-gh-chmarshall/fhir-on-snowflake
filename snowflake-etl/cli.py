#!/usr/bin/env python3
"""
CLI for running the FHIR resource loader locally against an external FHIR endpoint.

This script is NOT deployed to Snowflake - it's for local development and testing only.

Usage:
    python cli.py Patient RAW_FHIR_PATIENTS --connection my-connection
    python cli.py Claim RAW_FHIR_CLAIMS --fhir-url https://<endpoint>.snowflakecomputing.app/fhir
"""
import argparse
from pathlib import Path

try:
    import toml
except ImportError:
    print("Error: toml package required. Install with: pip install toml")
    exit(1)

import requests
from snowflake.snowpark import Session

from load_fhir_resource.app import main, reqsession, EVENT_TABLE


def configure_auth_for_external_url(fhir_url: str, connection_name: str) -> None:
    """Configure PAT authentication for external Snowflake-hosted FHIR endpoints."""
    if fhir_url.startswith("https://") and "snowflakecomputing.app" in fhir_url:
        connections_path = Path.home() / ".snowflake" / "connections.toml"
        if not connections_path.exists():
            raise FileNotFoundError(f"Snowflake connections file not found: {connections_path}")
        with open(connections_path) as f:
            connections = toml.load(f)
        if connection_name not in connections:
            raise ValueError(f"Connection '{connection_name}' not found in {connections_path}")
        pat = connections[connection_name].get("password")
        if not pat:
            raise ValueError(f"Connection '{connection_name}' has no password/PAT configured")
        reqsession.headers.update({"Authorization": f'Snowflake Token="{pat}"'})
        print(f"Configured PAT auth for external FHIR URL")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Load FHIR resources from HAPI FHIR server into Snowflake (local CLI)"
    )
    parser.add_argument("resource_type", help="FHIR resource type (Patient, Claim, Observation, etc.)")
    parser.add_argument("target_table", help="Target Snowflake table name")
    parser.add_argument(
        "--event-table", 
        default=EVENT_TABLE, 
        help=f"Event table for log correlation (default: {EVENT_TABLE})"
    )
    parser.add_argument(
        "--fhir-url", 
        required=True,
        help="FHIR server base URL (e.g., https://<endpoint>.snowflakecomputing.app/fhir)"
    )
    parser.add_argument(
        "--connection", 
        default="default", 
        help="Snowflake CLI connection name from connections.toml (default: default)"
    )
    args = parser.parse_args()
    
    configure_auth_for_external_url(args.fhir_url, args.connection)
    
    session = Session.builder.config("connection_name", args.connection).create()
    
    try:
        result = main(session, args.resource_type, args.target_table, args.event_table, args.fhir_url)
        print(result)
    finally:
        session.close()
