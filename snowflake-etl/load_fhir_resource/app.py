import os
import requests
import json
import logging
import uuid
from datetime import datetime, timezone
from snowflake.snowpark import Session
from requests.exceptions import HTTPError

reqsession = requests.Session()
logger = logging.getLogger("python_logger")
logger.info("Logging from python module")

DEFAULT_FHIR_BASE_URL = os.environ.get("FHIR_BASE_URL", "http://fhir-server:8080/fhir")
DATABASE = "HL7"
SCHEMA = "RAW"
WATERMARK_TABLE = f"{DATABASE}.{SCHEMA}.FHIR_SYNC_WATERMARKS"
HISTORY_TABLE = f"{DATABASE}.{SCHEMA}.FHIR_SYNC_HISTORY"
EVENT_TABLE = "SNOWFLAKE.TELEMETRY.EVENTS"

WATERMARK_DDL = f"""
CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
    RESOURCE_TYPE VARCHAR(100) NOT NULL,
    TARGET_TABLE VARCHAR(255) NOT NULL,
    LAST_UPDATED_VALUE TIMESTAMP_NTZ,
    PRIMARY KEY (RESOURCE_TYPE, TARGET_TABLE)
)
"""

HISTORY_DDL = f"""
CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
    RUN_ID VARCHAR(36) NOT NULL,
    RESOURCE_TYPE VARCHAR(100) NOT NULL,
    TARGET_TABLE VARCHAR(255) NOT NULL,
    RUN_START_TIME TIMESTAMP_NTZ NOT NULL,
    RUN_END_TIME TIMESTAMP_NTZ,
    RECORDS_SYNCED NUMBER DEFAULT 0,
    STATUS VARCHAR(20) NOT NULL,
    WATERMARK_BEFORE TIMESTAMP_NTZ,
    WATERMARK_AFTER TIMESTAMP_NTZ,
    ERROR_MESSAGE VARCHAR(4000),
    SESSION_ID VARCHAR(50),
    PRIMARY KEY (RUN_ID)
)
"""


def ensure_tables(session: Session) -> None:
    session.sql(WATERMARK_DDL).collect()
    session.sql(HISTORY_DDL).collect()


def get_session_id(session: Session) -> str | None:
    try:
        result = session.sql("SELECT CURRENT_SESSION()").collect()
        if result:
            return str(result[0][0])
    except Exception:
        pass
    return None


def get_watermark(session: Session, resource_type: str, target_table: str) -> datetime | None:
    query = f"""
    SELECT LAST_UPDATED_VALUE 
    FROM {WATERMARK_TABLE} 
    WHERE RESOURCE_TYPE = '{resource_type}' AND TARGET_TABLE = '{target_table}'
    """
    result = session.sql(query).collect()
    if result and result[0]["LAST_UPDATED_VALUE"]:
        return result[0]["LAST_UPDATED_VALUE"]
    return None


def update_watermark(session: Session, resource_type: str, target_table: str, 
                     last_updated: datetime) -> None:
    last_updated_str = last_updated.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    merge_sql = f"""
    MERGE INTO {WATERMARK_TABLE} AS target
    USING (SELECT '{resource_type}' AS RESOURCE_TYPE, '{target_table}' AS TARGET_TABLE) AS source
    ON target.RESOURCE_TYPE = source.RESOURCE_TYPE AND target.TARGET_TABLE = source.TARGET_TABLE
    WHEN MATCHED THEN UPDATE SET 
        LAST_UPDATED_VALUE = '{last_updated_str}'::TIMESTAMP_NTZ
    WHEN NOT MATCHED THEN INSERT (RESOURCE_TYPE, TARGET_TABLE, LAST_UPDATED_VALUE)
        VALUES ('{resource_type}', '{target_table}', '{last_updated_str}'::TIMESTAMP_NTZ)
    """
    session.sql(merge_sql).collect()


def insert_history(session: Session, run_id: str, resource_type: str, target_table: str,
                   run_start_time: datetime, run_end_time: datetime | None, records_synced: int,
                   status: str, watermark_before: datetime | None, watermark_after: datetime | None,
                   error_message: str | None, session_id: str | None) -> None:
    def fmt_ts(ts):
        if ts is None:
            return "NULL"
        return f"'{ts.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z'::TIMESTAMP_NTZ"
    
    def fmt_str(s):
        if s is None:
            return "NULL"
        escaped = s.replace("'", "''")[:4000]
        return f"'{escaped}'"
    
    insert_sql = f"""
    INSERT INTO {HISTORY_TABLE} (
        RUN_ID, RESOURCE_TYPE, TARGET_TABLE, RUN_START_TIME, RUN_END_TIME,
        RECORDS_SYNCED, STATUS, WATERMARK_BEFORE, WATERMARK_AFTER, ERROR_MESSAGE, SESSION_ID
    ) VALUES (
        '{run_id}', '{resource_type}', '{target_table}', {fmt_ts(run_start_time)}, {fmt_ts(run_end_time)},
        {records_synced}, '{status}', {fmt_ts(watermark_before)}, {fmt_ts(watermark_after)}, 
        {fmt_str(error_message)}, {fmt_str(session_id)}
    )
    """
    session.sql(insert_sql).collect()


def check_last_updated_support(resource_type: str, fhir_base_url: str) -> tuple[bool, str | None]:
    url = f"{fhir_base_url}/{resource_type}"
    params = {'_count': 1, '_format': 'json'}
    try:
        response = reqsession.get(url, params=params)
        response.raise_for_status()
        bundle = response.json()
        logger.info(f"check_last_updated_support for {resource_type}: status={response.status_code}, has_entry={bool(bundle.get('entry'))}")
        if bundle.get('entry'):
            for entry in bundle['entry']:
                resource = entry.get('resource', {})
                meta = resource.get('meta', {})
                logger.info(f"Entry resource meta keys: {list(meta.keys())}")
                if 'lastUpdated' in meta:
                    return True, None
        logger.warning(f"No lastUpdated found in {resource_type} entries")
        return False, f"Resource type {resource_type} does not support _lastUpdated"
    except Exception as e:
        logger.error(f"check_last_updated_support exception for {resource_type}: {e}")
        return False, f"Failed to connect to FHIR server: {e}"


def extract_last_updated(entry: dict) -> str | None:
    resource = entry.get('resource', {})
    meta = resource.get('meta', {})
    return meta.get('lastUpdated')


def parse_fhir_timestamp(ts_str: str) -> datetime:
    ts_str = ts_str.replace('Z', '+00:00')
    if '.' in ts_str:
        frac_part = ts_str.split('.')[1].split('+')[0].split('-')[0]
        if len(frac_part) > 6:
            before_frac = ts_str.split('.')[0]
            after_frac = ts_str[ts_str.find('.') + 1 + len(frac_part):]
            ts_str = f"{before_frac}.{frac_part[:6]}{after_frac}"
    return datetime.fromisoformat(ts_str)


def get_events_query(run_id: str, session_id: str | None, event_table: str = EVENT_TABLE) -> str:
    """
    Returns a query to fetch logs from the event table for a specific sync run.
    Join on session_id (from RESOURCE_ATTRIBUTES:snow.session.id) to correlate logs.
    """
    if session_id:
        return f"""
        SELECT *
        FROM {event_table}
        WHERE RESOURCE_ATTRIBUTES:"snow.session.id"::STRING = '{session_id}'
        ORDER BY TIMESTAMP
        """
    return f"-- No session_id captured for run_id {run_id}"


def load_fhir_resource(session: Session, resource_type: str, target_table: str, 
                       event_table: str = EVENT_TABLE,
                       fhir_base_url: str = DEFAULT_FHIR_BASE_URL) -> dict:
    run_id = str(uuid.uuid4())
    run_start_time = datetime.now(timezone.utc)
    session_id = get_session_id(session)
    
    ensure_tables(session)
    
    watermark_before = get_watermark(session, resource_type, target_table)
    
    supports_last_updated, error_msg = check_last_updated_support(resource_type, fhir_base_url)
    if not supports_last_updated:
        logger.warning(f"{error_msg}. Skipping.")
        insert_history(session, run_id, resource_type, target_table, run_start_time, 
                       datetime.now(timezone.utc), 0, "SKIPPED", watermark_before, None,
                       error_msg, session_id)
        return {
            "status": "skipped",
            "count": 0,
            "run_id": run_id,
            "session_id": session_id,
            "events_query": get_events_query(run_id, session_id, event_table),
            "message": error_msg
        }
    
    url = f"{fhir_base_url}/{resource_type}"
    params = {'_format': 'json', '_sort': '_lastUpdated', '_count': 1000}
    
    if watermark_before:
        watermark_str = watermark_before.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        params['_lastUpdated'] = f"gt{watermark_str}"
        logger.info(f"Fetching {resource_type} resources updated after {watermark_str}")
    else:
        logger.info(f"No watermark found for {resource_type}/{target_table}. Fetching all resources.")
    
    try:
        all_entries = []
        max_last_updated = None
        
        while url:
            response = reqsession.get(url, params=params if params else None)
            response.raise_for_status()
            
            json_response = response.json()
            
            if 'entry' in json_response and json_response['entry']:
                entries = json_response['entry']
                all_entries.extend(entries)
                
                for entry in entries:
                    last_updated_str = extract_last_updated(entry)
                    if last_updated_str:
                        entry_ts = parse_fhir_timestamp(last_updated_str)
                        if max_last_updated is None or entry_ts > max_last_updated:
                            max_last_updated = entry_ts
            
            next_url = None
            for link in json_response.get('link', []):
                if link.get('relation') == 'next':
                    next_url = link.get('url')
                    break
            url = next_url
            params = None
        
        if not all_entries:
            insert_history(session, run_id, resource_type, target_table, run_start_time,
                           datetime.now(timezone.utc), 0, "SUCCESS", watermark_before, 
                           watermark_before, None, session_id)
            return {
                "status": "success",
                "count": 0,
                "run_id": run_id,
                "session_id": session_id,
                "events_query": get_events_query(run_id, session_id, event_table),
                "message": f"No new {resource_type} resources found since last sync"
            }
        
        pull_timestamp = datetime.now(timezone.utc)
        
        records = []
        for entry in all_entries:
            last_updated_str = extract_last_updated(entry)
            records.append([
                json.dumps(entry.get('resource', entry)),
                entry.get('fullUrl'),
                resource_type,
                last_updated_str,
                pull_timestamp.isoformat()
            ])
        
        from snowflake.snowpark.types import StructType, StructField, StringType
        schema = StructType([
            StructField("RESOURCE", StringType()),
            StructField("FULLURL", StringType()),
            StructField("RESOURCETYPE", StringType()),
            StructField("RESOURCELASTUPDATED", StringType()),
            StructField("PULLTIMESTAMP", StringType())
        ])
        
        snowpark_df = session.create_dataframe(records, schema)
        fully_qualified_table = f"{DATABASE}.{SCHEMA}.{target_table}"
        snowpark_df.write.mode("append").save_as_table(fully_qualified_table)
        
        if max_last_updated:
            update_watermark(session, resource_type, target_table, max_last_updated)
        
        insert_history(session, run_id, resource_type, target_table, run_start_time,
                       datetime.now(timezone.utc), len(all_entries), "SUCCESS", 
                       watermark_before, max_last_updated, None, session_id)
        
        return {
            "status": "success",
            "count": len(all_entries),
            "run_id": run_id,
            "session_id": session_id,
            "events_query": get_events_query(run_id, session_id, event_table),
            "message": f"Loaded {len(all_entries)} {resource_type} resources into {target_table}"
        }
        
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        insert_history(session, run_id, resource_type, target_table, run_start_time,
                       datetime.now(timezone.utc), 0, "ERROR", watermark_before, None,
                       str(http_err), session_id)
        return {
            "status": "error",
            "count": 0,
            "run_id": run_id,
            "session_id": session_id,
            "events_query": get_events_query(run_id, session_id, event_table),
            "message": f"HTTP error: {http_err}"
        }
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        insert_history(session, run_id, resource_type, target_table, run_start_time,
                       datetime.now(timezone.utc), 0, "ERROR", watermark_before, None,
                       str(err), session_id)
        return {
            "status": "error", 
            "count": 0,
            "run_id": run_id,
            "session_id": session_id,
            "events_query": get_events_query(run_id, session_id, event_table),
            "message": f"Error: {err}"
        }


def main(session: Session, resource_type: str = "Claim", target_table: str = "RAW_FHIR_RESOURCES",
         event_table: str = EVENT_TABLE, fhir_base_url: str = DEFAULT_FHIR_BASE_URL) -> str:
    """
    Main entry point for the stored procedure.
    
    Uses high water mark pattern based on _lastUpdated to fetch only new/updated resources.
    
    Args:
        session: Snowpark session (provided by Snowflake)
        resource_type: FHIR resource type to load (default: 'Claim')
        target_table: Target table name (default: 'RAW_FHIR_RESOURCES')
        event_table: Event table for log correlation (default: 'SNOWFLAKE.TELEMETRY.EVENTS')
        fhir_base_url: FHIR server base URL (default: internal SPCS URL)
    
    Returns:
        JSON string with result status, run_id, session_id, and events_query for log correlation
    
    Example calls:
        CALL LOAD_FHIR_RESOURCE('Patient', 'RAW_FHIR_PATIENTS');
        CALL LOAD_FHIR_RESOURCE('Observation', 'RAW_FHIR_OBSERVATIONS');
        
    To view logs for a sync run, use the events_query from the response:
        SELECT * FROM SNOWFLAKE.TELEMETRY.EVENTS 
        WHERE RESOURCE_ATTRIBUTES:"snow.query.id"::STRING = '<session_id>'
    """
    result = load_fhir_resource(session, resource_type, target_table, event_table, fhir_base_url)
    logger.info(result['message'])
    return json.dumps(result)
