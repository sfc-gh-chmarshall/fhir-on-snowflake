# Security Review Tasks

## Completed - Environment-Specific URLs

All hardcoded environment-specific URLs have been removed:

| File | Change |
|------|--------|
| `snowflake-etl/cli.py` | `--fhir-url` now **required** (no default) |
| `snowflake-etl/load_test_data/load_fhir_test_data.py` | `--server-url` now **required** (no default) |
| `snowflake-etl/load_fhir_resource/app.py` | Uses `FHIR_BASE_URL` env var, falls back to `http://fhir-server:8080/fhir` |
| `snowflake-etl/snowflake.yml` | Default changed to generic `http://fhir-server:8080/fhir` |
| `spcs-service-spec.yaml` | Uses `${SPRING_DATASOURCE_URL}` and `${HAPI_FHIR_TESTER_HOME_SERVER_ADDRESS}` env vars |
| `docker-compose.yml` | Uses `${SPRING_DATASOURCE_URL}` env var |
| `src/main/resources/application-snowflake-postgres.yaml` | Uses `${SPRING_DATASOURCE_URL}` env var |
| `snowflake-etl/README.md` | Pool references changed to `<pool>` placeholder |

### Required Environment Variables

For deployment, set these environment variables:

```bash
# PostgreSQL connection (required for docker-compose and SPCS)
export SPRING_DATASOURCE_URL="jdbc:postgresql://<host>:5432/fhir?sslmode=require&currentSchema=fhir_schema"
export FHIR_DB_PASSWORD="<password>"

# FHIR server internal URL (optional, for SPCS tester UI)
export HAPI_FHIR_TESTER_HOME_SERVER_ADDRESS="http://fhir-server.<pool>.svc.spcs.internal:8080/fhir"

# For stored procedure (optional, set in Snowflake or passed as parameter)
export FHIR_BASE_URL="http://fhir-server.<pool>.svc.spcs.internal:8080/fhir"
```

---

## Remaining - SQL Injection Vulnerabilities

### `snowflake-etl/load_fhir_resource/app.py`

**Lines 65-69** - `get_watermark()` function uses string interpolation:
```python
query = f"""
SELECT LAST_UPDATED_VALUE 
FROM {WATERMARK_TABLE} 
WHERE RESOURCE_TYPE = '{resource_type}' AND TARGET_TABLE = '{target_table}'
"""
```

**Lines 78-87** - `update_watermark()` function uses string interpolation in MERGE statement

**Lines 105-115** - `insert_history()` function uses string interpolation in INSERT statement

**Recommended Fix**: Use Snowpark parameterized queries or input validation

---

## Low Priority

- [ ] `docker-compose-localpostgres.yml` has hardcoded `admin/admin` credentials (acceptable for local dev, add comment noting this)
