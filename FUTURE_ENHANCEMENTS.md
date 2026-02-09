# Future Enhancements

## In Progress

### Snowflake Postgres Backend Migration

**Priority:** High (next task)

Migrate HAPI FHIR JPA Server from in-memory H2 to Snowflake Managed Postgres for persistent storage.

**Phase 1: Instance Setup** ✅
- [x] Create Snowflake Postgres instance in Snowsight (`fhir-postgres`)
- [x] Select Postgres 17/18, configure instance size and storage
- [x] Save connection credentials securely
- [x] Configure network policy for SPCS access
- [x] Create `fhir` database and `fhir_app` user

**Phase 2: Application Configuration** ✅
- [x] Update `application.yaml` datasource to PostgreSQL JDBC URL
- [x] Switch Hibernate dialect to `HapiFhirPostgresDialect`
- [x] Verify PostgreSQL driver in `pom.xml`
- [x] Create `application-snowflake-postgres.yaml` profile for easy switching

**Phase 3: Docker/SPCS Updates**
- [ ] Update `docker-compose.yml` for local Postgres testing
- [ ] Update SPCS service spec with secrets and networking

**Phase 4: Testing**
- [ ] Local testing with `mvn spring-boot:run -Pboot`
- [ ] Verify schema auto-creation
- [ ] Test CRUD operations via HAPI FHIR UI
- [ ] Load sample FHIR bundles
- [ ] Run `mvn verify`

**Key Configuration Changes:**
```yaml
spring:
  datasource:
    url: jdbc:postgresql://<host>:5432/fhir?sslmode=require
    username: fhir_app
    password: ${FHIR_DB_PASSWORD}
    driver-class-name: org.postgresql.Driver
  jpa:
    properties:
      hibernate:
        dialect: ca.uhn.fhir.jpa.model.dialect.HapiFhirPostgresDialect
```

**References:**
- [Snowflake Postgres Docs](https://docs.snowflake.com/en/user-guide/snowflake-postgres/about)
- [Creating Instance](https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-create-instance)
- [Connecting](https://docs.snowflake.com/en/user-guide/snowflake-postgres/connecting-to-snowflakepg)

---

## Parking Lot

### SMART on FHIR Authentication

**Priority:** High (for Epic EHR integration)

Epic is expanding SMART on FHIR capabilities. When extending this codebase to production Epic environments, we'll need:

- OAuth2 authorization flows (authorization code, client credentials)
- JWT-based authentication with signed tokens
- Scope-based access control (patient/*.read, user/*.write, etc.)
- Token refresh handling and secure storage

**Consideration:** The HAPI FHIR Java client has built-in SMART on FHIR support. Evaluate whether to:
1. Implement OAuth2 flows in Python (using `authlib` or `requests-oauthlib`)
2. Create a Java service in SPCS for auth-heavy operations
3. Hybrid: Python orchestration + Java FHIR client for complex interactions

### Write Operations

**Priority:** Medium (planned extension)

Current implementation is read-only. Future write capabilities will require:

- Transaction bundles for atomic multi-resource writes
- Conditional create/update (If-None-Exist, If-Match headers)
- FHIR validation before submission
- Conflict resolution and retry logic
- Audit trail for compliance

**Consideration:** Write operations benefit more from the HAPI FHIR Java client due to:
- Strongly-typed resource builders (Patient, Claim objects)
- Built-in FHIR validation
- Transaction bundle support
- Better error handling for FHIR-specific responses

### Java Stored Procedures Evaluation

**Priority:** Low (revisit when write operations are needed)

Current Python approach is sufficient for read-and-land patterns. Revisit Java when:

| Trigger | Action |
|---------|--------|
| Need FHIR resource validation | Evaluate Java client |
| Complex transaction bundles | Evaluate Java client |
| $extract, $everything, $validate operations | Evaluate Java client |
| SMART on FHIR becomes blocking | Evaluate hybrid approach |

**Hybrid Architecture Option:**
```
[Snowflake Task] 
    → [Python Stored Proc - Orchestration]
        → [Java SPCS Service - FHIR Operations]
            → [Epic/HAPI FHIR Server]
```

### Epic-Specific Considerations

- Epic uses R4 FHIR version
- Epic's sandbox vs production authentication differs
- Rate limiting and bulk data access ($export) may be required for large datasets
- Epic App Orchard registration for production access

### Bulk FHIR Export

**Priority:** Medium

For large-scale data pulls, implement FHIR Bulk Data Access ($export):

- Async export initiation
- Polling for completion
- NDJSON file download and processing
- Better for full loads vs incremental sync
