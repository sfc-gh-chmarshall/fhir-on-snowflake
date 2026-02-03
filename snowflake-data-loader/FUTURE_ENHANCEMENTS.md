# Future Enhancements

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
