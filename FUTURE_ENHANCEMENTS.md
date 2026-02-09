# Future Enhancements

## SMART on FHIR Authentication

**Priority:** High (for EHR integration)

EHRs are expanding SMART on FHIR capabilities. When extending this codebase to production environments, we'll need:

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

### Bulk FHIR Export

**Priority:** Medium

For large-scale data pulls, implement FHIR Bulk Data Access ($export):

- Async export initiation
- Polling for completion
- NDJSON file download and processing
- Better for full loads vs incremental sync

---

## CDS Hooks Integration

**Priority:** Medium (for clinical decision support use cases)

[CDS Hooks](https://cds-hooks.org/) is an HL7 standard for delivering real-time Clinical Decision Support within EHR workflows. It "hooks" into specific moments in a clinician's workflow to provide context-aware recommendations.

### How CDS Hooks Work

1. **Hook Trigger**: Clinical events (e.g., opening a patient chart, ordering medication) trigger a hook
2. **CDS Service**: External service receives context + patient data via FHIR
3. **CDS Cards**: Service returns actionable recommendations displayed in the EHR
4. **SMART App Launch**: Cards can optionally launch SMART on FHIR apps for deeper interaction

### Standard Hook Types

| Hook | Trigger |
|------|---------|
| `patient-view` | Opening a patient record |
| `order-select` | Selecting orders to place |
| `order-sign` | Immediately before signing an order |
| `encounter-start` | Beginning a patient encounter |

### Integration with FHIR-on-Snowflake

This architecture enables powerful CDS scenarios by combining real-time FHIR data with Snowflake analytics:

```
[EHR] → [CDS Hook Request]
            ↓
    [HAPI FHIR Server (SPCS)]
            ↓
    [Snowflake Postgres] ← [Analytics/ML Models]
            ↓
    [CDS Cards Response] → [EHR Display]
```

**Potential Use Cases:**

1. **Risk Scoring**: Query Snowflake ML models for patient risk scores when chart opens (`patient-view`)
2. **Clinical Trial Matching**: Check patient eligibility against trial criteria stored in Snowflake
3. **Drug Interaction Alerts**: Cross-reference orders against analytics on adverse events (`order-sign`)
4. **Population Health Insights**: Surface relevant cohort analytics during patient encounters
5. **Cost/Utilization Alerts**: Provide cost-effectiveness recommendations from claims analytics

### Implementation Approach

**Phase 1: CDS Service Endpoint**
- Add CDS Hooks discovery endpoint to HAPI FHIR server (`/cds-services`)
- Implement `patient-view` hook as initial proof of concept
- Return basic CDS Cards with patient context

**Phase 2: Snowflake Analytics Integration**
- Create stored procedures that generate CDS recommendations from Snowflake data
- Build ML models (e.g., readmission risk) deployable via Snowflake Model Registry
- Wire CDS service to query Snowflake for real-time scoring

**Phase 3: SMART App Links**
- Develop SMART on FHIR apps for complex decision support workflows
- Link CDS Cards to launch Streamlit apps hosted in SPCS
- Enable deeper exploration of Snowflake analytics within clinical context

### Key Considerations

- **Latency**: CDS services must respond in near-real-time (<500ms ideal)
- **Alert Fatigue**: Carefully curate recommendations to avoid overwhelming clinicians
- **Security**: Implement CDS Hooks security model (JWT bearer tokens, CORS)
- **SMART on FHIR**: Requires OAuth2 flows (see SMART on FHIR section above)

### References

- [CDS Hooks Specification](https://cds-hooks.hl7.org/)
- [CDS Hooks Sandbox](http://sandbox.cds-hooks.org/)
- [FHIR Clinical Reasoning](https://build.fhir.org/clinicalreasoning-cds-on-fhir.html)
