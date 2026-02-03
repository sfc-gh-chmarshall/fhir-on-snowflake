USE ROLE SYSADMIN;
CREATE OR REPLACE DATABASE HL7_FHIR COMMENT = 'HL7 FHIR DATABASE';;
CREATE OR REPLACE SCHEMA HL7_FHIR_V1;
CREATE OR REPLACE WAREHOUSE "HL7_FHIR_WH"
    WAREHOUSE_SIZE = 'SMALL'
    INITIALLY_SUSPENDED = FALSE
    AUTO_SUSPEND=600
    AUTO_RESUME=TRUE
    MIN_CLUSTER_COUNT=1
    MAX_CLUSTER_COUNT=1
    SCALING_POLICY='STANDARD'
    COMMENT='';
CREATE OR REPLACE NETWORK RULE HAPI_FHIR_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hapi.fhir.org', 'hapi.fhir.org:80');

  USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION HAPI_FHIR_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (HAPI_FHIR_NETWORK_RULE)
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION HAPI_FHIR_ACCESS_INTEGRATION to role SYSADMIN;

USE ROLE SYSADMIN;

ALTER TABLE RAW_FHIR_CLAIMS set ENABLE_SCHEMA_EVOLUTION = true;

CREATE OR REPLACE  PROCEDURE LOAD_HAPI_FHIR_CLAIMS()
         RETURNS string
         LANGUAGE PYTHON
         RUNTIME_VERSION=3.8
         IMPORTS=('@HL7.FHIRDEMO.deployments/load_hapi_fhir_claims/app.zip')
         HANDLER='app.main'
         PACKAGES=('pandas','requests','snowflake-snowpark-python','toml')
         external_access_integrations = (HAPI_FHIR_ACCESS_INTEGRATION);

CALL LOAD_HAPI_FHIR_CLAIMS();

select count(*) , responsetime 
from raw_fhir_claims
group by all;
--Example flattening query
SELECT 
nvl(C.RESOURCE:patient.display, C.RESOURCE:patient.reference) as patient
, C.RESOURCE:patient.displya as patient_typo
, nvl(C.resource:provider.display, C.RESOURCE:provider.reference) as provider
, *
FROM RAW_FHIR_CLAIMS C;

--To troubleshoot. Replace table name with your own event table.
select * from LOGS.PUBLIC.MYLOGS order by observed_timestamp desc;
