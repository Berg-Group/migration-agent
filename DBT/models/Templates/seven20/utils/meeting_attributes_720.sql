{{ config(
    materialized='table',
    alias='meeting_attributes_720',
    tags = ["seven20"]
) }}

SELECT 'candidate' AS alias, 'Origination Call - Candidate (OOCA)' AS value
UNION ALL
SELECT 'candidate', 'Qualification Call - Candidate (QCCA)'
UNION ALL
SELECT 'candidate', 'Process Mgmt - Candidate (PMCA)'
UNION ALL
SELECT 'client', 'Origination Call - Client (OOCL)'
UNION ALL
SELECT 'client', 'Qualification Call - Vacancy (QCCL)'
UNION ALL
SELECT 'client', 'Process Mgmt - Client (PMCL)'
UNION ALL
SELECT 'client', 'Client Meeting / Presentation Call'