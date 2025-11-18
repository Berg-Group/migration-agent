{{ config(
    materialized='table',
    alias='custom_attributes_neuco',
    tags = ["bullhorn"]
) }}

WITH custom_attributes AS (
    SELECT 'Notice Period' AS entity_name
    UNION ALL
    SELECT 'Source' AS entity_name
    UNION ALL
    SELECT 'AD/BD Lead' AS entity_name
    UNION ALL
    SELECT 'Service Type' AS entity_name
    UNION ALL
    SELECT 'Confidential Role' AS entity_name
    UNION ALL
    SELECT 'Candidate Action' AS entity_name
    UNION ALL
    SELECT 'Client Action' AS entity_name
    UNION ALL
    SELECT 'Target Action' AS entity_name
    UNION ALL
    SELECT 'Company Status' AS entity_name
    UNION ALL
    SELECT 'Company Sector' AS entity_name
    UNION ALL
    SELECT 'Project Role Type' AS entity_name
)

SELECT
    {{ atlas_uuid("'custom' || entity_name || ' attribute " ~ var('agency_id') ~ "'") }} AS atlas_id,
    entity_name AS name,
    CASE 
        WHEN entity_name = 'Notice Period' THEN 'notice_period'
        WHEN entity_name = 'Source' THEN 'source'
        WHEN entity_name = 'AD/BD Lead' THEN 'ad_bd_lead'
        WHEN entity_name = 'Service Type' THEN 'service_type'
        WHEN entity_name = 'Confidential Role' THEN 'confidential_role'
        WHEN entity_name = 'Candidate Action' THEN 'candidate_action'
        WHEN entity_name = 'Client Action' THEN 'client_action'
        WHEN entity_name = 'Target Action' THEN 'target_action'
        WHEN entity_name = 'Company Status' THEN 'company_status'
        WHEN entity_name = 'Company Sector' THEN 'company_sector'
        WHEN entity_name = 'Project Role Type' THEN 'project_role_type'
        ELSE LOWER(REGEXP_REPLACE(entity_name, '\s+', '_'))
    END AS alias,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    TRUE AS multiple_values,
    FALSE AS ai,
    CASE 
        WHEN entity_name = 'Notice Period' THEN 'person'
        WHEN entity_name IN ('Source', 'AD/BD Lead', 'Company Status', 'Company Sector') THEN 'company'
        WHEN entity_name IN ('Service Type', 'Confidential Role', 'Project Role Type') THEN 'project'
        WHEN entity_name IN ('Candidate Action', 'Client Action', 'Target Action') THEN 'interview'
        ELSE 'person'
    END AS of
FROM custom_attributes