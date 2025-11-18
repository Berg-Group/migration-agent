{{ config(
    materialized='table',
    alias='custom_attributes_bh',
    tags = ["bullhorn"]
) }}

WITH custom_attributes AS (
    SELECT 'Gender' AS entity_name
    UNION ALL
    SELECT 'Job Title' AS entity_name
    UNION ALL
    SELECT 'Specialism' AS entity_name
    UNION ALL
    SELECT 'Desired Location' AS entity_name
    UNION ALL
    SELECT 'Business Area' AS entity_name
    UNION ALL
    SELECT 'Project Job Title' AS entity_name
    UNION ALL
    SELECT 'Project Specialism' AS entity_name
    UNION ALL
    SELECT 'Company Type' AS entity_name
)
SELECT
    {{ atlas_uuid("'custom' || entity_name || ' attribute " ~ var('agency_id') ~ "'") }} AS atlas_id,
    entity_name AS name,
    CASE 
        WHEN entity_name = 'Gender' THEN 'gender'
        WHEN entity_name = 'Job Title' THEN 'job_title'
        WHEN entity_name = 'Specialism' THEN 'specialism'
        WHEN entity_name = 'Desired Location' THEN 'desired_location'
        WHEN entity_name = 'Business Area' THEN 'business_area'
        WHEN entity_name = 'Project Job Title' THEN 'project_job_title'
        WHEN entity_name = 'Project Specialism' THEN 'project_specialism'
        WHEN entity_name = 'Company Type' THEN 'company_type'
        ELSE LOWER(REGEXP_REPLACE(entity_name, '\s+', '_'))
    END AS alias,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    TRUE AS multiple_values,
    FALSE AS ai,
    'options' AS type,
    CASE 
        WHEN entity_name IN ('Gender', 'Job Title', 'Specialism', 'Desired Location', 'Business Area') THEN 'person'
        WHEN entity_name IN ('Company Type') THEN 'company'
        WHEN entity_name IN ('Project Job Title', 'Project Specialism') THEN 'project'
        ELSE 'person'
    END AS of
FROM custom_attributes