{{ config(
    materialized='table',
    alias='custom_attributes_sainty',
    tags=["saintyhird"]
) }}

WITH custom_attributes AS (
    -- People
    SELECT 'Status' AS entity_name, 'person' AS of
    UNION ALL SELECT 'Nationality', 'person'
    UNION ALL SELECT 'Current location', 'person'
    UNION ALL SELECT 'Gender', 'person'
    UNION ALL SELECT 'Industry', 'person'
    UNION ALL SELECT 'Job Function', 'person'
    UNION ALL SELECT 'Qualification', 'person'
    UNION ALL SELECT 'International', 'person'
    UNION ALL SELECT 'Language', 'person'
    UNION ALL SELECT 'Client Type', 'person'
    UNION ALL SELECT 'Title', 'person'
    UNION ALL SELECT 'Product', 'person'
    UNION ALL SELECT 'Style/Theme', 'person'

    -- Projects
    UNION ALL SELECT 'Project Industry', 'project'
    UNION ALL SELECT 'Project Function', 'project'
    UNION ALL SELECT 'Project International', 'project'

    -- Companies
    UNION ALL SELECT 'Company Industry', 'company'
)

SELECT
    {{ atlas_uuid("'custom' || entity_name || ' ' || of || ' attribute " ~ var('agency_id') ~ "'") }} AS atlas_id,
    entity_name AS name,
    CASE 
        WHEN entity_name = 'Status' THEN 'status'
        WHEN entity_name = 'Nationality' THEN 'nationality'
        WHEN entity_name = 'Current location' THEN 'current_location'
        WHEN entity_name = 'Gender' THEN 'gender'
        WHEN entity_name = 'Industry' THEN 'industry'
        WHEN entity_name = 'Job Function' THEN 'job_function'
        WHEN entity_name = 'Qualification' THEN 'qualification'
        WHEN entity_name = 'International' THEN 'international'
        WHEN entity_name = 'Language' THEN 'language'
        WHEN entity_name = 'Client Type' THEN 'client_type'
        WHEN entity_name = 'Title' THEN 'title'
        WHEN entity_name = 'Product' THEN 'product'
        WHEN entity_name = 'Style/Theme' THEN 'style_theme'
        WHEN entity_name = 'Project Industry' THEN 'project_industry'
        WHEN entity_name = 'Project Function' THEN 'project_function'
        WHEN entity_name = 'Project International' THEN 'project_international'
        WHEN entity_name = 'Company Industry' THEN 'company_industry'
        ELSE LOWER(REGEXP_REPLACE(entity_name, '\\s+', '_'))
    END AS alias,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    CASE
        WHEN entity_name IN ('Status', 'Current location', 'Gender') THEN FALSE
        ELSE TRUE
    END AS multiple_values,
    FALSE AS ai,
    'options' AS type,
    of
FROM custom_attributes