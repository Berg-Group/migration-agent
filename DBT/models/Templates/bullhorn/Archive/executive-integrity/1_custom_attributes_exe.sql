{{ config(
    materialized='table',
    alias='custom_attributes_exe',
    tags = ["bullhorn"]
) }}

WITH custom_attributes AS (
    SELECT 'Specialism' AS entity_name
    UNION ALL
    SELECT 'Company Sector' AS entity_name
)
SELECT
    {{ atlas_uuid("'custom' || entity_name || ' attribute " ~ var('agency_id') ~ "'") }} AS atlas_id,
    entity_name AS name,
    CASE 
        WHEN entity_name = 'Specialism' THEN 'specialism'
        WHEN entity_name = 'Company Sector' THEN 'company_sector'
        ELSE LOWER(REGEXP_REPLACE(entity_name, '\s+', '_'))
    END AS alias,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    TRUE AS multiple_values,
    FALSE AS ai,
    'options' AS type,
    CASE 
        WHEN entity_name IN ('Specialism') THEN 'person'
        WHEN entity_name IN ('Company Sector') THEN 'company'
        ELSE 'person'
    END AS of
FROM custom_attributes