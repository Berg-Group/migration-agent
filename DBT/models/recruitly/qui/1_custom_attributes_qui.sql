{{ config(
    materialized='table',
    alias='custom_attributes_qui',
    tags = ["qui"]
) }}

WITH custom_attributes AS (
    SELECT 'Recruiting/Interviews' AS entity_name, TRUE AS multiple_values
    UNION ALL SELECT 'Gender', FALSE
    UNION ALL SELECT 'Nationality', FALSE
    UNION ALL SELECT 'Sectors', TRUE
    UNION ALL SELECT 'Industries', TRUE
)
SELECT
    {{ atlas_uuid("'custom' || entity_name || ' attribute " ~ var('agency_id') ~ "'") }} AS atlas_id,
    entity_name AS name,
    LOWER(REPLACE(entity_name, '/', '_')) AS alias,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    multiple_values,
    FALSE AS ai,
    'options' AS type,
    'person' AS of
FROM custom_attributes

