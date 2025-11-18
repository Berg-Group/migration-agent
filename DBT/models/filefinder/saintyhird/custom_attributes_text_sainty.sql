{{ config(
    materialized='table',
    alias='custom_attributes_text_sainty',
    tags=["saintyhird"]
) }}

WITH custom_attributes AS (
    -- People
    SELECT 'Known as' AS entity_name, 'person' AS of
    UNION ALL SELECT 'Maiden Name', 'person'
    UNION ALL SELECT 'Date of Birth', 'person'

    -- Companies
    UNION ALL SELECT 'Company Aliases', 'company'
)

SELECT
    {{ atlas_uuid("'custom' || entity_name || ' ' || of || ' attribute " ~ var('agency_id') ~ "'") }} AS atlas_id,
    entity_name AS name,
    CASE 
        WHEN entity_name = 'Known as' THEN 'known_as'
        WHEN entity_name = 'Maiden Name' THEN 'maiden_name'
        WHEN entity_name = 'Date of Birth' THEN 'date_of_birth'
        WHEN entity_name = 'Company Aliases' THEN 'company_aliases'
        ELSE LOWER(REGEXP_REPLACE(entity_name, '\\s+', '_'))
    END AS alias,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    FALSE AS multiple_values,
    FALSE AS ai,
    'text_block' AS type,
    of
FROM custom_attributes