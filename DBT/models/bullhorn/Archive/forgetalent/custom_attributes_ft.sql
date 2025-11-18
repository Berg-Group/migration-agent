{{ config(
    materialized='table',
    alias='custom_attributes_forgetalent',
    tags=["forgetalent"]
) }}

WITH attribute_list AS (
    SELECT 'Candidate' AS attribute_name
    UNION ALL
    SELECT 'Qualification' AS attribute_name
    UNION ALL 
    SELECT 'Employment Preference' AS attribute_name
    UNION ALL 
    SELECT 'Immediately Available' AS attribute_name
    UNION ALL 
    SELECT 'Notice Period' AS attribute_name 
    UNION ALL 
    SELECT 'Status' AS attribute_name)
   

SELECT
    {{atlas_uuid('attribute_name')}} AS atlas_id,
    attribute_name AS name,
    '2025-08-04T00:00:00' AS created_at,
    '2025-08-04T00:00:00' AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    TRUE AS multiple_values,
    'person' AS of,
    'options' AS type,
    FALSE AS ai
FROM
    attribute_list