{{ config(
    materialized='table',
    alias='custom_attributes_blackwood'
) }}

WITH attribute_list AS (
    SELECT 'department current' AS attribute_name
    UNION ALL
    SELECT 'department previous' AS attribute_name
    UNION ALL
    SELECT 'language' AS attribute_name
    UNION ALL 
    SELECT 'nationality' AS attribute_name
    UNION ALL 
    SELECT 'position previous' AS attribute_name
    UNION ALL 
    SELECT 'position current' AS attribute_name 
    UNION ALL 
    SELECT 'product current' AS attribute_name 
    UNION ALL 
    SELECT 'product previous' AS attribute_name
    UNION ALL 
    SELECT 'job sector' AS attribute_name 
    UNION ALL 
    SELECT 'job function' AS attribute_name
    UNION ALL 
    SELECT 'job practice' AS attribute_name 
    UNION ALL 
    SELECT 'job product' AS attribute_name 
)

SELECT
    LOWER(
        substring(md5(attribute_name || '{{ var('clientName') }}'), 1, 8) || '-' ||
        substring(md5(attribute_name || '{{ var('clientName') }}'), 9, 4) || '-' ||
        substring(md5(attribute_name || '{{ var('clientName') }}'), 13, 4) || '-' ||
        substring(md5(attribute_name || '{{ var('clientName') }}'), 17, 4) || '-' ||
        substring(md5(attribute_name || '{{ var('clientName') }}'), 21, 12)
    ) AS atlas_id,
    attribute_name AS name,
    '2025-04-10T00:00:00' AS created_at,
    '2025-05-10T00:00:00' AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    TRUE AS multiple_values,
    CASE WHEN attribute_name IN ('job function', 'job product', 'job sector', 'job practice') THEN 'project' 
        ELSE 'person' END AS of,
    'options' AS type,
    FALSE AS ai
FROM
    attribute_list