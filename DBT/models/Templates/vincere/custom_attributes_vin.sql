{{ config(
    materialized='table',
    alias='custom_attributes_vin'
) }}

WITH attribute_list AS (
    SELECT  'functional_expertise' AS attribute_name
    UNION ALL
    SELECT 'sub_functional_expertise' AS attribute_name
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
    '2025-05-29T00:00:00' AS created_at,
    '2025-05-29T00:00:00' AS updated_at,
    NULL AS deleted_at,
    '{{ var('agency_id') }}' AS agency_id,
    TRUE AS multiple_values,
    'person' AS of,
    'options' AS type,
    FALSE AS ai
FROM
    attribute_list