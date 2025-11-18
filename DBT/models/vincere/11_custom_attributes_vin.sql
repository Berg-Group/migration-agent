{{ config(
    materialized='table',
    alias='custom_attributes_vincere'
) }}

WITH attribute_list AS (
    SELECT 'talent pool'::varchar AS attribute_name
)

SELECT
    {{ atlas_uuid("'" ~ var('agency_id') ~ "'::varchar || attribute_name") }} AS atlas_id,
    attribute_name AS name,
    TIMESTAMP '2025-07-09 00:00:00' AS created_at,
    TIMESTAMP '2025-07-09 00:00:00' AS updated_at,
    NULL::timestamp AS deleted_at,
    '{{ var("agency_id") }}'::varchar AS agency_id,
    TRUE AS multiple_values,
    'person'::varchar AS of,
    FALSE AS ai,
    'options'::varchar AS type
FROM attribute_list