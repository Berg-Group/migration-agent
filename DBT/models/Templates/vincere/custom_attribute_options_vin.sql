{{ config(
    materialized='table',
    alias='custom_attribute_options_vin'
) }}

WITH functional_expertise AS (
    SELECT
        id AS external_id,
        name AS value,
        ROW_NUMBER() OVER (ORDER BY name) AS position,
        'functional_expertise'::text AS attribute_name,
        '2025-05-29T00:00:00' AS created_at,
        '2025-05-29T00:00:00' AS updated_at,
        LOWER(
            substring(md5(id::text || '{{ var("clientName") }}'), 1, 8) || '-' ||
            substring(md5(id::text || '{{ var("clientName") }}'), 9, 4) || '-' ||
            substring(md5(id::text || '{{ var("clientName") }}'), 13, 4) || '-' ||
            substring(md5(id::text || '{{ var("clientName") }}'), 17, 4) || '-' ||
            substring(md5(id::text || '{{ var("clientName") }}'), 21, 12)
        ) AS atlas_id
    FROM
        {{ var('source_database') }}."functional_expertise"
),
sub_functional_expertise AS (
    SELECT
        id AS external_id,
        name AS value,
        'sub_functional_expertise'::text AS attribute_name,
        '2025-05-29T00:00:00' AS created_at,
        '2025-05-29T00:00:00' AS updated_at,
        LOWER(
            substring(md5(id::text || '{{ var("clientName") }}'), 1, 8) || '-' ||
            substring(md5(id::text || '{{ var("clientName") }}'), 9, 4) || '-' ||
            substring(md5(id::text || '{{ var("clientName") }}'), 13, 4) || '-' ||
            substring(md5(id::text || '{{ var("clientName") }}'), 17, 4) || '-' ||
            substring(md5(id::text || '{{ var("clientName") }}'), 21, 12)
        ) AS atlas_id
    FROM
        {{ var('source_database') }}."sub_functional_expertise"
)

SELECT
    functional_expertise.atlas_id,
    a.atlas_id AS atlas_attribute_id,
    value,
    ROW_NUMBER() OVER (PARTITION BY a.atlas_id ORDER BY value) AS position,
    agency_id,
    functional_expertise.created_at,
    functional_expertise.updated_at,
    external_id
FROM
    functional_expertise 
LEFT JOIN 
    {{ref('custom_attributes_vin')}} a ON a.name = functional_expertise.attribute_name

UNION ALL

SELECT
    sub_functional_expertise.atlas_id,
    a.atlas_id AS atlas_attribute_id,
    value,
    ROW_NUMBER() OVER (PARTITION BY a.atlas_id ORDER BY value) AS position,
    agency_id,
    sub_functional_expertise.created_at,
    sub_functional_expertise.updated_at,
    external_id
FROM
    sub_functional_expertise
LEFT JOIN 
    {{ref('custom_attributes_vin')}} a ON a.name = sub_functional_expertise.attribute_name
