{{ config(
    materialized='table',
    alias='custom_attribute_options_rcrm',
    tags = ["recruit_crm"]
) }}

WITH internal_attributes AS (
    SELECT
        atlas_id,
        alias
    FROM
        {{ ref('custom_attributes_rcrm') }}
)
SELECT
    hd.hotlist_id AS id,
    {{ atlas_uuid('hd.hotlist_id') }} AS atlas_id,
    ia.atlas_id AS atlas_attribute_id,
    hd.name AS value,
    ROW_NUMBER() OVER (PARTITION BY ia.atlas_id ORDER BY hd.name ASC) AS position,
    '2025-06-03T00:00:00' AS created_at,
    '2025-06-03T00:00:00' AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    {{ var('source_database') }}."hotlist_data" hd
INNER JOIN 
    internal_attributes ia ON ia.alias = hd.entity_name
ORDER BY
    atlas_attribute_id,
    position