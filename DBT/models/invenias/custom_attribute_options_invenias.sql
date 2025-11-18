{{ config(
    materialized = 'table',
    alias = 'custom_attribute_options_invenias'
) }}

WITH internal_attributes AS (

    SELECT
        atlas_id,
        name
    FROM
        {{ ref('custom_attributes_invenias') }}
)
SELECT
    c.itemid AS id,
    {{ atlas_uuid('c.itemid') }} AS atlas_id,
    ia.atlas_id AS atlas_attribute_id,
    c.fileas AS value,
    ROW_NUMBER() over (PARTITION BY ia.atlas_id ORDER BY value) AS position,
    '{{ var('agency_id') }}' AS agency_id,
    '2025-06-09T00:00:00' AS created_at,
    '2025-06-09T00:00:00' AS updated_at
FROM
    {{ var('source_database') }}."categorylistentries" c
INNER JOIN 
    {{ var('source_database') }}."categorylists" cl ON cl.itemid = c.categorylistid
INNER JOIN 
    internal_attributes ia ON ia.name = cl.fileas
ORDER BY
    atlas_attribute_id,
    position
