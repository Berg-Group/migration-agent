{{ config(
    materialized = 'table',
    alias = 'custom_attribute_options_cr'
) }}

WITH internal_attributes AS (

    SELECT
        atlas_id,
        name
    FROM
        {{ ref('custom_attributes_cr') }}
),

base AS (
SELECT
    c.itemid AS id,
    {{ atlas_uuid('c.itemid') }} AS atlas_id,
    ia.atlas_id AS atlas_attribute_id,
    c.fileas AS value,
    ROW_NUMBER() over (PARTITION BY ia.atlas_id ORDER BY value) AS position,
    '{{ var('agency_id') }}' AS agency_id,
    TO_CHAR(c.datecreated::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(c.datemodified::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
FROM
    {{ var('source_database') }}."categorylistentries" c
INNER JOIN 
    {{ var('source_database') }}."categorylists" cl ON cl.itemid = c.categorylistid 
    AND cl.fileas = 'International Reach'
INNER JOIN 
    internal_attributes ia ON ia.name = cl.fileas


UNION ALL 

SELECT 
    c.itemid AS id,
    {{ atlas_uuid('c.itemid') }} AS atlas_id,
    ia.atlas_id AS atlas_attribute_id,
    c.name AS value,    
    ROW_NUMBER() over (PARTITION BY ia.atlas_id ORDER BY value) AS position,
    '{{ var('agency_id') }}' AS agency_id,
    TO_CHAR(c.datecreated::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(c.datemodified::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
FROM
    {{ var('source_database') }}."lookuplistentries" c
INNER JOIN 
    {{ var('source_database') }}."lookuplists" cl ON cl.itemid = c.lookuplistid 
    AND cl.name = 'EngagementType'
INNER JOIN 
    internal_attributes ia ON ia.name = cl.name)

SELECT * FROM base
ORDER BY
    atlas_attribute_id,
    position