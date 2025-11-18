{{ config(
    materialized='table',
    alias='meeting_custom_attribute_values_neuco',
    tags = ["bullhorn"]
) }}

WITH internal_meetings AS (
    SELECT
        m.id AS meetings_id,
        m.atlas_id AS atlas_meetings_id,
        TRIM(m.action) AS action_value
    FROM {{ ref('13_meetings_bh') }} m
    WHERE m.action IS NOT NULL
      AND TRIM(m.action) != ''
),  
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        cao.atlas_id AS option_id,
        cao.id AS external_id,
        ca.alias AS attribute_type,
        cao.value AS option_value
    FROM 
        {{ ref('2_custom_attribute_options_neuco') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_neuco') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'interview'
)
SELECT DISTINCT
    {{ atlas_uuid('im.meetings_id::text || io.option_value::text || io.option_id::text') }} AS atlas_id,
    im.meetings_id,
    im.atlas_meetings_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    internal_meetings im
INNER JOIN 
    internal_options io ON LOWER(io.option_value) = LOWER(im.action_value)
ORDER BY
    im.meetings_id,
    io.atlas_attribute_id 