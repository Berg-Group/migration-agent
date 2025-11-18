{{ config(
    materialized='table',
    alias='meeting_custom_attribute_values_source',
    tags = ["seven20"]
) }}

WITH internal_meetings AS (
    SELECT
        m.id AS meeting_id,
        m.atlas_id AS atlas_meeting_id,
        TRIM(m.attribute) AS attribute_value
    FROM {{ ref('12_meetings_720') }} m
    WHERE m.attribute IS NOT NULL
      AND TRIM(m.attribute) != ''
),
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        cao.atlas_id AS option_id,
        cao.id AS external_id,
        ca.alias AS attribute_type,
        cao.value AS option_value
    FROM 
        {{ ref('2_custom_attribute_options_source') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_source') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'interview'
)
SELECT DISTINCT
    {{ atlas_uuid('im.meeting_id::text || io.option_value::text || io.option_id::text') }} AS atlas_id,
    im.meeting_id,
    im.atlas_meeting_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    internal_meetings im
INNER JOIN 
    internal_options io ON io.option_value = im.attribute_value
ORDER BY
    im.meeting_id,
    io.atlas_attribute_id 