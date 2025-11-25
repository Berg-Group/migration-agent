{{ config(
    materialized='table',
    alias='person_custom_attribute_values_neuco',
    tags = ["bullhorn"]
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id
    FROM 
        {{ ref('1_people_bh') }}
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
    WHERE ca.alias = 'notice_period'
),
notice_period_mappings AS (
    SELECT DISTINCT
        c.UserID AS person_id,
        TRIM(c.customtext4) AS notice_period_value
    FROM {{ var('source_database') }}.bh_usercontact c
    WHERE c.customtext4 IS NOT NULL 
      AND TRIM(c.customtext4) != ''
)
SELECT DISTINCT
    {{ atlas_uuid('ip.person_id::text || io.atlas_attribute_id::text || io.option_id::text') }} AS atlas_id,
    ip.person_id,
    ip.atlas_person_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    notice_period_mappings npm
INNER JOIN 
    internal_persons ip ON ip.person_id = npm.person_id
INNER JOIN 
    internal_options io ON io.option_value = npm.notice_period_value
ORDER BY
    ip.person_id,
    io.atlas_attribute_id 