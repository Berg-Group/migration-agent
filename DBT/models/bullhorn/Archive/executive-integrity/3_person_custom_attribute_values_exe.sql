{{ config(
    materialized='table',
    alias='person_custom_attribute_values_exe',
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
        {{ ref('2_custom_attribute_options_exe') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_exe') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'person'
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
specialism_values AS (
    SELECT DISTINCT
        c.UserID AS person_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.skillset, ';', ','), ',,', ','), ',', numbers.n)) AS value
    FROM {{ var('source_database') }}.bh_usercontact c
    CROSS JOIN numbers
    WHERE c.skillset IS NOT NULL AND TRIM(c.skillset) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.skillset, ';', ','), ',,', ','), ',', numbers.n) != ''
),
combined_person_values AS (
    SELECT 
        person_id, 
        'specialism'::text AS attribute_type, 
        value::text AS value
    FROM specialism_values
)
SELECT DISTINCT
    {{ atlas_uuid('ip.person_id::text || io.atlas_attribute_id::text || io.option_id::text') }} AS atlas_id,
    ip.person_id,
    ip.atlas_person_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    combined_person_values cpv
INNER JOIN 
    internal_persons ip ON ip.person_id = cpv.person_id
INNER JOIN 
    internal_options io ON io.attribute_type = cpv.attribute_type AND io.option_value = cpv.value
ORDER BY
    ip.person_id,
    io.atlas_attribute_id 