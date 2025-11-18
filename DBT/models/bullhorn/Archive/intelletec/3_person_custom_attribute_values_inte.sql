{{ config(
    materialized='table',
    alias='person_custom_attribute_values_inte',
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
        {{ ref('2_custom_attribute_options_inte') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_inte') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'person'
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
pronouns_values AS (
    SELECT DISTINCT
        c.UserID AS person_id,
        TRIM(c.customtext14) AS value
    FROM {{ var('source_database') }}.bh_usercontact c
    WHERE c.customtext14 IS NOT NULL
        AND TRIM(c.customtext14) != ''
        AND TRIM(c.customtext14) != '0'
),
education_level_values AS (
    SELECT DISTINCT
        c.UserID AS person_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.degreelist, ';', ','), ',,', ','), ',', numbers.n)) AS value
    FROM {{ var('source_database') }}.bh_usercontact c
    CROSS JOIN numbers
    WHERE c.degreelist IS NOT NULL
        AND TRIM(c.degreelist) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.degreelist, ';', ','), ',,', ','), ',', numbers.n) != ''
),
current_location_values AS (
    SELECT DISTINCT
        c.UserID AS person_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.desiredlocations, ';', ','), ',,', ','), ',', numbers.n)) AS value
    FROM {{ var('source_database') }}.bh_usercontact c
    CROSS JOIN numbers
    WHERE c.desiredlocations IS NOT NULL
        AND TRIM(c.desiredlocations) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.desiredlocations, ';', ','), ',,', ','), ',', numbers.n) != ''
),
primary_skill_values AS (
    SELECT DISTINCT
        us.userid AS person_id,
        CASE
            WHEN l.categoryid = 2000027 THEN 'Energy - ' || TRIM(s.name)
            WHEN l.categoryid = 2000012 THEN 'Mobile - ' || TRIM(s.name)
            WHEN l.categoryid = 2000018 THEN 'Data - ' || TRIM(s.name)
            WHEN l.categoryid IN (2000013, 2000028) THEN 'BD - ' || TRIM(s.name)
            WHEN l.categoryid = 2000030 THEN 'Health Care - ' || TRIM(s.name)
        END AS value
    FROM {{ var('source_database') }}.bh_userskill us
    INNER JOIN {{ var('source_database') }}.bh_skilllist s ON s.skillid = us.skillid
    INNER JOIN {{ var('source_database') }}.bh_categoryskillassociation c ON c.skillid = us.skillid
    INNER JOIN {{ var('source_database') }}.bh_categorylist l ON l.categoryid = c.categoryid
    WHERE l.categoryid IN (2000027, 2000012, 2000018, 2000013, 2000028, 2000030)
        AND s.name IS NOT NULL
        AND TRIM(s.name) != ''
),
work_authority_values AS (
    SELECT DISTINCT
        c.userid AS person_id,
        TRIM(c.employeetype) AS value
    FROM {{ var('source_database') }}.bh_candidate c
    WHERE c.employeetype IS NOT NULL
        AND TRIM(c.employeetype) NOT IN ('', '"To Re-qualify"')
),
combined_person_values AS (
    SELECT person_id, 'pronouns' AS attribute_type, value
    FROM pronouns_values
    UNION ALL
    SELECT person_id, 'education_level' AS attribute_type, value
    FROM education_level_values
    UNION ALL
    SELECT person_id, 'current_location' AS attribute_type, value
    FROM current_location_values
    UNION ALL
    SELECT person_id, 'primary_skill' AS attribute_type, value
    FROM primary_skill_values
    UNION ALL
    SELECT person_id, 'work_authority' AS attribute_type, value
    FROM work_authority_values
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
    combined_person_values cpv
INNER JOIN 
    internal_persons ip ON ip.person_id = cpv.person_id
INNER JOIN 
    internal_options io ON io.attribute_type = cpv.attribute_type AND io.option_value = cpv.value
ORDER BY
    ip.person_id,
    io.atlas_attribute_id 