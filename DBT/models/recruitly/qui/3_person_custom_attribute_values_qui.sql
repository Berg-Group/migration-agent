{{ config(
    materialized='table',
    alias='person_custom_attribute_values_qui',
    tags = ["qui"]
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id
    FROM 
        {{ ref('2_people_rect') }}
),  
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        cao.atlas_id AS option_id,
        cao.id AS external_id,
        ca.alias AS attribute_type,
        cao.value AS option_value
    FROM 
        {{ ref('2_custom_attribute_options_qui') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_qui') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'person'
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
pipeline_values AS (
    SELECT DISTINCT
        jp.candidate_id AS person_id,
        TRIM(jp.pipeline_state) AS value
    FROM qui_rec_public.job_pipelines jp
    WHERE jp.pipeline_state IS NOT NULL
      AND TRIM(jp.pipeline_state) != ''
),
gender_values AS (
    SELECT DISTINCT
        c.candidate_id AS person_id,
        TRIM(c.gender) AS value
    FROM {{ var('source_database') }}.candidates c
    WHERE c.gender IS NOT NULL AND TRIM(c.gender) != ''
),
nationality_values AS (
    SELECT DISTINCT
        c.candidate_id AS person_id,
        TRIM(c.nationality) AS value
    FROM {{ var('source_database') }}.candidates c
    WHERE c.nationality IS NOT NULL AND TRIM(c.nationality) != ''
),
sector_values AS (
    SELECT DISTINCT
        c.candidate_id AS person_id,
        TRIM(SPLIT_PART(c.sectors, ',', numbers.n)) AS value
    FROM {{ var('source_database') }}.candidates c
    CROSS JOIN numbers
    WHERE c.sectors IS NOT NULL AND TRIM(c.sectors) != ''
      AND SPLIT_PART(c.sectors, ',', numbers.n) != ''
),
industry_values AS (
    SELECT DISTINCT
        c.candidate_id AS person_id,
        TRIM(SPLIT_PART(c.industries, ',', numbers.n)) AS value
    FROM {{ var('source_database') }}.candidates c
    CROSS JOIN numbers
    WHERE c.industries IS NOT NULL AND TRIM(c.industries) != ''
      AND SPLIT_PART(c.industries, ',', numbers.n) != ''
),
combined_person_values AS (
    SELECT person_id, 'recruiting_interviews' AS attribute_type, value FROM pipeline_values
    UNION ALL
    SELECT person_id, 'gender' AS attribute_type, value FROM gender_values
    UNION ALL
    SELECT person_id, 'nationality' AS attribute_type, value FROM nationality_values
    UNION ALL
    SELECT person_id, 'sectors' AS attribute_type, value FROM sector_values
    UNION ALL
    SELECT person_id, 'industries' AS attribute_type, value FROM industry_values
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

