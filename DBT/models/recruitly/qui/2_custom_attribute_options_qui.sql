{{ config(
    materialized='table',
    alias='custom_attribute_options_qui',
    tags = ["qui"]
) }}

WITH internal_attributes AS (
    SELECT
        atlas_id,
        alias
    FROM
        {{ ref('1_custom_attributes_qui') }}
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
pipeline_states AS (
    SELECT DISTINCT TRIM(jp.pipeline_state) AS value
    FROM qui_rec_public.job_pipelines jp 
    WHERE jp.pipeline_state IS NOT NULL
      AND TRIM(jp.pipeline_state) != ''
),
genders AS (
    SELECT DISTINCT TRIM(c.gender) AS value
    FROM {{ var('source_database') }}.candidates c
    WHERE c.gender IS NOT NULL AND TRIM(c.gender) != ''
),
nationalities AS (
    SELECT DISTINCT TRIM(c.nationality) AS value
    FROM {{ var('source_database') }}.candidates c
    WHERE c.nationality IS NOT NULL AND TRIM(c.nationality) != ''
),
sectors AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(c.sectors, ',', numbers.n)) AS value
    FROM {{ var('source_database') }}.candidates c
    CROSS JOIN numbers
    WHERE c.sectors IS NOT NULL
      AND TRIM(c.sectors) != ''
      AND SPLIT_PART(c.sectors, ',', numbers.n) != ''
),
industries AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(c.industries, ',', numbers.n)) AS value
    FROM {{ var('source_database') }}.candidates c
    CROSS JOIN numbers
    WHERE c.industries IS NOT NULL
      AND TRIM(c.industries) != ''
      AND SPLIT_PART(c.industries, ',', numbers.n) != ''
),
combined_values AS (
    SELECT 'recruiting_interviews' AS alias, value FROM pipeline_states
    UNION ALL
    SELECT 'gender' AS alias, value FROM genders
    UNION ALL
    SELECT 'nationality' AS alias, value FROM nationalities
    UNION ALL
    SELECT 'sectors' AS alias, value FROM sectors
    UNION ALL
    SELECT 'industries' AS alias, value FROM industries
)
SELECT
    cv.alias || '_' || value AS id,
    {{ atlas_uuid("cv.alias || value") }} AS atlas_id,
    ia.atlas_id AS atlas_attribute_id,
    value AS value,
    ROW_NUMBER() OVER (PARTITION BY ia.atlas_id ORDER BY value ASC) AS position,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    combined_values cv
INNER JOIN 
    internal_attributes ia ON ia.alias = cv.alias
ORDER BY
    atlas_attribute_id,
    position

