{{ config(materialized='table', alias='educations_manatal') }}

{% set db = var('source_database') %}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id,
        candidate_id
    FROM 
        {{ ref('1_people_manatal') }}
),

candidate_educations AS (
    SELECT 
        ce.id,
        ce.candidate_id,
        ce.school_name,
        ce.degree_name,
        ce.specialization,
        ce.description,
        ce.started_at,
        ce.ended_at
    FROM 
        {{ db }}.candidate_education ce
    WHERE 
        ce.school_name IS NOT NULL
        AND TRIM(ce.school_name) <> ''
),

educations_with_ids AS (
    SELECT 
        {{ atlas_uuid('ce.id') }} AS atlas_id,
        ip.person_id,
        ip.atlas_person_id,
        ce.school_name AS name,
        ce.degree_name AS degree,
        CASE 
            WHEN ce.specialization IS NULL OR TRIM(ce.specialization) = '' 
            THEN NULL
            ELSE ce.specialization
        END AS field_of_study,
        ce.description,
        'migration' AS source,
        -- Handle empty started_at
        CASE
            WHEN ce.started_at IS NULL OR TRIM(ce.started_at) = '' 
            THEN NULL
            ELSE ce.started_at
        END AS started_at,
        -- Handle empty ended_at
        CASE
            WHEN ce.ended_at IS NULL OR TRIM(ce.ended_at) = '' 
            THEN NULL
            ELSE ce.ended_at
        END AS finished_at,
        ROW_NUMBER() OVER (PARTITION BY {{ atlas_uuid('ce.id') }} ORDER BY ce.started_at) AS rn
    FROM 
        candidate_educations ce
    JOIN 
        internal_persons AS ip
        ON ip.candidate_id = ce.candidate_id
)

SELECT 
    atlas_id,
    person_id,
    atlas_person_id,
    name,
    degree,
    field_of_study,
    description,
    source,
    started_at,
    finished_at
FROM 
    educations_with_ids
WHERE 
    rn = 1
