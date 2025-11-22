{{ config(materialized='table', alias='experiences_manatal') }}

{% set db = var('source_database') %}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id,
        candidate_id
    FROM 
        {{ ref('1_people_manatal') }}
),

internal_companies AS (
    SELECT
        id AS company_id,
        atlas_id AS atlas_company_id,
        name AS company_name
    FROM 
        {{ ref('3_companies_manatal') }}
),

candidate_experiences AS (
    SELECT 
        ce.id,
        ce.candidate_id,
        ce.started_at,
        ce.ended_at,
        ce.position_name AS title,
        ce.description,
        ce.employer_name AS company_name
    FROM 
        {{ db }}.candidate_experience ce
    WHERE 
        -- Remove entries with NULL or empty started_at
        ce.started_at IS NOT NULL
        AND TRIM(ce.started_at) <> ''
        AND ce.position_name IS NOT NULL
        AND ce.employer_name IS NOT NULL
        AND TRIM(ce.employer_name) <> ''
),

experiences_with_company AS (
    SELECT 
        {{ atlas_uuid('ce.id') }} AS atlas_id,
        ip.person_id,
        ip.atlas_person_id,
        ic.company_id,
        ic.atlas_company_id,
        -- Use the dates directly
        ce.started_at AS started_at,
        -- Convert empty finished_at to NULL
        CASE
            WHEN ce.ended_at IS NULL OR TRIM(ce.ended_at) = '' 
            THEN NULL
            ELSE ce.ended_at
        END AS finished_at,
        ce.title,
        ce.description,
        'migration' AS source,
        COALESCE(ic.company_name, ce.company_name) AS company_name
    FROM 
        candidate_experiences ce
    JOIN 
        internal_persons AS ip
        ON ip.candidate_id = ce.candidate_id
    LEFT JOIN 
        internal_companies AS ic
        ON lower(trim(ic.company_name)) = lower(trim(ce.company_name))
    WHERE
        COALESCE(ic.company_name, ce.company_name) IS NOT NULL
),

-- Deduplicate based on atlas_id
deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY atlas_id 
            ORDER BY 
                atlas_person_id NULLS LAST,
                atlas_company_id NULLS LAST,
                started_at
        ) AS rn
    FROM experiences_with_company
)

SELECT 
    atlas_id,
    person_id,
    atlas_person_id,
    company_id,
    atlas_company_id,
    started_at,
    finished_at,
    title,
    description,
    source,
    company_name
FROM 
    deduplicated
WHERE 
    rn = 1


