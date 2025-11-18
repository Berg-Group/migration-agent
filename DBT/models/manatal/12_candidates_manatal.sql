{{ config(materialized='table', alias='candidates_manatal') }}

{% set db = var('source_database') %}

WITH internal_persons AS (
    SELECT 
        id as person_id,
        atlas_id as atlas_person_id,
        candidate_id
    FROM {{ ref('1_people_manatal') }}
),

internal_projects AS (
    SELECT 
        id AS project_id,
        atlas_id AS atlas_project_id
    FROM {{ ref('10_projects_manatal') }}
),

-- Base match data with minimal transformations
base_match AS (
    SELECT
        id,
        candidate_id,
        job_id,
        creator_id,
        created_at,
        updated_at,
        dropped_at,
        hired_at,
        offer_at,
        interview_at,
        submitted_at
    FROM {{ db }}.match
)

-- Add debugging CTE to check status values
SELECT 
    bm.id AS id,
    {{ atlas_uuid('bm.id::text') }} AS atlas_id,
    ip.person_id,
    ip.atlas_person_id,
    bm.job_id AS project_id,
    ipp.atlas_project_id,
    'Candidate' AS class_type,
    -- Debug status fields directly
    CASE 
        WHEN bm.hired_at IS NOT NULL AND bm.hired_at::text <> '' THEN 'Placed'
        WHEN bm.offer_at IS NOT NULL AND bm.offer_at::text <> '' THEN 'Offer'
        WHEN bm.interview_at IS NOT NULL AND bm.interview_at::text <> '' THEN 'Client IV'
        WHEN bm.submitted_at IS NOT NULL AND bm.submitted_at::text <> '' THEN 'Presented'
        ELSE 'Added'
    END AS status,
    bm.creator_id AS owner_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
    TO_CHAR(current_date, 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(current_date, 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    -- Refine rejection fields while keeping status logic intact
    CASE
        WHEN bm.dropped_at IS NOT NULL AND bm.dropped_at::text <> '' THEN 'by_us'
        ELSE NULL
    END AS rejection_type,
    CASE
        WHEN bm.dropped_at IS NOT NULL AND bm.dropped_at::text <> '' THEN 'other'
        ELSE NULL
    END AS rejection_reason,
    CASE
        WHEN bm.dropped_at IS NOT NULL AND bm.dropped_at::text <> '' THEN 
            TO_CHAR(DATE_TRUNC('day', bm.dropped_at::timestamp), 'YYYY-MM-DD"T00:00:00"')
        ELSE NULL
    END AS rejected_at,
    -- Debug values
    bm.hired_at::text AS debug_hired_at,
    bm.offer_at::text AS debug_offer_at,
    bm.interview_at::text AS debug_interview_at,
    bm.submitted_at::text AS debug_submitted_at
FROM 
    base_match bm
JOIN 
    internal_persons AS ip 
    ON ip.candidate_id = bm.candidate_id
LEFT JOIN 
    internal_projects AS ipp
    ON ipp.project_id = bm.job_id
LEFT JOIN
    {{ ref('user_mapping') }} AS u
    ON u.id = bm.creator_id
WHERE
    ip.atlas_person_id IS NOT NULL