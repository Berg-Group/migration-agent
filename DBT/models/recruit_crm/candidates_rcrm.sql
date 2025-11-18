{{ config(materialized='table', alias='candidates_rcrm') }}

{% set db = var('source_database') %}

WITH internal_persons AS (
    SELECT 
        id as person_id,
        atlas_id as atlas_person_id
    FROM {{ ref('people_rcrm') }}
),

internal_projects AS (
    SELECT 
        id  AS project_id,
        atlas_id AS atlas_project_id
    FROM {{ ref('projects_rcrm') }}
),

candidate_data AS (
    SELECT 
        slug AS candidate_slug,
        owner_id
    FROM {{ db }}.candidate_data
)

SELECT 
    {{ atlas_uuid('a.candidate_slug || a.job_slug') }} AS atlas_id,
    a.candidate_slug AS person_id,
    p.atlas_person_id,
    a.job_slug AS project_id,
    ipp.atlas_project_id,
    'Candidate' AS class_type,
    CASE WHEN a.candidate_status IN (
        'Assigned',
        'Rejected',
        'Applied',
        'Long List/ Targets'
    ) THEN 'Added'
    WHEN a.candidate_status IN ('CV Sent',
        'Shortlist')
        THEN 'Presented'
    WHEN a.candidate_status IN (
        '1st round Interview',
        '2nd Round Interview',
        '3rd Round Interview',
        'Final Interview'
    ) THEN 'Client IV' 
    WHEN a.candidate_status = 'Did Not Join' THEN 'Offer'
    WHEN a.candidate_status = 'Placed' THEN 'Placed' END AS status, 
    cd.owner_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
    to_char(date_trunc('day', timestamp 'epoch' + (a.created_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"') AS created_at,
    to_char(date_trunc('day', timestamp 'epoch' + (a.updated_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"') AS updated_at

FROM 
    {{ db }}.assignment_data  a
LEFT JOIN 
    internal_persons AS p 
    ON p.person_id = a.candidate_slug
LEFT JOIN 
    internal_projects AS ipp
    ON ipp.project_id = a.job_slug
LEFT JOIN
    candidate_data AS cd
    ON cd.candidate_slug = a.candidate_slug
LEFT JOIN
    {{ ref('user_mapping') }} AS u
    ON u.id = cd.owner_id