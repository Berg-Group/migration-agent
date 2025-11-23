{{ config(materialized='table', alias='projects_rcrm') }}

{% set db = var('source_database') %}

WITH internal_companies AS (
    SELECT 
        id as company_id,
        atlas_id as atlas_company_id
    FROM {{ ref('5_companies_rcrm') }}
),

assignments AS (
    SELECT 
        job_slug,
        MAX(to_char(date_trunc('day', timestamp 'epoch' + (stage_date::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"')) AS assignment_date
    FROM 
        {{ db }}.assignment_data
    GROUP BY 1
)


SELECT 
    j.slug AS id,
    {{ atlas_uuid('j.slug') }} AS atlas_id,
    j.name AS job_role,
    to_char(date_trunc('day', timestamp 'epoch' + (j.created_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"') AS created_at,
    to_char(date_trunc('day', timestamp 'epoch' + (j.updated_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"') AS updated_at,   
    {{ clean_html('jd.job_description') }} AS description,
    'full_time' AS contract_type,
    'project' AS class_type,
    '1' AS hire_target,
    j.owner_id,
    COALESCE(o.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
    CASE WHEN job_status IN ('Filled', 'Closed') THEN 'closed' 
        WHEN job_status = 'On Hold' THEN 'on_hold'
        WHEN job_status = 'Lead/ Prospect' THEN 'pitch'
        ELSE 'active' END AS state,
    CASE 
        WHEN job_status IN ('Filled', 'Closed') THEN 
            COALESCE(a.assignment_date, to_char(date_trunc('day', timestamp 'epoch' + (j.updated_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"'))
        WHEN job_status = 'Lead/ Prospect' AND to_char(date_trunc('day', timestamp 'epoch' + (j.created_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"') < '2025-03-01T00:00:00' THEN
            '2025-05-10T00:00:00'
        END AS closed_at,
    CASE 
        WHEN job_status = 'Filled' THEN 'won' 
        WHEN job_status = 'Closed' THEN 'worked_lost'
        WHEN job_status = 'Lead/ Prospect' AND to_char(date_trunc('day', timestamp 'epoch' + (j.created_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"') < '2025-03-01T00:00:00' THEN
            'pitch_lost'
        END AS close_reason,
    j.company_slug AS company_id,
    ic.atlas_company_id AS atlas_company_id,
    FALSE AS public,
    '{{ var('agency_id')}}' AS agency_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_id,
    COALESCE(u2.atlas_id, '{{ var("master_id") }}') AS updated_by_id,
    {{ clean_html('nd.note') }}  AS notes,
    job_code AS job_number
FROM 
    {{ db }}.job_data j
LEFT JOIN 
    {{ db }}.job_description_data jd 
    ON jd.job_slug = j.slug
LEFT JOIN 
    internal_companies AS ic 
    ON ic.company_id = j.company_slug
LEFT JOIN 
    {{ ref('user_mapping') }} AS o 
    ON o.id = j.owner_id
LEFT JOIN 
    {{ ref('user_mapping') }} AS u
    ON u.id = j.created_by
LEFT JOIN 
    {{ ref('user_mapping') }} AS u2
    ON u2.id = j.updated_by
LEFT JOIN 
    assignments AS a 
    ON a.job_slug = j.slug
LEFT JOIN 
    {{ db }}."note_data" nd
    ON nd.related_to = j.slug
