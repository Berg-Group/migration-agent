{{ config(materialized='table', alias='projects_manatal') }}

{% set db = var('source_database') %}

WITH internal_companies AS (
    SELECT 
        id as company_id,
        atlas_id as atlas_company_id
    FROM {{ ref('3_companies_manatal') }}
),

internal_users AS (
    SELECT
        id,
        atlas_id
    FROM {{ ref('user_mapping') }}
)

SELECT 
    j.id AS id,
    {{ atlas_uuid('j.id') }} AS atlas_id,
    j.position_name AS job_role,
    TO_CHAR(DATE_TRUNC('day', j.created_at::timestamp), 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(DATE_TRUNC('day', j.updated_at::timestamp), 'YYYY-MM-DD"T00:00:00"') AS updated_at,   
    j.description AS description,
    'full_time' AS contract_type,
    'project' AS class_type,
    '1' AS hire_target,
    j.owner_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
    CASE 
        WHEN j.status IN ('lost', 'won') THEN 'closed' 
        WHEN j.status = 'on_hold' THEN 'on_hold'
        ELSE 'active' 
    END AS state,
    CASE 
        WHEN j.status IN ('lost', 'won') THEN 
            TO_CHAR(DATE_TRUNC('day', j.updated_at::timestamp), 'YYYY-MM-DD"T00:00:00"')
        ELSE NULL
    END AS closed_at,
    CASE 
        WHEN j.status = 'won' THEN 'won'
        WHEN j.status = 'lost' THEN 'worked_lost'
        ELSE NULL
    END AS close_reason,
    j.organization_id AS company_id,
    ic.atlas_company_id,
    FALSE AS public,
    '{{ var('agency_id')}}' AS agency_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS updated_by_id,
    j.id AS job_number
FROM 
    {{ db }}.job j
LEFT JOIN 
    internal_companies AS ic 
    ON ic.company_id = j.organization_id
LEFT JOIN 
    internal_users AS u 
    ON u.id = j.owner_id
WHERE 
    j.position_name IS NOT NULL
    AND TRIM(j.position_name) <> ''
