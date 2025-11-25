{{ config(
    materialized='table',
    alias='projects_rect',
    tags=['recruitly']
) }}

WITH source_projects AS (
    SELECT
        j.job_id AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || j.job_id::text") }} AS atlas_id,
        NULLIF(TRIM(j.title), '') AS job_role,
        CASE 
            WHEN j.job_description ILIKE '%This is a dummy job%' THEN NULL
            ELSE {{ html_to_markdown('j.job_description') }}
        END AS job_description,
        CASE 
            WHEN j.short_description ILIKE '%This is a dummy job%' THEN NULL
            ELSE {{ html_to_markdown('j.short_description') }}
        END AS notes,
        j.company_id AS company_id,
        c.atlas_id AS atlas_company_id,
        {{ string_to_timestamp('j.createdon') }} AS created_at,
        {{ string_to_timestamp('j.modifiedon') }} AS updated_at,
        CASE
            WHEN LOWER(NULLIF(TRIM(j.job_type), '')) LIKE '%permanent%' OR LOWER(NULLIF(TRIM(j.job_type), '')) LIKE '%full%' THEN 'full_time'
            WHEN LOWER(NULLIF(TRIM(j.job_type), '')) LIKE '%contract%' THEN 'contract'
            WHEN LOWER(NULLIF(TRIM(j.job_type), '')) LIKE '%part%' THEN 'part_time'
            ELSE 'full_time'
        END AS contract_type,
        LOWER(COALESCE(j.archived::varchar, 'false')) AS archived_str,
        NULLIF(TRIM(j.job_location), '') AS location_locality,
        j.job_reference_id AS job_number,
        j.owner_id AS owner_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
        '{{ var("agency_id") }}' AS agency_id
    FROM {{ var('source_database') }}.jobs j
    INNER JOIN {{ ref('4_companies_rect') }} c ON c.id = j.company_id
    LEFT JOIN {{ ref('1_users_rect') }} u ON u.id = j.owner_id
)
SELECT
    p.id,
    p.atlas_id,
    p.job_role,
    p.job_description,
    p.notes,
    p.company_id,
    p.atlas_company_id,
    p.created_at,
    p.updated_at,
    CASE 
        WHEN p.archived_str = 'true' THEN 'closed'
        WHEN to_timestamp(p.created_at, 'YYYY-MM-DD"T"HH24:MI:SS')::date < CURRENT_DATE - INTERVAL '6 months' THEN 'closed'
        ELSE 'active'
    END AS state,
    CASE 
        WHEN p.archived_str = 'true' THEN 'worked_lost'
        WHEN to_timestamp(p.created_at, 'YYYY-MM-DD"T"HH24:MI:SS')::date < CURRENT_DATE - INTERVAL '6 months' THEN 'worked_lost'
        ELSE NULL
    END AS close_reason,
    CASE 
        WHEN p.archived_str = 'true' THEN p.updated_at
        WHEN to_timestamp(p.created_at, 'YYYY-MM-DD"T"HH24:MI:SS')::date < CURRENT_DATE - INTERVAL '6 months' THEN p.updated_at
        ELSE NULL
    END AS closed_at,
    p.contract_type,
    p.location_locality,
    p.job_number,
    p.owner_id,
    p.atlas_owner_id,
    p.agency_id
FROM source_projects p