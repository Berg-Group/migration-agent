{{ config(
    materialized='table',
    alias='projects_vincere'
) }}

WITH internal_users AS (
    SELECT id, atlas_id FROM {{ ref('users_vin') }}
),

base AS (
    SELECT
        s.id AS id,
        {{ atlas_uuid("'vincere_project_'::varchar || '{{ var(\"clientName\") }}'::varchar || s.id::varchar") }} AS atlas_id,
        s.name AS job_role,
        to_char(s.insert_timestamp::timestamp(0),'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        to_char(coalesce(s.updated_timestamp, s.insert_timestamp)::timestamp(0),'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        {{ clean_html('s.public_description') }} AS job_description,
        CASE 
            WHEN lower(js.name) ilike '%hold%' THEN 'on_hold' 
            WHEN lower(js.name) ilike '%cancelled%' THEN 'closed'
            WHEN lower(js.name) ilike '%open%' THEN 'active'
            ELSE lower(js.name)
        END AS state,
        js.name AS original_state,
        CASE WHEN s.active IN (2,3) THEN 'cancelled' END AS close_reason,
        CASE WHEN s.active IN (2,3) THEN to_char(coalesce(s.updated_timestamp, s.insert_timestamp)::timestamp(0),'YYYY-MM-DD"T"HH24:MI:SS') END AS closed_at,
        1 AS hire_target,
        '{{ var("agency_id") }}' AS agency_id,
        s.company_id AS company_id,
        cv.atlas_id AS atlas_company_id,
        s.creator_account_id::varchar AS owner_id,
        coalesce(iu.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
        '{{ var("master_id") }}' AS created_by_atlas_id,
        false AS public,
        s.forecast_annual_fee AS expected_fee,
        'GBP' AS expected_fee_currency,
        'project' AS class_type,
        s.active AS active_code
    FROM {{ var('source_database') }}."public_position_description" s
    LEFT JOIN {{ ref('3_companies_vin') }} cv ON s.company_id = cv.id
    LEFT JOIN {{ var('source_database') }}.public_job_status js ON js.id = s.active
    LEFT JOIN internal_users iu ON iu.id = s.creator_account_id
),

hires AS (
    SELECT
        s.position_description_id AS project_id,
        min(date_trunc('second', s.hire_date::timestamp)) AS first_hire_dt
    FROM {{ var('source_database') }}."public_position_candidate" s
    WHERE s.hire_date IS NOT NULL
    GROUP BY 1
)

SELECT 
    b.id,
    b.atlas_id,
    b.job_role,
    b.created_at,
    b.updated_at,
    b.job_description,
    b.original_state,
    CASE 
        WHEN h.first_hire_dt IS NOT NULL THEN 'closed'
        WHEN b.created_at < '2025-02-01T00:00:00' THEN 'closed'
        WHEN b.original_state ILIKE '%open%' THEN 'active' 
        WHEN b.original_state ILIKE '%on%hold%' THEN 'on-hold'
        WHEN b.original_state ILIKE '%closed%' THEN 'closed'
        WHEN b.original_state ILIKE '%cancelled%' THEN 'closed'
        ELSE 'active'
    END AS state,
    CASE
        WHEN h.first_hire_dt IS NOT NULL THEN 'won'
        WHEN b.created_at < '2025-02-01T00:00:00' THEN 'cancelled'
        WHEN b.original_state ILIKE '%closed%' THEN 'cancelled'
        WHEN b.original_state ILIKE '%cancelled%' THEN 'cancelled'
        ELSE b.close_reason
    END AS close_reason,
    CASE
        WHEN h.first_hire_dt IS NOT NULL THEN b.updated_at
        WHEN b.created_at < '2025-02-01T00:00:00' THEN b.updated_at
        WHEN b.original_state ILIKE '%closed%' THEN b.updated_at
        WHEN b.original_state ILIKE '%cancelled%' THEN b.updated_at
        ELSE b.closed_at
    END AS closed_at,
    b.hire_target,
    b.agency_id,
    b.company_id,
    b.atlas_company_id,
    b.owner_id,
    b.atlas_owner_id,
    b.public,
    b.expected_fee,
    b.expected_fee_currency,
    b.class_type
FROM base b
LEFT JOIN hires h ON h.project_id = b.id