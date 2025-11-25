{{ config(
    materialized = 'table',
    alias        = 'project_fees_ft',
    tags=["bullhorn"]
) }}

WITH internal_projects AS (
    SELECT 
        id AS project_id,
        atlas_id AS atlas_project_id
    FROM 
        {{ ref('10_projects_ft') }}
),
internal_fee_types AS (
    SELECT 
        name AS fee_type_name,
        agency_id,
        atlas_id AS fee_type_id
    FROM 
        {{ ref('14_fee_types_ft') }}
),

internal_users AS (
    SELECT 
        id AS user_id,
        atlas_id AS atlas_user_id
    FROM
        {{ref('users_ft')}}
)
SELECT
    j.customfloat3 || '_projected_' || j.JobPostingID AS id,
    {{ atlas_uuid("j.customfloat3 || '_projected_' || j.JobPostingID") }} AS atlas_id,
    ip.project_id,
    ip.atlas_project_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    NULL AS deleted_at,
    j.userid AS created_by_id,
    COALESCE(atlas_user_id, '{{ var("master_id") }}') AS created_by_atlas_id,
    '{{ var("agency_id") }}' AS agency_id,
    ift.fee_type_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS fee_date,
    TRIM(j.customfloat3) AS amount,
    TRIM(j.customfloat3) AS default_amount,
    'GBP' AS currency,
    'projected' AS project_fee_status,
    'Migrated Agreed Fee' AS notes,
    'GBP' AS agency_currency
FROM {{ var('source_database') }}.bh_jobopportunity j
LEFT JOIN internal_users io ON io.user_id = j.userid
INNER JOIN internal_projects ip ON ip.project_id = j.JobPostingID
INNER JOIN internal_fee_types ift ON ift.agency_id = '{{ var("agency_id") }}'
WHERE j.customfloat3 IS NOT NULL AND TRIM(j.customfloat3) != '' AND TRIM(j.customfloat3) != '0'