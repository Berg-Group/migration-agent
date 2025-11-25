{{ config(
    materialized='table',
    alias='projects_ezekia'
) }}

WITH companies_map AS (
    SELECT
        c.id,
        c.atlas_id AS atlas_company_id
    FROM {{ ref('companies_ezekia') }} c
),

internal_users AS (
    SELECT 
        id AS owner_id,
        atlas_id AS atlas_owner_id 
    FROM 
        {{ref('users_ezekia')}}
),

base_briefs AS (
    SELECT
        sb.brief_id AS id,
        {{atlas_uuid('sb.brief_id::text')}} AS atlas_id,
        TO_CHAR(sb.created_at::TIMESTAMP, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(sb.updated_at::TIMESTAMP, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        sb.title     AS job_role,
        sb.client_id AS client_id,
        TO_CHAR(sb.end_date::timestamp, 'YYYY-MM-DD"T"00:00:00') AS closed_at,
        'false'   AS public,
        'project' AS class_type,
        coalesce(user_id, '1') as owner_id,
        COALESCE(atlas_owner_id, '{{var("master_id")}}') AS atlas_owner_id,
        sb.description AS job_description_text,
        sb.label
    FROM {{ var("source_database") }}.search_firms_briefs sb
    LEFT JOIN internal_users iu ON iu.owner_id = sb.user_id
    WHERE deleted_at IS NULL  
),

comp_ranges AS (
    SELECT
        bcr.brief_id,
        bcr.min,
        bcr.max,
        bcr.currency
    FROM {{ var("source_database") }}.briefs_compensation_ranges bcr
),

joined AS (
    SELECT
        bb.id,
        bb.atlas_id,
        bb.created_at,
        bb.updated_at,
        bb.job_role,
        bb.client_id,
        bb.closed_at,
        bb.public,
        bb.class_type,
        CASE
            WHEN cr.min IS NOT NULL AND cr.max IS NOT NULL THEN cr.min::text || ' - ' || cr.max::text
            WHEN cr.min IS NOT NULL THEN cr.min::text
            WHEN cr.max IS NOT NULL THEN cr.max::text
            ELSE NULL
        END AS salary,
        bb.label,
        cr.currency AS salary_currency,
        owner_id,
        atlas_owner_id,
        job_description_text
    FROM base_briefs bb
    LEFT JOIN comp_ranges cr
           ON bb.id = cr.brief_id
),

final AS (
    SELECT
        j.id,
        j.atlas_id,
        j.created_at,
        j.updated_at,
        j.job_role,
        j.client_id,
        cm.atlas_company_id,
        j.public,
        j.class_type,
        j.closed_at,
        j.salary,
        j.salary_currency,
        j.owner_id,
        j.atlas_owner_id,
        j.job_description_text,
        j.label
    FROM joined j
    LEFT JOIN companies_map cm
           ON j.client_id = cm.id
)

SELECT
    id,
    atlas_id,
    created_at,
    updated_at,
    job_role,
    client_id AS company_id,
    COALESCE(atlas_company_id, '9f6c77dc-b4ae-5579-7a6a-b94e759cb85e') AS atlas_company_id, --fallback for Dryden
    public,
    class_type,
    COALESCE(closed_at, TO_CHAR(current_timestamp, 'YYYY-MM-DD"T"00:00:00')) AS closed_at, --closing specifically for Dryden
    salary,
    salary_currency,
    'closed' AS state, --specifically for dryden
    CASE
        WHEN closed_at IS NOT NULL THEN 'won'
        ELSE 'cancelled'
    END AS close_reason,
    owner_id,
    atlas_owner_id,
    job_description_text
FROM final
