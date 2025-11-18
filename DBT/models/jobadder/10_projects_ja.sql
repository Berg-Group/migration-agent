{{ config(
    materialized='table',
    alias='projects_ja'
) }}

WITH base AS (
    SELECT
        s.joborderid AS id,
        s.joborderid::text || '{{ var("clientName") }}' AS uuid_input,
        to_char(
            s.datecreated::date,
            'YYYY-MM-DD"T00:00:00"'
        ) AS created_at,
        to_char(
            s.dateupdated::date,
            'YYYY-MM-DD"T00:00:00"'
        ) AS updated_at,
        regexp_replace(s.jobdescription, '<[^>]*>', ' ') AS job_description,
        s.jobtitle AS job_role,
        CASE
            WHEN s.dateclosed IS NULL THEN 'active'
            ELSE 'closed'
        END AS state,
        CASE
            WHEN s.dateclosed IS NOT NULL THEN
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM {{ var('source_database') }}.placement p
                        WHERE p.joborderid = s.joborderid
                    ) THEN 'won'
                    ELSE 'worked_lost'
                END
            ELSE NULL
        END AS close_reason,
        to_char(
            s.dateclosed::date,
            'YYYY-MM-DD"T00:00:00"'
        ) AS closed_at,
        CASE
            WHEN s.placementtype = 'permanent' THEN 'full_time'
            ELSE 'full_time'
        END AS contract_type,
        l.name AS location_locality,
        s.numberofjobs AS hire_target,
        s.companyid AS company_id,
        c.atlas_id AS atlas_company_id,
        CASE
            WHEN s.dateclosed IS NOT NULL THEN '{{ var("master_id") }}'
            ELSE NULL
        END AS closed_by_id,
        s.owneruserid AS owner_id,
        false AS public,
        'project' AS class_type,
        CONCAT(s.feerate, '%') AS fee_terms,
        s.feeamount AS expected_fee,
        s.feecurrencycode AS expected_fee_currency

    FROM
        {{ var('source_database') }}.joborder s
    LEFT JOIN
        {{ var('source_database') }}.location l USING (locationid)
    INNER JOIN
        {{ref('3_companies_ja')}} c ON s.companyid = c.id
),

-- Get user mappings for owner_id to atlas_owner_id
user_mapping AS (
    SELECT
        id AS user_id,
        atlas_id AS user_atlas_id
    FROM {{ ref('users_ja') }}
)

SELECT
    base.id,
    {{ atlas_uuid('base.uuid_input') }} AS atlas_id,
    base.created_at,
    base.updated_at,
    base.job_description,
    base.job_role,
    base.state,
    base.close_reason,
    base.closed_at,
    base.contract_type,
    base.location_locality,
    base.hire_target,
    base.company_id,
    base.atlas_company_id,
    base.closed_by_id,
    base.owner_id,
    COALESCE(um.user_atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
    base.public,
    base.class_type,
    base.fee_terms,
    base.expected_fee,
    base.expected_fee_currency
FROM base
LEFT JOIN user_mapping um ON base.owner_id = um.user_id
