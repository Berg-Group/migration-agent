{{ config(
    materialized='table',
    alias='outdated_check_and_delete'
) }}

WITH base AS (
    SELECT
        s.joborderid AS id,
        lower(
            substring(md5(s.joborderid::text || '{{ var("clientName") }}'), 1, 8) || '-' ||
            substring(md5(s.joborderid::text || '{{ var("clientName") }}'), 9, 4) || '-' ||
            substring(md5(s.joborderid::text || '{{ var("clientName") }}'), 13, 4) || '-' ||
            substring(md5(s.joborderid::text || '{{ var("clientName") }}'), 17, 4) || '-' ||
            substring(md5(s.joborderid::text || '{{ var("clientName") }}'), 21, 12)
        ) AS atlas_id,
        to_char(s.datecreated::date,'YYYY-MM-DD"T00:00:00"') AS created_at,
        to_char(s.dateupdated::date,'YYYY-MM-DD"T00:00:00"') AS updated_at,
        {{clean_html('s.jobdescription')}} AS job_description,
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
        to_char(s.dateclosed::date, 'YYYY-MM-DD"T00:00:00"') AS closed_at,
        CASE
            WHEN s.placementtype = 'permanent' THEN 'full_time'
            ELSE 'contract'
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
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
        false AS public,
        'project' AS class_type,

        -- Concatenate s.feerate with '%' for d.fee_terms
        CONCAT(s.feerate, '%') AS fee_terms,

        -- Map s.feeamount to d.expected_fee
        s.feeamount AS expected_fee,

        -- Map s.feecurrencycode to d.expected_fee_currency
        s.feecurrencycode AS expected_fee_currency

    FROM
        {{ var('source_database') }}.joborder s
    LEFT JOIN
        {{ var('source_database') }}.location l ON s.locationid = l.locationid
    LEFT JOIN
       {{ref('3_companies_ja')}} c ON s.companyid = c.id
    LEFT JOIN 
        {{ref('users_rz')}} u ON u.id = s.owneruserid
    WHERE s.deleted = FALSE 
)
SELECT *
FROM base
