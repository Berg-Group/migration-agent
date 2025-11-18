{{ config(
    materialized='table',
    alias='projects_loxo',
    tags=["loxo"]
) }}

WITH source_projects AS (
    SELECT
        j.id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || j.id::text") }} AS atlas_id,
        j.title AS job_role,
        j.company AS company_id,
        c.atlas_id AS atlas_company_id,
        REGEXP_REPLACE(
            COALESCE(j."desc"),
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS job_description,
        REGEXP_REPLACE(
            COALESCE(j.notes),
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS notes,
        TO_CHAR(TRY_CAST(j.created AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(TRY_CAST(j.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        CASE 
            WHEN LOWER(j.status) = 'inactive' THEN
                TO_CHAR(TRY_CAST(j.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS')
            ELSE NULL
        END AS closed_at,
        CASE
            WHEN LOWER(j.status) = 'inactive' THEN 'closed'
            WHEN LOWER(j.status) = 'hold' THEN 'on_hold'
            ELSE 'active'
        END AS state,
        CASE
            WHEN LOWER(j.status) = 'inactive' THEN 'won'
            WHEN LOWER(j.status) = 'inactive' THEN 'worked_lost'
            ELSE NULL
        END AS close_reason,
        CASE
            WHEN LOWER(j.type) LIKE '%full time%' THEN 'full_time'
            WHEN LOWER(j.type) LIKE '%contract%' THEN 'contract'
            WHEN LOWER(j.type) LIKE '%part time%' THEN 'part_time'
            ELSE 'full_time'
        END AS contract_type,
        BTRIM(REGEXP_REPLACE(COALESCE(j.address, ''), '[^a-zA-Z0-9 ]+', ' ')) AS location_street_address,
        BTRIM(REGEXP_REPLACE(COALESCE(j.city, ''), '[^a-zA-Z0-9 ]+', ' ')) AS location_metro,
        BTRIM(REGEXP_REPLACE(COALESCE(j.state, ''), '[^a-zA-Z0-9 ]+', ' ')) AS location_region,
        BTRIM(REGEXP_REPLACE(COALESCE(j.zip, ''), '[^a-zA-Z0-9 ]+', ' ')) AS location_postal_code,
        BTRIM(REGEXP_REPLACE(COALESCE(j.country, ''), '[^a-zA-Z0-9 ]+', ' ')) AS location_country
    FROM {{ var('source_database') }}.jobs j
    LEFT JOIN {{ ref('3_companies_loxo') }} c ON c.id = j.company
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
    p.closed_at,
    p.state,
    p.close_reason,
    p.contract_type,
    {{ build_location_locality(
        'p.location_street_address', 
        'NULL', 
        'p.location_metro', 
        'p.location_region', 
        'p.location_postal_code', 
        'p.location_country'
    ) }} AS location_locality,
    p.location_street_address,
    p.location_metro,
    p.location_region,
    p.location_postal_code,
    p.location_country,
    p.id AS job_number,
    NULL AS owner_id,
    '{{ var("master_id") }}' AS atlas_owner_id,
    '{{ var("agency_id") }}' AS agency_id
FROM source_projects p