{{ config(
    materialized='table',
    alias='companies_loxo',
    tags=["loxo"]
) }}

WITH companies AS (
    SELECT
        c.id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || c.id::text") }} AS atlas_id,
        c.name,
        CASE 
            WHEN LOWER(ct.value) LIKE '%client%' THEN 'client'
            ELSE 'none'
        END AS relationship,
        REGEXP_REPLACE(
            COALESCE(c."desc", c.culture),
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS summary,
        TO_CHAR(TRY_CAST(c.created AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(TRY_CAST(c.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
    FROM 
        {{ var('source_database') }}.companies c
    LEFT JOIN {{ var('source_database') }}.companies_types ct ON ct.root_id = c.id AND ct.row_index = 1
    WHERE 
        c.hidden IS NULL OR LOWER(c.hidden) != 'true'
),
company_addresses AS (
    SELECT * 
    FROM (
        SELECT
            ca.root_id AS company_id,
            btrim(regexp_replace(coalesce(ca.city, ''), '[^a-zA-Z0-9 ]+', ' ')) AS city,
            btrim(regexp_replace(coalesce(ca.state, ''), '[^a-zA-Z0-9 ]+', ' ')) AS state,
            btrim(regexp_replace(coalesce(ca.country, ''), '[^a-zA-Z0-9 ]+', ' ')) AS country,
            btrim(regexp_replace(coalesce(ca.zip, ''), '[^a-zA-Z0-9 ]+', ' ')) AS zip,
            row_number() OVER (PARTITION BY ca.root_id ORDER BY ca.root_id) AS rn
        FROM {{ var('source_database') }}.companies_addresses ca
    ) WHERE rn = 1
)
SELECT
    c.atlas_id,
    c.id,
    c.name,
    c.summary,
    c.relationship,
    NULL AS location_street_address,
    ca.city AS location_metro,
    ca.state AS location_region,
    ca.zip AS location_postal_code,
    ca.country AS location_country,
    {{ build_location_locality('NULL', 'NULL', 'ca.city', 'ca.state', 'ca.zip', 'ca.country') }} AS location_locality,
    c.created_at,
    c.updated_at
FROM companies c
LEFT JOIN company_addresses ca ON ca.company_id = c.id
