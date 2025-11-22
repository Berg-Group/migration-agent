{{ config(
    materialized='table',
    alias='companies_vincere'
) }}

WITH locations AS (
    SELECT 
        company_id,
        NULLIF(
            TRIM(BOTH ',. ' FROM
                REGEXP_REPLACE(
                    location_name,
                    '(\\s*,?\\s*[Nn]one\\s+[Ss]pecified)+',
                    '')),'')  AS location_locality,
        city AS location_metro,
        country AS location_country,
        state AS location_region
    FROM 
        {{var('source_database')}}."public_company_location"
    WHERE location_types_array ilike '%HEADQUARTER%'
),

base AS (
    SELECT
        c.id,
        {{ atlas_uuid("c.id::text") }} AS atlas_id,
        TRIM(c.name) AS name,
        to_char(c.insert_timestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        COALESCE(l.location_locality, l.location_metro, l.location_country, l.location_region) AS location_locality,
        l.location_metro,
        l.location_country,
        l.location_region,
        '{{ var('agency_id') }}' AS agency_id,
        'target' AS relationship

    FROM 
        {{ var('source_database') }}."public_company" c
    LEFT JOIN locations l ON l.company_id = c.id
    WHERE TRIM(c.name) <> '' AND TRIM(c.name) <> ' ' AND TRIM(c.name) IS NOT NULL
)

SELECT *
FROM base
