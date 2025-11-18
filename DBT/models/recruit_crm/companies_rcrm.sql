{{ config(materialized='table', alias='companies_rcrm') }}

{% set db = var('source_database') %}

WITH src AS (

    /* ───────── pull raw rows ───────── */
    SELECT
        slug                          AS id,
        {{ atlas_uuid('slug') }}      AS atlas_id,
        company                       AS name,

        /* nice-to-have inputs that do exist */
        full_address                  AS location_locality_raw,
        city                          AS city_raw,
        {{number_range('company_size')}}  AS company_size,
        city                          AS location_metro,
        company_state                 AS location_region,
        country                       AS location_country,
        created_on::bigint            AS created_on_epoch,
        updated_on::bigint            AS updated_on_epoch
    FROM {{ db }}.company_data
),

clean AS (
    SELECT
        id,
        atlas_id,
        name,
        'target'                                   AS relationship,

        /* locality fallback: full_address → city → NULL */
        CASE
            WHEN COALESCE(TRIM(location_locality_raw), '') <> ''
                 THEN TRIM(location_locality_raw)
            WHEN COALESCE(TRIM(city_raw), '') <> ''
                 THEN TRIM(city_raw)
            ELSE NULL
        END                                         AS location_locality,

        city_raw                                    AS location_name,

        /* company_data has no summary column → always NULL */
        NULL                                        AS summary,

        /* midnight ISO-8601 strings */
        TO_CHAR(
            DATE_TRUNC(
                'day',
                TIMESTAMP 'epoch' + created_on_epoch * INTERVAL '1 second'
            ),
            'YYYY-MM-DD"T00:00:00"'
        )                                           AS created_at,

        TO_CHAR(
            DATE_TRUNC(
                'day',
                TIMESTAMP 'epoch' + updated_on_epoch * INTERVAL '1 second'
            ),
            'YYYY-MM-DD"T00:00:00"'
        )                                           AS updated_at,

        company_size,
        location_region,
        location_metro,
        location_country
    FROM src
)

SELECT
    id,
    atlas_id,
    name,
    relationship,
    location_locality,
    location_name,
    location_region,
    location_metro,
    location_country,
    summary,
    created_at,
    updated_at,
    company_size AS size
FROM clean
