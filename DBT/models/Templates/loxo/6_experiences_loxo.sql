{{ config(
    materialized='table',
    alias='experiences_loxo',
    tags=["loxo"]
) }}

WITH raw_experiences AS (
    SELECT
        e.row_uid AS id,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        CAST(e.companyid AS VARCHAR) AS company_id,
        c.atlas_id AS atlas_company_id,
        NULLIF(TRIM(e.company), '') AS company_name,
        NULLIF(TRIM(e.title), '') AS title,
        REGEXP_REPLACE(
            COALESCE(e."desc"),
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS description,
        CASE 
            WHEN e.from_year IS NOT NULL AND e.from_year > 0 AND e.from_month IS NOT NULL AND e.from_month BETWEEN 1 AND 12
                THEN LPAD(e.from_year::text, 4, '0') || '-' || LPAD(e.from_month::text, 2, '0') || '-01'
            WHEN e.from_year IS NOT NULL AND e.from_year > 0
                THEN LPAD(e.from_year::text, 4, '0') || '-01-01'
            ELSE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
        END AS started_at,
        CASE
            WHEN COALESCE(LOWER(e.current), 'false') = 'true' THEN NULL
            WHEN e.to_year IS NOT NULL AND e.to_year > 0 AND e.to_month IS NOT NULL AND e.to_month BETWEEN 1 AND 12
                THEN LPAD(e.to_year::text, 4, '0') || '-' || LPAD(e.to_month::text, 2, '0') || '-30'
            WHEN e.to_year IS NOT NULL AND e.to_year > 0
                THEN LPAD(e.to_year::text, 4, '0') || '-12-30'
            ELSE NULL
        END AS finished_at
    FROM {{ var('source_database') }}.people_experience e
    INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = e.root_id
    LEFT JOIN {{ ref('3_companies_loxo') }} c ON c.id = e.companyid
    WHERE 
        (NULLIF(TRIM(e.title), '') IS NOT NULL)
)
SELECT
    id,
    atlas_id,
    created_at,
    updated_at,
    source,
    agency_id,
    started_at,
    finished_at,
    company_name,
    title,
    description,
    person_id,
    atlas_person_id,
    company_id,
    atlas_company_id
FROM (
    SELECT
        id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || id::text") }} AS atlas_id,
        person_id,
        atlas_person_id,
        company_id,
        atlas_company_id,
        company_name,
        title,
        description,
        started_at,
        finished_at,
        '{{ var('agency_id') }}' AS agency_id,
        'migration' AS source,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        ROW_NUMBER() OVER (PARTITION BY id) AS rn
    FROM raw_experiences
) deduped
WHERE rn = 1
