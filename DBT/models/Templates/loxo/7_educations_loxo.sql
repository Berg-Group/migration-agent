{{ config(
    materialized='table',
    alias='educations_loxo',
    tags=["loxo"]
) }}

WITH raw_educations AS (
    SELECT
        e.row_uid AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || e.row_uid") }} AS atlas_id,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        NULLIF(TRIM(e.school), '') AS name,
        CASE 
            WHEN NULLIF(TRIM(e.degree), '') IS NULL THEN NULL
            WHEN (LENGTH(NULLIF(TRIM(e.degree), '')) - LENGTH(REPLACE(NULLIF(TRIM(e.degree), ''), ',', ''))) >= 1
                THEN BTRIM(REGEXP_REPLACE(NULLIF(TRIM(e.degree), ''), ',[^,]*$', ''))
            ELSE NULLIF(TRIM(e.degree), '')
        END AS degree,
        CASE 
            WHEN NULLIF(TRIM(e.degree), '') IS NULL THEN NULL
            WHEN (LENGTH(NULLIF(TRIM(e.degree), '')) - LENGTH(REPLACE(NULLIF(TRIM(e.degree), ''), ',', ''))) >= 1
                THEN BTRIM(SPLIT_PART(
                    NULLIF(TRIM(e.degree), ''),
                    ',',
                    (LENGTH(NULLIF(TRIM(e.degree), '')) - LENGTH(REPLACE(NULLIF(TRIM(e.degree), ''), ',', ''))) + 1
                ))
            ELSE NULL
        END AS field_of_study,
        REGEXP_REPLACE(
            COALESCE(e."desc"),
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS description,
        NULL AS started_at,
        CASE
            WHEN e.year IS NOT NULL AND e.year > 0 AND e.month IS NOT NULL AND e.month BETWEEN 1 AND 12
                THEN LPAD(e.year::text, 4, '0') || '-' || LPAD(e.month::text, 2, '0') || '-30'
            WHEN e.year IS NOT NULL AND e.year > 0
                THEN LPAD(e.year::text, 4, '0') || '-12-30'
            ELSE NULL
        END AS finished_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        'migration' AS source,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.people_education e
    INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = e.root_id
    WHERE 
        (NULLIF(TRIM(e.school), '') IS NOT NULL)
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
    name,
    degree,
    field_of_study,
    description,
    person_id,
    atlas_person_id
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id) AS rn
    FROM raw_educations
) deduped
WHERE rn = 1
