{{ config(
    materialized='table',
    alias='meetings_loxo',
    tags=["loxo"]
) }}

WITH base AS (
    SELECT
        a.id,
        {{ atlas_uuid("a.id::text || a.person::text") }} AS atlas_id,
        a.person AS person_id,
        NULL AS created_by_id,
        '{{ var("master_id") }}' AS created_by_atlas_id,
        REGEXP_REPLACE(
            COALESCE(a.notes),
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS notes,
        'completed' AS status,
        'migrated meeting' AS name,
        TO_CHAR(TRY_CAST(a.created AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(TRY_CAST(a.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
    FROM {{ var('source_database') }}.activities a
    WHERE a.notes IS NOT NULL
      AND TRIM(a.notes) <> ''
      AND a."type" IN {{ get_agency_filter('meetings') }}
)
SELECT
    b.id,
    b.atlas_id,
    b.person_id,
    p.atlas_id AS atlas_person_id,
    b.created_by_id,
    b.created_by_atlas_id,
    b.notes,
    b.status,
    b.name,
    b.created_at,
    b.updated_at
FROM base b
INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = b.person_id
