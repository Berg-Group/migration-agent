{{ config(
    materialized='table',
    alias='person_notes_loxo',
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
        ) AS text,
        'manual' AS type,
        TO_CHAR(TRY_CAST(a.created AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(TRY_CAST(a.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
    FROM {{ var('source_database') }}.activities a
    WHERE a.notes IS NOT NULL
      AND TRIM(a.notes) <> ''
      AND a."type" IN {{ get_agency_filter('notes') }}
),
form_notes AS (
    WITH answers AS (
        SELECT
            f.id AS form_id,
            f.personid AS person_id,
            f.title AS form_title,
            TRIM(fa.question) AS question,
            TRIM(fa.text) AS answer
        FROM {{ var('source_database') }}.forms f
        INNER JOIN {{ var('source_database') }}.forms_answers fa ON fa.root_id = f.id
        WHERE fa.text IS NOT NULL
          AND TRIM(fa.text) <> ''
    ),
    aggregated AS (
        SELECT
            person_id,
            form_id,
            form_title,
            LISTAGG('- ' || question || ': ' || answer, '\n') WITHIN GROUP (ORDER BY question) AS answers_text
        FROM answers
        GROUP BY person_id, form_id, form_title
    )
    SELECT
        'form_' || form_id AS id,
        {{ atlas_uuid("form_id::text || person_id::text") }} AS atlas_id,
        person_id,
        NULL AS created_by_id,
        '{{ var("master_id") }}' AS created_by_atlas_id,
        (form_title || '\n\n' || answers_text) AS text,
        'manual' AS type,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD"T"00:00:00') AS updated_at
    FROM aggregated
),
combined AS (
    SELECT * FROM form_notes
    UNION ALL
    SELECT * FROM base
)
SELECT
    b.id,
    b.atlas_id,
    b.person_id,
    p.atlas_id AS atlas_person_id,
    b.created_by_id,
    b.created_by_atlas_id,
    b.text,
    b.created_at,
    b.updated_at,
    b.type
FROM combined b
INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = b.person_id
