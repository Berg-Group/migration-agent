{{ config(
    materialized='table',
    alias='person_salaries_loxo',
    tags=["loxo"]
) }}

SELECT
    p.id,
    {{ atlas_uuid("'salary' || p.id::text") }} AS atlas_id,
    p.id AS person_id,
    pl.atlas_id AS atlas_person_id,
    TO_CHAR(TRY_CAST(p.created AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(TRY_CAST(p.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    TO_CHAR(TRY_CAST(p.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS relevant_date,
    CASE
        WHEN NULLIF(BTRIM(p.comp_comp), '') IS NOT NULL AND BTRIM(p.comp_comp) ~ '^[0-9]+(\.[0-9]+)?$'
            THEN CAST(ROUND(TRY_CAST(p.comp_comp AS DECIMAL(15,2))) AS BIGINT)
        ELSE NULL
    END AS basic_salary,
    NULL AS expected_salary_max,
    NULL AS expected_salary_min,
    NULL AS expected_bonus_salary_max,
    NULL AS expected_bonus_salary_min,
    NULLIF(BTRIM(REGEXP_REPLACE(
        p.comp_notes,
        '<[^>]+>',
        ' ',
        1,
        'i'
    )), '') AS notes,
    'USD' AS currency,
    'gross' AS tax_method,
    'total' AS expected_salary_type,
    'migration' AS source,
    'actual' AS type,
    NULL AS created_by_id,
    '{{ var("master_id") }}' AS created_by_atlas_id,
    '{{ var("agency_id") }}' AS agency_id
FROM {{ var('source_database') }}.people p
INNER JOIN {{ ref('1_people_loxo') }} pl ON pl.id = p.id
WHERE (p.comp_comp IS NOT NULL AND TRIM(p.comp_comp) <> '')

