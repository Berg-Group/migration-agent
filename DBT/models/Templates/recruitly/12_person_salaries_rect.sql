{{ config(
    materialized='table',
    alias='person_salaries_rect',
    tags=['recruitly']
) }}

WITH actual_salaries AS (
    SELECT
        'Salary::' || c.candidate_id AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || '::salary::' || c.candidate_id::text") }} AS atlas_id,
        c.candidate_id AS person_id,
        p.atlas_id AS atlas_person_id,
        {{ string_to_timestamp('c.createdon') }} AS created_at,
        {{ string_to_timestamp('c.modifiedon') }} AS updated_at,
        {{ string_to_timestamp('c.modifiedon') }} AS relevant_date,
        CASE
            WHEN NULLIF(BTRIM(c.current_salary), '') IS NOT NULL AND BTRIM(c.current_salary) ~ '^[0-9]+(\.[0-9]+)?$'
                THEN CAST(ROUND(TRY_CAST(c.current_salary AS DECIMAL(15,2))) AS BIGINT)
            ELSE NULL
        END AS basic_salary,
        NULL AS expected_salary_max,
        NULL AS expected_salary_min,
        NULL AS expected_bonus_salary_max,
        NULL AS expected_bonus_salary_min,
        NULL AS notes,
        'GBP' AS currency,
        'gross' AS tax_method,
        'total' AS expected_salary_type,
        'migration' AS source,
        'actual' AS type,
        NULL AS created_by_id,
        '{{ var("master_id") }}' AS created_by_atlas_id,
        '{{ var("agency_id") }}' AS agency_id
    FROM {{ var('source_database') }}.candidates c
    INNER JOIN {{ ref('2_people_rect') }} p ON p.id = c.candidate_id
    WHERE
        (NULLIF(BTRIM(c.current_salary), '') IS NOT NULL AND BTRIM(c.current_salary) <> '0.0')
),
expected_salaries AS (
    SELECT
        'Salary_expected::' || c.candidate_id AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || '::salary_expected::' || c.candidate_id::text") }} AS atlas_id,
        c.candidate_id AS person_id,
        p.atlas_id AS atlas_person_id,
        {{ string_to_timestamp('c.createdon') }} AS created_at,
        {{ string_to_timestamp('c.modifiedon') }} AS updated_at,
        {{ string_to_timestamp('c.modifiedon') }} AS relevant_date,
        NULL AS basic_salary,
        CASE
            WHEN REGEXP_SUBSTR(c.expected_pay, '(\\d[\\d,]*\\.?\\d*)', 1, 2) IS NOT NULL THEN CAST(ROUND(TRY_CAST(REPLACE(REGEXP_SUBSTR(c.expected_pay, '(\\d[\\d,]*\\.?\\d*)', 1, 2), ',', '') AS DECIMAL(15,2))) AS BIGINT)
            WHEN REGEXP_SUBSTR(c.expected_pay, '(\\d[\\d,]*\\.?\\d*)', 1, 1) IS NOT NULL THEN CAST(ROUND(TRY_CAST(REPLACE(REGEXP_SUBSTR(c.expected_pay, '(\\d[\\d,]*\\.?\\d*)', 1, 1), ',', '') AS DECIMAL(15,2))) AS BIGINT)
            ELSE NULL
        END AS expected_salary_max,
        CASE
            WHEN REGEXP_SUBSTR(c.expected_pay, '(\\d[\\d,]*\\.?\\d*)', 1, 1) IS NOT NULL THEN CAST(ROUND(TRY_CAST(REPLACE(REGEXP_SUBSTR(c.expected_pay, '(\\d[\\d,]*\\.?\\d*)', 1, 1), ',', '') AS DECIMAL(15,2))) AS BIGINT)
            ELSE NULL
        END AS expected_salary_min,
        NULL AS expected_bonus_salary_max,
        NULL AS expected_bonus_salary_min,
        NULL AS notes,
        'GBP' AS currency,
        'gross' AS tax_method,
        'total' AS expected_salary_type,
        'migration' AS source,
        'expected' AS type,
        NULL AS created_by_id,
        '{{ var("master_id") }}' AS created_by_atlas_id,
        '{{ var("agency_id") }}' AS agency_id
    FROM {{ var('source_database') }}.candidates c
    INNER JOIN {{ ref('2_people_rect') }} p ON p.id = c.candidate_id
    WHERE
        NULLIF(BTRIM(c.expected_pay), '') IS NOT NULL
        AND UPPER(BTRIM(c.expected_pay)) <> 'NA'
)
SELECT * FROM actual_salaries
UNION ALL
SELECT * FROM expected_salaries

