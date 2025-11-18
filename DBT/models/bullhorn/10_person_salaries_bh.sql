{{ config(
    materialized = 'table',
    alias        = 'person_salaries_bh',
    tags         = ['bullhorn']
) }}

WITH internal_persons AS (
    SELECT id        AS person_id,
           atlas_id  AS atlas_person_id
    FROM {{ ref('1_people_bh') }}
)

SELECT
    {{ atlas_uuid("s.userid || COALESCE(s.salary::text, '') || COALESCE(s.dayrate::text, '') || COALESCE(s.hourlyrate::text, '')") }} AS atlas_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    '{{ var("agency_id") }}' AS agency_id,
    NULL AS created_by_id,
    '{{ var("master_id") }}' AS created_by_atlas_id,
    s.userid AS person_id,
    ip.atlas_person_id,
    'USD' AS currency,
    ROUND(TRIM(s.salary)::numeric) AS basic_salary,
    NULL AS bonus_salary,
    ROUND(TRIM(s.salary)::numeric) AS total_salary,
    NULL AS expected_salary_min,
    NULL AS expected_salary_max,
    NULL AS expected_bonus_salary_min,
    NULL AS expected_bonus_salary_max,
    'gross' AS tax_method,
    'actual' AS type,
    'total' AS expected_salary_type,
    'migration' AS source,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS relevant_date
FROM {{ var('source_database') }}."bh_usercontact" s
INNER JOIN internal_persons ip ON ip.person_id = s.userid
WHERE s.salary IS NOT NULL AND s.salary != 0