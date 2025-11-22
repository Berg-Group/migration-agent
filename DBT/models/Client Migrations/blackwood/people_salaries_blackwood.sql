{{ config(
    materialized = 'table',
    alias        = 'person_salaries_blackwood',
    tags         = ['blackwood']
) }}

WITH internal_persons AS (
    SELECT id        AS person_id,
           atlas_id  AS atlas_person_id
    FROM {{ ref('people_blackwood') }}
),

pivot_lineitems AS (
    SELECT
        li.salaryid,
        MAX(CASE WHEN li.salarylinetypeid = 1
                 THEN li.salarylinetype_value::bigint END) AS basic_salary,
        MAX(CASE WHEN li.salarylinetypeid = 3
                 THEN li.salarylinetype_value::bigint END) AS bonus_salary,

        MAX(CASE WHEN li.salarylinetypeid = 2
                 THEN li.salarylinetype_value::bigint END) AS benefits_amount,

        MAX(CASE WHEN li.salarylinetypeid = 4
                 THEN li.salarylinetype_value::bigint END) AS company_car_amount,
        MIN(li.salarylinetype_ccy) AS currency
    FROM {{ var('source_database') }}."candidate_salary_lineitems" li
    GROUP BY li.salaryid
),

fin AS (
SELECT
    s.salaryid                                           AS id,
    lower(
        substring(md5(s.salaryid || s.cref),  1,  8) || '-' ||
        substring(md5(s.salaryid || s.cref),  9,  4) || '-' ||
        substring(md5(s.salaryid || s.cref), 13,  4) || '-' ||
        substring(md5(s.salaryid || s.cref), 17,  4) || '-' ||
        substring(md5(s.salaryid || s.cref), 21, 12)
    )                                                    AS atlas_id,
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')       AS created_at,
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')       AS updated_at,
    '{{ var("created_by_id") }}'                         AS created_by_atlas_id,
    s.cref                                               AS person_id,
    ip.atlas_person_id,
    NULL                                                 AS experience_id,
    p.basic_salary,
    p.bonus_salary,
    (p.basic_salary + p.bonus_salary)                    AS total_salary, 
    NULL AS expected_salary_min,
    NULL AS expected_salary_max,
    COALESCE(p.currency, 'GBP')                          AS currency,
    'United Kingdom'                                     AS country,
    s.salarynotes                                        AS notes,
    'gross'                                              AS tax_method,
    'total'                                              AS expected_salary_type,
    'migration'                                          AS source,
    to_char(coalesce(s.yearend, current_date), 'YYYY-MM-DD"T"00:00:00')          AS relevant_date
FROM {{ var('source_database') }}."candidate_salary" s
LEFT JOIN pivot_lineitems p USING (salaryid) 
INNER JOIN internal_persons  ip ON ip.person_id = s.cref)

SELECT * FROM fin 
WHERE basic_salary NOTNULL 
    OR bonus_salary NOTNULL 
    OR total_salary NOTNULL 
    OR expected_salary_min NOTNULL 
    OR expected_salary_max NOTNULL 
    OR notes NOTNULL 
