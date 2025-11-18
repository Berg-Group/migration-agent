{{ config(
    materialized = 'table',
    alias        = 'person_salaries_ft',
    tags         = ['forgetalent']
) }}

WITH internal_persons AS (
    SELECT id        AS person_id,
           atlas_id  AS atlas_person_id
    FROM {{ ref('1_people_ft') }}
)

SELECT
    {{atlas_uuid('userid || salary')}} AS atlas_id, 
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')       AS created_at,
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')       AS updated_at,
    '{{ var("agency_id") }}'                             AS agency_id,
    '{{ var("created_by_id") }}'                         AS created_by_atlas_id,
    userid                                               AS person_id,
    ip.atlas_person_id,
    NULL                                                 AS experience_id,
    salarylow AS basic_salary,
    salary                                               AS total_salary, 
    salarylow AS expected_salary_min,
    salary AS expected_salary_max,
    'GBP'                                                AS currency,
    'United Kingdom'                                     AS country,
    'gross'                                              AS tax_method,
    'total'                                              AS expected_salary_type,
    'migration'                                          AS source,
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')          AS relevant_date
FROM {{ var('source_database') }}."bh_usercontact" s
INNER JOIN internal_persons  ip ON ip.person_id = s.userid
WHERE salarylow >0 OR salary >0

UNION ALL 

SELECT
    {{atlas_uuid('userid || salary')}} AS atlas_id, 
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')       AS created_at,
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')       AS updated_at,
    '{{ var("agency_id") }}'                             AS agency_id,
    '{{ var("created_by_id") }}'                         AS created_by_atlas_id,
    userid                                               AS person_id,
    ip.atlas_person_id,
    NULL                                                 AS experience_id,
    dayratelow AS basic_salary,
    dayrate                                               AS total_salary, 
    dayratelow AS expected_salary_min,
    dayrate AS expected_salary_max,
    'GBP'                                                AS currency,
    'United Kingdom'                                     AS country,
    'gross'                                              AS tax_method,
    'day_rate'                                              AS expected_salary_type,
    'migration'                                          AS source,
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')          AS relevant_date
FROM {{ var('source_database') }}."bh_usercontact" s
INNER JOIN internal_persons  ip ON ip.person_id = s.userid
WHERE dayratelow >0 OR dayrate >0

UNION ALL 

SELECT
    {{atlas_uuid('userid || salary')}} AS atlas_id, 
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')       AS created_at,
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')       AS updated_at,
    '{{ var("agency_id") }}'                             AS agency_id,
    '{{ var("created_by_id") }}'                         AS created_by_atlas_id,
    userid                                               AS person_id,
    ip.atlas_person_id,
    NULL                                                 AS experience_id,
    hourlyratelow AS basic_salary,
    hourlyrate                                               AS total_salary, 
    hourlyratelow AS expected_salary_min,
    hourlyrate AS expected_salary_max,
    'GBP'                                                AS currency,
    'United Kingdom'                                     AS country,
    'gross'                                              AS tax_method,
    'hourly_rate'                                              AS expected_salary_type,
    'migration'                                          AS source,
    to_char(current_date, 'YYYY-MM-DD"T"00:00:00')          AS relevant_date
FROM {{ var('source_database') }}."bh_usercontact" s
INNER JOIN internal_persons  ip ON ip.person_id = s.userid
WHERE hourlyratelow >0 OR hourlyrate >0
