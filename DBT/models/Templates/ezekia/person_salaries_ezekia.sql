{{ config(
    materialized = 'table',
    alias        = 'person_salaries_ezekia',
    tags         = ['ezekia']
) }}

WITH internal_persons AS (
    SELECT id::text AS person_id, atlas_id AS atlas_person_id
    FROM {{ ref('people_ezekia') }}
),
aspirations AS (
    SELECT
        pa.id::text          AS aspiration_id,
        pa.person_id::text   AS person_id,
        pa.summary           AS notes,
        TO_CHAR(pa.created_at::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS')                               AS created_at_text,
        TO_CHAR(COALESCE(pa.updated_at, pa.created_at)::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS')      AS updated_at_text,
        COALESCE(pa.updated_at, pa.created_at)::date                                               AS relevant_dt,
        pa.base              AS base_from_people,
        pa.bonus             AS bonus_from_people,
        pa.equity            AS equity_from_people
    FROM {{ var('source_database') }}.people_aspirations pa
),
permanent AS (
    SELECT
        ap.id::text            AS id,
        ap.aspiration_id::text AS aspiration_id,
        ap.gbp_base            AS gbp_base,
        ap.base                AS base_from_perm,
        ap.currency            AS currency_from_perm,
        ap.bonus               AS bonus_from_perm,
        ap.equity              AS equity_from_perm,
        ap.created_at          AS created_at_src,
        ap.updated_at          AS updated_at_src
    FROM {{ var('source_database') }}.aspirations_permanent ap
),
joined AS (
    SELECT
        p.id AS id,
        {{ atlas_uuid("p.id || a.person_id") }} AS atlas_id,
        a.created_at_text AS created_at,
        a.updated_at_text AS updated_at,
        '{{ var("agency_id") }}' AS agency_id,
        '1' AS created_by_id,
        '{{ var("master_id") }}' AS created_by_atlas_id,
        a.person_id AS person_id,
        ip.atlas_person_id,
        NULL AS experience_id,
        COALESCE(a.base_from_people, p.base_from_perm)::bigint AS basic_salary,
        CASE
            WHEN COALESCE(a.base_from_people, p.base_from_perm) IS NOT NULL
             AND COALESCE(p.bonus_from_perm, a.bonus_from_people) IS NOT NULL
             AND COALESCE(p.bonus_from_perm, a.bonus_from_people) BETWEEN 0 AND 100
                THEN (
                    COALESCE(a.base_from_people, p.base_from_perm)
                    * COALESCE(p.bonus_from_perm, a.bonus_from_people) / 100.0
                )::bigint
            WHEN COALESCE(p.bonus_from_perm, a.bonus_from_people) IS NOT NULL
                THEN COALESCE(p.bonus_from_perm, a.bonus_from_people)::bigint
            ELSE NULL
        END AS bonus_salary,
        CASE
            WHEN COALESCE(a.base_from_people, p.base_from_perm) IS NOT NULL
              OR COALESCE(p.bonus_from_perm, a.bonus_from_people) IS NOT NULL
                THEN (
                    COALESCE(a.base_from_people, p.base_from_perm, 0)
                    + COALESCE(
                        CASE
                            WHEN COALESCE(a.base_from_people, p.base_from_perm) IS NOT NULL
                             AND COALESCE(p.bonus_from_perm, a.bonus_from_people) IS NOT NULL
                             AND COALESCE(p.bonus_from_perm, a.bonus_from_people) BETWEEN 0 AND 100
                                THEN COALESCE(a.base_from_people, p.base_from_perm)
                                     * COALESCE(p.bonus_from_perm, a.bonus_from_people) / 100.0
                            ELSE COALESCE(p.bonus_from_perm, a.bonus_from_people)
                        END, 0
                    )
                )::bigint
            ELSE NULL
        END AS total_salary,
        NULL AS expected_salary_min,
        NULL AS expected_salary_max,
        p.currency_from_perm AS currency,
        NULL AS country,
        a.notes AS notes,
        'gross' AS tax_method,
        'total' AS expected_salary_type,
        'migration' AS source,
        TO_CHAR(a.relevant_dt, 'YYYY-MM-DD"T"00:00:00') AS relevant_date
    FROM permanent p
    JOIN aspirations a ON a.aspiration_id = p.aspiration_id
    JOIN internal_persons ip ON ip.person_id = a.person_id
)

SELECT *
FROM joined
WHERE basic_salary IS NOT NULL
   OR bonus_salary IS NOT NULL
   OR total_salary IS NOT NULL
   OR notes IS NOT NULL
