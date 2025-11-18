{{ config(
    materialized='table',
    alias='old_experiences_vincere'
) }}

WITH source_data AS (
    SELECT
        id,
        candidate_id,
        start_date,
        end_date,
        job_title,
        company,
        "current_employer"  -- Directly reference current_employer without aliasing
    FROM
        {{ var("source_database") }}."candidate_work_history"
    WHERE
        candidate_id IS NOT NULL
        AND "current_employer" IS NOT NULL
),

company_name_lookup AS (
    SELECT
        name AS company_name,
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM
        "{{ this.schema }}"."companies_vincere"
),

people_lookup AS (
    SELECT
        id AS person_id,
        atlas_id AS atlas_person_id
    FROM
        "{{ this.schema }}"."people_vincere"
)

SELECT
    source_data.id,
    LOWER(
        SUBSTRING(MD5(CAST(source_data.id AS TEXT)), 1, 8) || '-' ||
        SUBSTRING(MD5(CAST(source_data.id AS TEXT)), 9, 4) || '-' ||
        SUBSTRING(MD5(CAST(source_data.id AS TEXT)), 13, 4) || '-' ||
        SUBSTRING(MD5(CAST(source_data.id AS TEXT)), 17, 4) || '-' ||
        SUBSTRING(MD5(CAST(source_data.id AS TEXT)), 21, 12)
    ) AS atlas_id,  -- Generate UUID-style atlas_id
    source_data.candidate_id AS person_id,
    people_lookup.atlas_person_id,
    company_name_lookup.company_id,  -- Match based on current_employer
    company_name_lookup.atlas_company_id,
    '2025-03-05T00:00:00'::timestamp AS created_at,
    'migration' AS source,
    source_data.start_date AS started_at,
    source_data.end_date AS finished_at,
    source_data.job_title AS title,
    source_data.company AS description,
    source_data."current_employer",
    '{{ var('agency_id') }}'  AS agency_id,
    '{{ var('created_by_id') }}' AS created_by_id,
    'personal' AS type
FROM
    source_data
LEFT JOIN
    company_name_lookup ON source_data."current_employer" = company_name_lookup.company_name
LEFT JOIN
    people_lookup ON source_data.candidate_id = people_lookup.person_id
