-- File: models/vincere/experiences_cc_vin.sql

{{ config(
    materialized='table',
    alias='old_experiences_cc_vincere'
) }}

WITH source_data AS (
    SELECT
        s.id,
        s.company_id,
        s.job_title
    FROM
        {{ var("source_database") }}."contact" s
    WHERE
        s.company_id IS NOT NULL 
),

people_lookup AS (
    SELECT
        pv.id AS person_id,
        pv.atlas_id AS atlas_person_id
    FROM
        "{{ this.schema }}"."people_cc_vincere" pv
),

companies_lookup AS (
    SELECT
        cv.id AS company_id,
        cv.atlas_id AS atlas_company_id,
        cv.name AS company_name
    FROM
        "{{ this.schema }}"."companies_vincere" cv
)

SELECT
    LOWER(
        SUBSTRING(MD5(CAST(s.company_id AS TEXT) || CAST(s.id AS TEXT)), 1, 8) || '-' ||
        SUBSTRING(MD5(CAST(s.company_id AS TEXT) || CAST(s.id AS TEXT)), 9, 4) || '-' ||
        SUBSTRING(MD5(CAST(s.company_id AS TEXT) || CAST(s.id AS TEXT)), 13, 4) || '-' ||
        SUBSTRING(MD5(CAST(s.company_id AS TEXT) || CAST(s.id AS TEXT)), 17, 4) || '-' ||
        SUBSTRING(MD5(CAST(s.company_id AS TEXT) || CAST(s.id AS TEXT)), 21, 12)
    ) AS id,  -- Dashed UUID-style id
    '2025-03-05T00:00:00' AS created_at,
    '2025-03-05T00:00:00' AS updated_at,
    'migration' AS source,
    '2024-01-01'::date AS started_at,
    NULL AS finished_at,
    cl.company_name AS company_name,
    cl.atlas_company_id,
    s.company_id AS company_id,
    COALESCE(s.job_title, 'company contact') AS title,
    'cc' || s.id AS person_id,
    pl.atlas_person_id,
    'corporate' AS type,
    false AS secondary

FROM
    source_data s
LEFT JOIN
    people_lookup pl ON 'cc' || s.id = pl.person_id
LEFT JOIN
    companies_lookup cl ON s.company_id = cl.company_id
