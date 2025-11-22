{{ config(
    materialized='table',
    alias='experiences_ezekia'
) }}

WITH people_map AS (
    SELECT
        p.id             AS people_id,
        p.atlas_id       AS atlas_person_id
    FROM {{ ref('people_ezekia') }} p
),

companies_map AS (
    SELECT
        c.id          AS company_id,
        c.atlas_id    AS atlas_company_id,
        name AS company_name
    FROM {{ ref('companies_ezekia') }} c
),

source_data AS (
    SELECT
        pp.id,
        {{atlas_uuid('pp.id')}} AS atlas_id,
        TO_CHAR(pp.created_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(pp.updated_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        'migration' AS source,
        pp.person_id,
        pp.company_id,
        pp.company   AS company_name,
        pp.title,
        pp.start_date AS started_at,
        CASE
            WHEN pp.end_date = '9999-12-31' THEN NULL
            ELSE pp.end_date
        END AS finished_at,
        pp.summary AS descriptions,
        '{{ var("master_id") }}' AS created_by_id,
        '{{ var("master_id") }}' AS created_by_atlas_id
    FROM {{ var("source_database") }}.people_positions pp
),

joined AS (
    SELECT
        sd.id,
        sd.atlas_id,
        sd.created_at,
        sd.updated_at,
        sd.source,
        sd.person_id,
        pm.atlas_person_id,
        sd.company_id,
        CASE
            WHEN sd.company_id IS NOT NULL THEN cm.atlas_company_id
            ELSE NULL
        END AS atlas_company_id,
        COALESCE(sd.company_name, cm.company_name) AS company_name,
        sd.title,
        sd.started_at,
        sd.finished_at,
        sd.descriptions,
        sd.created_by_id,
        sd.created_by_atlas_id
    FROM source_data sd
    LEFT JOIN people_map pm
           ON sd.person_id = pm.people_id
    LEFT JOIN companies_map cm USING (company_id)
)

SELECT
    id,
    atlas_id,
    created_at,
    updated_at,
    source,
    person_id,
    atlas_person_id,
    company_name,
    company_id,
    atlas_company_id,
    title,
    started_at,
    finished_at,
    descriptions,
    created_by_id,
    created_by_atlas_id
FROM joined
WHERE started_at NOTNULL 
    AND NULLIF(TRIM(title), '') NOTNULL
    AND NULLIF(TRIM(company_name), '') NOTNULL