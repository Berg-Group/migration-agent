{{ config(materialized='table', alias='experiences_rcrm') }}

{% set db = var('source_database') %}



{% set work_tables = [
    'work_history_1_data',
    'work_history_2_data',
    'work_history_3_data',
    'work_history_4_data',
    'work_history_5_data',
    'work_history_6_data'
] %}

WITH internal_persons AS (
    SELECT  id        AS person_id,
            atlas_id  AS atlas_person_id
    FROM {{ ref('3_people_rcrm') }}
),

internal_companies AS (
    SELECT  id        AS company_id,
            atlas_id  AS atlas_company_id,
            name      AS company_name
    FROM {{ ref('5_companies_rcrm') }}
),

raw_experiences AS (
    {% for tbl in work_tables %}
        SELECT  candidate_slug,
                work_start_date::DATE   AS started_at,
                work_end_date::DATE     AS finished_at,
                title,
                description,
                work_company_name       AS raw_company_name,
                'migration'             AS source
        FROM {{ db }}.{{ tbl }}
        WHERE work_start_date IS NOT NULL
          AND title           IS NOT NULL
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

deduped AS (
    SELECT DISTINCT
        candidate_slug,
        started_at,
        finished_at,
        title,
        description,
        raw_company_name,
        source
    FROM raw_experiences
),

final_enriched AS (
    SELECT
        id AS company_id,
        atlas_id AS atlas_company_id,
        name AS company_name
    FROM 
        {{ ref('5_companies_rcrm') }}
),

work_history_data AS (
    SELECT 
        candidate_slug,
        work_start_date,
        work_end_date,
        title,
        description,
        work_company_name
    FROM 
        {{ ref('10a_experiences_concat_rcrm') }}
    WHERE 
        work_start_date IS NOT NULL
        AND title IS NOT NULL
        AND work_company_name IS NOT NULL
        AND TRIM(work_company_name) <> ''
),

experiences_with_company AS (
    SELECT 
        {{ atlas_uuid('wh.candidate_slug || wh.title || wh.work_start_date') }} AS atlas_id,
        ip.person_id,
        ip.atlas_person_id,
        ic.company_id,
        ic.atlas_company_id,
        wh.work_start_date::DATE AS started_at,
        wh.work_end_date::DATE AS finished_at,
        wh.title AS title,
        wh.description AS description,
        'migration' AS source,
        COALESCE(ic.company_name, wh.work_company_name) AS company_name
    FROM 
        work_history_data wh
    LEFT JOIN 
        internal_persons AS ip
        ON ip.person_id = wh.candidate_slug
    LEFT JOIN 
        internal_companies AS ic
        ON lower(trim(ic.company_name)) = lower(trim(wh.work_company_name))
    WHERE
        COALESCE(ic.company_name, wh.work_company_name) IS NOT NULL
),

-- Deduplicate based on atlas_id
deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY atlas_id 
            ORDER BY 
                atlas_person_id NULLS LAST,
                atlas_company_id NULLS LAST,
                started_at
        ) AS rn
    FROM experiences_with_company
)

SELECT 
    atlas_id,
    person_id,
    atlas_person_id,
    company_id,
    atlas_company_id,
    started_at,
    finished_at,
    title,
    description,
    source,
    company_name
FROM 
    deduplicated
WHERE 
    rn = 1


