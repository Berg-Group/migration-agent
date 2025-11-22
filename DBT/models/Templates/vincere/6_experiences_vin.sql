{{ config(materialized = 'table', alias = 'experiences_vin') }}

{% set qs  = '"' ~ this.schema ~ '"' %}
{% set src = var('source_database') %}

-- ════════════════════════════════════════════════════════════════════
-- ❶  company ↔︎ atlas lookup (optional)
-- ════════════════════════════════════════════════════════════════════
WITH company_xref AS (
    SELECT
        id        AS company_id,
        atlas_id  AS atlas_company_id,
        name      AS company_name
    FROM {{ref('3_companies_vin')}}
),

-- ════════════════════════════════════════════════════════════════════
-- ❷  every *valid* person in people_vincere
-- ════════════════════════════════════════════════════════════════════
people_xref AS (
    SELECT id AS person_id,
           atlas_id AS atlas_person_id
    FROM   {{ref('1_people_vincere')}}
),

-- ════════════════════════════════════════════════════════════════════
-- ❸  candidate work‑history
--     – keep row *only* if candidate_id exists in people_vincere
-- ════════════════════════════════════════════════════════════════════
wh AS (
    SELECT
        pcwh.id,                                   -- original PK
        pcwh.candidate_id::varchar  AS person_id,
        TRY_CAST(pcwh.start_date AS date) AS started_at,
        TRY_CAST(pcwh.end_date   AS date) AS finished_at,
        pcwh.job_title                     AS title,
        pcwh.company                       AS description,
        pcwh.current_employer              AS company_name
    FROM {{ src }}."public_candidate_work_history" pcwh
    JOIN people_xref px                    -- ✦ guarantees the person exists ✦
      ON px.person_id = pcwh.candidate_id::varchar
    WHERE pcwh.current_employer IS NOT NULL
      AND TRIM(pcwh.current_employer) <> ''
),

-- ════════════════════════════════════════════════════════════════════
-- ❹  join with company and person references
-- ════════════════════════════════════════════════════════════════════
joined_data AS (
    SELECT
        wh.id,
        {{ atlas_uuid("'vincere_experience_' || '{{ var(\"clientName\") }}' || wh.id::text") }} AS atlas_id,
        wh.person_id,
        px.atlas_person_id,
        cx.atlas_company_id,
        wh.title,
        wh.started_at,
        wh.finished_at,
        wh.company_name,                              -- ← now always current_employer
        '{{ var("agency_id") }}'      AS agency_id,
        '{{ var("created_by_id") }}'  AS created_by_id,
        'migration'                   AS source,
        TO_CHAR(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SS') AS created_at
    FROM   wh
    JOIN   people_xref  px  USING (person_id)       -- inner join already enforced
    LEFT   JOIN company_xref cx 
              ON LOWER(cx.company_name) = LOWER(wh.company_name)
    WHERE  wh.started_at IS NOT NULL AND TRIM(wh.title) <> ''
),

-- ════════════════════════════════════════════════════════════════════
-- ❺  deduplicate - ensure only one row per original ID
-- ════════════════════════════════════════════════════════════════════
deduped_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id 
            ORDER BY 
                -- Prioritize rows that have a company match
                CASE WHEN atlas_company_id IS NOT NULL THEN 1 ELSE 2 END
        ) AS rn
    FROM joined_data
)

-- ════════════════════════════════════════════════════════════════════
-- ❻  final output - only unique IDs
-- ════════════════════════════════════════════════════════════════════
SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    atlas_company_id,
    title,
    started_at,
    finished_at,
    company_name,
    agency_id,
    created_by_id,
    source,
    created_at
FROM deduped_data
WHERE rn = 1
