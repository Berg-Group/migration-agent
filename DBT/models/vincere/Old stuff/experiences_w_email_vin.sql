{{ config(materialized = 'table', alias = 'experiences__w_email_vin') }}

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
    FROM {{ qs }}."companies_vincere"
),

-- ════════════════════════════════════════════════════════════════════
-- ❷  every *valid* person in people_vincere
-- ════════════════════════════════════════════════════════════════════
people_xref AS (
    SELECT id AS person_id,
           atlas_id AS atlas_person_id
    FROM   {{ qs }}."people_vincere"
),

-- ════════════════════════════════════════════════════════════════════
-- ❸  candidate work-history + e-mail
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
        pcwh.current_employer              AS company_name,
        LOWER(TRIM(c.email))               AS email         -- ← NEW COLUMN
    FROM {{ src }}."public_candidate_work_history" pcwh
    JOIN people_xref px                    -- ✦ guarantees the person exists ✦
      ON px.person_id = pcwh.candidate_id::varchar
    LEFT JOIN {{ src }}."public_candidate" c             -- ← add e-mail
      ON c.id::varchar = pcwh.candidate_id::varchar
)

-- ════════════════════════════════════════════════════════════════════
-- ❹  final rows
-- ════════════════════════════════════════════════════════════════════
SELECT
    wh.id,
    LOWER(
        SUBSTRING(md5(wh.id::text),  1, 8) || '-' ||
        SUBSTRING(md5(wh.id::text),  9, 4) || '-' ||
        SUBSTRING(md5(wh.id::text), 13, 4) || '-' ||
        SUBSTRING(md5(wh.id::text), 17, 4) || '-' ||
        SUBSTRING(md5(wh.id::text), 21,12)
    )                             AS atlas_id,
    wh.person_id,
    px.atlas_person_id,
    cx.atlas_company_id,
    wh.title,
    wh.started_at,
    wh.finished_at,
    wh.company_name,
    wh.email,                                       -- ← NEW COLUMN
    '{{ var("agency_id") }}'      AS agency_id,
    '{{ var("created_by_id") }}'  AS created_by_id,
    'migration'                   AS source,
    TO_CHAR(current_timestamp,'YYYY-MM-DD\"T\"HH24:MI:SS') AS created_at
FROM   wh
JOIN   people_xref  px  USING (person_id)          -- inner join already enforced
LEFT   JOIN company_xref cx 
          ON LOWER(cx.company_name) = LOWER(wh.company_name)
WHERE  wh.started_at IS NOT NULL
