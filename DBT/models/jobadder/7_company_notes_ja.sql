{{ config(materialized='table', alias='company_notes_ja', tags=['jobadder']) }}

{% set db = var('source_database') %}

WITH internal_companies AS (
  SELECT 
    id AS company_id,
    atlas_id AS atlas_company_id
  FROM {{ ref('3_companies_ja') }}
),

notes_raw AS (
  SELECT
    n.noteid::VARCHAR AS id,
    {{ clean_html('n.text') }} AS text_clean,
    TO_CHAR(n.datecreated::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(n.dateupdated::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    n.createdbyuserid AS created_by_id,
    n.updatedbyuserid AS updated_by_id,
    'manual' AS type,
    companyid AS company_id
  FROM {{ db }}."note" n
  INNER JOIN {{ db }}."companynote" cn USING (noteid)
  WHERE deleted = FALSE
),

notes AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY company_id) AS rn
  FROM notes_raw
)

SELECT 
  n.id || '_'::varchar || rn::varchar AS id,
  {{ atlas_uuid('ic.atlas_company_id::varchar || n.id::varchar') }} AS atlas_id,
  n.text_clean AS text,
  n.created_at,
  n.updated_at,
  n.created_by_id,
  n.updated_by_id,
  COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
  COALESCE(u2.atlas_id, '{{ var("master_id") }}') AS updated_by_atlas_id,
  n.type,
  n.company_id,
  ic.atlas_company_id
FROM notes n
JOIN internal_companies ic USING (company_id)
LEFT JOIN {{ ref('users_ja') }} u  ON u.id  = n.created_by_id
LEFT JOIN {{ ref('users_ja') }} u2 ON u2.id = n.updated_by_id
WHERE TRIM(COALESCE(n.text_clean, '')) <> ''