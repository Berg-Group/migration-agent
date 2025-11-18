{{ config(
    materialized='table',
    alias='projects_bba',
    tags=['bba']
) }}

WITH j AS (
  SELECT
      reference,
      job_title,
      closed_dt,
      close_reason,
      status,
      start_dt,
      end_dt
  FROM {{ var('source_database') }}."prop_job_gen"
),
h AS (
  SELECT
      entity_id,
      createddate,
      updateddate,
      deleteddate,
      status as entity_status
  FROM {{ var('source_database') }}."entity_table"
),
short_map AS (
  SELECT
      xsc.job::text AS project_id,
      COALESCE(c_by_ref.reference::text, c_by_id.reference::text) AS company_id
  FROM {{ var('source_database') }}."prop_x_short_cand" xsc
  LEFT JOIN {{ var('source_database') }}."prop_client_gen" c_by_ref
    ON c_by_ref.reference::text = xsc.client::text
  LEFT JOIN {{ var('source_database') }}."prop_client_gen" c_by_id
    ON c_by_id.client_id::text = xsc.client::text
  WHERE xsc.job IS NOT NULL
),
pick AS (
  SELECT
      project_id,
      company_id,
      ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY company_id) AS rn
  FROM short_map
  WHERE company_id IS NOT NULL
)

SELECT
    j.reference                                                     AS id,
    {{ atlas_uuid('j.reference') }}                                 AS atlas_id,
    TO_CHAR(h.createddate::timestamp, 'YYYY-MM-DD"T"00:00:00')      AS created_at,
    TO_CHAR(h.updateddate::timestamp, 'YYYY-MM-DD"T"00:00:00')      AS updated_at,
    j.job_title                                                     AS job_role,
    pk.company_id                                                   AS company_id,
    cb.atlas_id                                                     AS atlas_company_id,
    'false'                                                         AS public,
    'project'                                                       AS class_type,
    TO_CHAR(coalesce(j.closed_dt::timestamp, current_date), 'YYYY-MM-DD"T"00:00:00')        AS closed_at,
    NULL                                                            AS salary,
    NULL                                                            AS salary_currency,
    '1'                                                             AS owner_id,
    '{{ var("master_id") }}'                                        AS atlas_owner_id,
    NULL                                                            AS job_description_text,
    'closed'                                                        AS state,
    'cancelled'                                                     AS close_reason

FROM j
LEFT JOIN pick pk
  ON pk.project_id = j.reference::text
 AND pk.rn = 1
LEFT JOIN h
  ON h.entity_id = j.reference
INNER JOIN {{ ref('companies_bba') }} cb
  ON cb.id = pk.company_id
WHERE h.entity_status != 'D' OR h.entity_status IS NULL