{{ config(
    materialized='table',
    alias='candidates_bba',
    tags=['bba']
) }}

WITH internal_people AS (
  SELECT
      id::text AS person_id,
      atlas_id AS atlas_person_id
  FROM {{ ref('people_bba') }}
),
internal_projects AS (
  SELECT
      id::text   AS project_id,
      atlas_id   AS atlas_project_id,
      company_id AS project_company_id
  FROM {{ ref('projects_bba') }}
),
clients AS (
  SELECT
      client_id::text AS client_id,
      reference::text AS company_id
  FROM {{ var('source_database') }}."prop_client_gen"
),
jobs_company AS (
  SELECT
      j.reference::text AS project_id,
      c.company_id      AS company_id
  FROM {{ var('source_database') }}."prop_job_gen" j
  JOIN clients c ON c.client_id = j.tbref::text
),
candidates_raw AS (
  SELECT candidate::text AS candidate_id, job::text AS project_id FROM {{ var('source_database') }}."prop_x_short_cand"
  UNION
  SELECT candidate::text, job::text FROM {{ var('source_database') }}."prop_x_int_cand"
  UNION
  SELECT candidate::text, job::text FROM {{ var('source_database') }}."prop_x_assig_cand"
),
person_map AS (
  SELECT
      cr.candidate_id,
      COALESCE(ppg_ref.reference::text, ppg_pid.reference::text, pcg.reference::text) AS person_id
  FROM candidates_raw cr
  LEFT JOIN {{ var('source_database') }}."prop_person_gen" ppg_ref ON ppg_ref.reference::text = cr.candidate_id
  LEFT JOIN {{ var('source_database') }}."prop_person_gen" ppg_pid ON ppg_pid.person_id::text = cr.candidate_id
  LEFT JOIN {{ var('source_database') }}."prop_cand_gen"  pcg      ON pcg.reference::text    = cr.candidate_id
),
shortlist AS (
  SELECT
      pm.person_id,
      xsc.job::text   AS project_id,
      'Added'         AS status,
      CURRENT_TIMESTAMP AS status_ts
  FROM {{ var('source_database') }}."prop_x_short_cand" xsc
  JOIN person_map pm ON pm.candidate_id = xsc.candidate::text
  WHERE xsc.job IS NOT NULL
),
interviews_cache AS (
  SELECT
      candidateid::text                                       AS person_id,
      clientid::text                                          AS company_id,
      COALESCE(enddatetime, ivdate, startdatetime)::timestamp AS event_ts
  FROM {{ var('source_database') }}."phx_previousinterviewsdatacache"
  UNION ALL
  SELECT
      candidateid::text,
      clientid::text,
      COALESCE(enddatetime, ivdate, startdatetime)::timestamp
  FROM {{ var('source_database') }}."phx_upcominginterviewsdatacache"
),
interview AS (
  SELECT
      pm.person_id,
      xic.job::text                                           AS project_id,
      'Internal IV'                                           AS status,
      COALESCE(MAX(ic.event_ts), CURRENT_TIMESTAMP)          AS status_ts
  FROM {{ var('source_database') }}."prop_x_int_cand" xic
  JOIN person_map pm ON pm.candidate_id = xic.candidate::text
  LEFT JOIN jobs_company jc ON jc.project_id = xic.job::text
  LEFT JOIN interviews_cache ic ON ic.person_id = pm.person_id AND ic.company_id = jc.company_id
  WHERE xic.job IS NOT NULL
  GROUP BY pm.person_id, xic.job
),
placed AS (
  SELECT
      pm.person_id,
      xac.job::text                                           AS project_id,
      'Hired'                                                 AS status,
      COALESCE(ag.filled_dt, ag.start_dt, ag.end_dt, CURRENT_TIMESTAMP)::timestamp AS status_ts
  FROM {{ var('source_database') }}."prop_x_assig_cand" xac
  JOIN person_map pm ON pm.candidate_id = xac.candidate::text
  LEFT JOIN {{ var('source_database') }}."prop_assig_gen" ag ON ag.assig_id::text = xac.assignment::text
  WHERE xac.job IS NOT NULL
),
unioned AS (
  SELECT * FROM placed
  UNION ALL
  SELECT * FROM interview
  UNION ALL
  SELECT * FROM shortlist
),
ranked AS (
  SELECT
      u.person_id,
      u.project_id,
      u.status,
      u.status_ts,
      ROW_NUMBER() OVER (
        PARTITION BY u.person_id, u.project_id
        ORDER BY
          CASE u.status WHEN 'Hired' THEN 1 WHEN 'Internal IV' THEN 2 WHEN 'Added' THEN 3 ELSE 9 END,
          u.status_ts DESC
      ) AS rn
  FROM unioned u
)

SELECT
    (r.person_id || '_' || r.project_id)                   AS id,
    {{ atlas_uuid("r.person_id || '_' || r.project_id") }} AS atlas_id,
    r.person_id                                            AS person_id,
    ip.atlas_person_id                                     AS atlas_person_id,
    r.project_id                                           AS project_id,
    pr.atlas_project_id                                    AS atlas_project_id,
    TO_CHAR(r.status_ts, 'YYYY-MM-DD"T"00:00:00')          AS created_at,
    TO_CHAR(r.status_ts, 'YYYY-MM-DD"T"00:00:00')          AS updated_at,
    r.status                                               AS status,
    '1'                                                    AS owner_id,
    '{{ var("master_id") }}'                               AS atlas_owner_id,
    NULL                                                   AS rejection_type,
    NULL                                                   AS rejection_reason,
    NULL                                                   AS rejected_at,
    NULL                                                   AS atlas_rejected_by_id
FROM ranked r
INNER JOIN internal_people   ip ON ip.person_id  = r.person_id
INNER JOIN internal_projects pr ON pr.project_id = r.project_id
WHERE r.rn = 1
  AND ip.atlas_person_id IS NOT NULL
  AND pr.atlas_project_id IS NOT NULL