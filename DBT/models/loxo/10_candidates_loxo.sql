{{ config(
    materialized='table',
    alias='candidates_loxo',
    tags=["loxo"]
) }}

WITH activity_candidates AS (
  SELECT
    a.id,
    {{ atlas_uuid("'" ~ var('clientName') ~ "' || a.person::text || '-' || a.job::text") }} AS atlas_id,
    a.job AS project_id,
    pl.atlas_id AS atlas_project_id,
    a.person AS person_id,
    p.atlas_id AS atlas_person_id,
    NULL AS owner_id,
    '{{ var("master_id") }}' AS atlas_owner_id,
    'Candidate' AS class_type,
    TO_CHAR(TRY_CAST(a.created AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(TRY_CAST(a.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    CASE 
      WHEN a."type" IN (
        'Sourced & Messaged on LinkedIn',
        'Spoken to / In-progress',
        'Sourced & Messaged on LinkedIn / Unsourced',
        'Marked as Yes',
        'Sourced',
        'Scorecard Submitted',
        'Moved to Applied',
        'Moved to Rejected'
      ) THEN 'Added'

      WHEN a."type" IN (
        'Interview',
        'Moved to Interview',
        'Submitted'
      ) THEN 'Client IV'

      WHEN a."type" IN (
        'Moved to To submit',
        'Moved to Rejected after Submitted / Interviewed',
        'Rejected',
        'Moved to Submitted'
      ) THEN 'Presented'

      WHEN a."type" IN (
        'Offer',
        'Moved to Offer'
      ) THEN 'Offer'

      WHEN a."type" IN (
        'Moved to Last follow up',
        'Moved to Still chasing',
        'Moved to Spoken to / In-progress',
        'Moved to Sourced',
        'Rejected / By Candidate'
      ) THEN 'Internal IV'

      WHEN a."type" IN (
        'Moved to Hired',
        'Hired'
      ) THEN 'Hired'

      ELSE 'Added'
    END AS status,
    CASE
      WHEN a."type" = 'Rejected / By Candidate' THEN 'self'
      WHEN a."type" = 'Moved to Rejected after Submitted / Interviewed' THEN 'by_client'
      WHEN a."type" IN ('Rejected', 'Moved to Rejected') THEN 'by_us'
      ELSE NULL
    END AS rejection_type,
    CASE
      WHEN a."type" IN ('Rejected', 'Rejected / By Candidate', 'Moved to Rejected after Submitted / Interviewed', 'Moved to Rejected')
        THEN 'other'
      ELSE NULL
    END AS rejection_reason,
    CASE
      WHEN a."type" IN ('Rejected', 'Rejected / By Candidate', 'Moved to Rejected after Submitted / Interviewed', 'Moved to Rejected')
        THEN TO_CHAR(TRY_CAST(a.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS')
      ELSE NULL
    END AS rejected_at,
    CASE
      WHEN a."type" IN ('Rejected', 'Rejected / By Candidate', 'Moved to Rejected after Submitted / Interviewed', 'Moved to Rejected')
        THEN '{{ var("master_id") }}'
      ELSE NULL
    END AS atlas_rejected_by_id,
    CASE
      WHEN a."type" IN ('Hired', 'Moved to Hired')
        THEN TO_CHAR(TRY_CAST(a.updated AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS')
      ELSE NULL
    END AS hired_at
  FROM {{ var('source_database') }}.activities a
  INNER JOIN {{ ref('8_projects_loxo') }} pl ON pl.id = a.job
  INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = a.person
  WHERE a."type" IN {{ get_agency_filter('candidates') }}
)
SELECT
  id,
  atlas_id,
  created_at,
  updated_at,
  project_id,
  atlas_project_id,
  person_id,
  atlas_person_id,
  owner_id,
  atlas_owner_id,
  class_type,
  status,
  rejected_at,
  rejection_type,
  rejection_reason,
  atlas_rejected_by_id,
  hired_at
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY atlas_project_id, atlas_person_id
      ORDER BY updated_at DESC, created_at DESC, id DESC
    ) AS rn
  FROM activity_candidates
) deduplicated
WHERE rn = 1