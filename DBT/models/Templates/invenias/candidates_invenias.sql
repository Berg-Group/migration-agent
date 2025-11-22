{{ config(
    materialized = 'table',
    alias = 'candidates_invenias',
    tags = ["invenias"]
) }}

WITH internal_persons AS (
  SELECT DISTINCT id AS person_id, atlas_id AS atlas_person_id
  FROM {{ ref('people_invenias') }}
),

internal_projects AS (
  SELECT id AS project_id, atlas_id AS atlas_project_id, owner_id, atlas_owner_id
  FROM {{ ref('projects_invenias') }}
),

placements_src AS (
  SELECT
    rp."personid"       AS person_id,
    ra."assignmentid"   AS project_id,
    pl."itemid"         AS placement_id,
    pl."startdate"      AS startdate,
    pl."enddate"        AS enddate,
    COALESCE(pl."datemodified", pl."datecreated") AS ts
  FROM {{ var('source_database') }}."placements" pl
  LEFT JOIN {{ var('source_database') }}."relation_assignmenttoplacement" ra ON ra."placementid" = pl."itemid"
  LEFT JOIN {{ var('source_database') }}."relation_persontoplacement"    rp ON rp."placementid" = pl."itemid"
),

placements_one AS (
  SELECT
    person_id,
    project_id,
    placement_id,
    ROW_NUMBER() OVER (
      PARTITION BY person_id, project_id
      ORDER BY
        CASE WHEN enddate IS NULL AND startdate IS NOT NULL THEN 0 ELSE 1 END,
        ts DESC,
        placement_id DESC
    ) AS rn
  FROM placements_src
),

pl AS (
  SELECT person_id, project_id
  FROM placements_one
  WHERE rn = 1
),

statuses_base AS (
  SELECT
    rc."relationid"                           AS relation_id,
    rc."personid"                             AS person_id,
    rc."assignmentid"                         AS project_id,
    COALESCE(rc."datemodified", rc."datecreated") AS ts,
    TRIM(CASE WHEN SUBSTRING(l."name",1,1)='*' THEN SUBSTRING(l."name",2) ELSE l."name" END) AS name_norm
  FROM {{ var('source_database') }}."relation_candidatetoassignment" rc
  LEFT JOIN {{ var('source_database') }}."lookuplistentries" l ON l."itemid" = rc."recordstatus"
),

statuses_raw AS (
  SELECT
    relation_id,
    person_id,
    project_id,
    ts,
    name_norm,
    CASE
      WHEN name_norm IN (
        'Rejected',
        'Rejected by Client','Rejected by Consultant','Rejected by Candidate',
        'Rejected by Candidate - Location','Rejected by Candidate - Role','Rejected by Candidate - Timing',
        '09. Rejected after 1st Client interview','09. Rejected after 2nd Client interview',
        '09. Rejected after 3+ Client interview','09. Rejected by Candidate after Client Interview'
      ) THEN 1 ELSE 0
    END AS is_rejected
  FROM statuses_base
),

last_rc AS (
  SELECT
    relation_id,
    person_id,
    project_id,
    ts,
    name_norm,
    is_rejected,
    ROW_NUMBER() OVER (
      PARTITION BY person_id, project_id
      ORDER BY ts DESC, relation_id DESC
    ) AS rn
  FROM statuses_raw
),

curr AS (
  SELECT relation_id, person_id, project_id, ts, name_norm, is_rejected
  FROM last_rc
  WHERE rn = 1
),

prev_nonrej_ranked AS (
  SELECT
    person_id,
    project_id,
    name_norm,
    ts,
    ROW_NUMBER() OVER (
      PARTITION BY person_id, project_id
      ORDER BY ts DESC
    ) AS rn
  FROM statuses_raw
  WHERE is_rejected = 0
),

prev_nonrej AS (
  SELECT person_id, project_id, name_norm AS prev_name_norm, ts AS prev_ts
  FROM prev_nonrej_ranked
  WHERE rn = 1
)

SELECT
  c.relation_id                                              AS id,
  {{ atlas_uuid('c.relation_id') }}                           AS atlas_id,

  c.person_id                                                AS person_id,
  ip.atlas_person_id                                         AS atlas_person_id,

  c.project_id                                               AS project_id,
  ipp.atlas_project_id                                       AS atlas_project_id,

  'candidate'                                                AS class_type,

  CASE
    WHEN pl.person_id IS NOT NULL THEN 'Hired'
    WHEN c.is_rejected = 1 THEN
      CASE COALESCE(pn.prev_name_norm, '01. Target Candidate')
        WHEN '12. Placed'               THEN 'Hired'
        WHEN '11. Offer'                THEN 'Offer'
        WHEN '10. Finalist(s)'          THEN 'Client IV'
        WHEN '09. Client Interview'     THEN 'Client IV'
        WHEN '09. 3+ Client Interview'  THEN 'Client IV'
        WHEN '09. 2nd Client Interview' THEN 'Client IV'
        WHEN '09. 1st Client Interview' THEN 'Client IV'
        WHEN '08. Submitted to Client'  THEN 'Presented'
        WHEN '07. Shortlisted'          THEN 'Presented'
        WHEN '06. Consultant Interview' THEN 'Internal IV'
        WHEN '05. In Discussion'        THEN 'Added'
        WHEN 'Active'                   THEN 'Added'
        WHEN '14. Under-Development'    THEN 'Added'
        WHEN '03. Contacted'            THEN 'Added'
        WHEN '03. Left Message'         THEN 'Added'
        WHEN '02. Sent Email'           THEN 'Added'
        WHEN 'Benchmark Candidate'      THEN 'Added'
        WHEN '12. Identified Candidate' THEN 'Added'
        WHEN '01. Target Candidate'     THEN 'Added'
        WHEN 'Candidate Held'           THEN 'Internal IV'
        WHEN '04. Application Received' THEN 'Added'
        ELSE 'Added'
      END
    ELSE
      CASE c.name_norm
        WHEN '12. Placed'               THEN 'Hired'
        WHEN '11. Offer'                THEN 'Offer'
        WHEN '10. Finalist(s)'          THEN 'Client IV'
        WHEN '09. Client Interview'     THEN 'Client IV'
        WHEN '09. 3+ Client Interview'  THEN 'Client IV'
        WHEN '09. 2nd Client Interview' THEN 'Client IV'
        WHEN '09. 1st Client Interview' THEN 'Client IV'
        WHEN '08. Submitted to Client'  THEN 'Presented'
        WHEN '07. Shortlisted'          THEN 'Presented'
        WHEN '06. Consultant Interview' THEN 'Internal IV'
        WHEN '05. In Discussion'        THEN 'Added'
        WHEN 'Active'                   THEN 'Added'
        WHEN '14. Under-Development'    THEN 'Added'
        WHEN '03. Contacted'            THEN 'Added'
        WHEN '03. Left Message'         THEN 'Added'
        WHEN '02. Sent Email'           THEN 'Added'
        WHEN 'Benchmark Candidate'      THEN 'Added'
        WHEN '12. Identified Candidate' THEN 'Added'
        WHEN '01. Target Candidate'     THEN 'Added'
        WHEN 'Candidate Held'           THEN 'Internal IV'
        WHEN '04. Application Received' THEN 'Added'
        WHEN 'Rejected'                 THEN 'Added'
        ELSE 'Added'
      END
  END                                                        AS status,

  CASE
    WHEN c.is_rejected = 1 THEN
      CASE c.name_norm
        WHEN 'Rejected by Client' THEN 'by_client'
        WHEN '09. Rejected after 1st Client interview' THEN 'by_client'
        WHEN '09. Rejected after 2nd Client interview' THEN 'by_client'
        WHEN '09. Rejected after 3+ Client interview'  THEN 'by_client'
        WHEN 'Rejected by Candidate'                   THEN 'self'
        WHEN '09. Rejected by Candidate after Client Interview' THEN 'self'
        WHEN 'Rejected by Candidate - Location'       THEN 'self'
        WHEN 'Rejected by Candidate - Role'           THEN 'self'
        WHEN 'Rejected by Candidate - Timing'         THEN 'self'
        WHEN 'Rejected by Consultant'                 THEN 'by_us'
        WHEN 'Inactive'                               THEN 'by_us'
        WHEN 'Reference'                              THEN 'by_us'
        WHEN 'Source'                                 THEN 'by_us'
        WHEN 'Rejected'                               THEN 'by_us'
        ELSE NULL
      END
    ELSE NULL
  END                                                        AS rejection_type,

  CASE
    WHEN c.is_rejected = 1 THEN
      CASE c.name_norm
        WHEN 'Rejected by Candidate - Location' THEN 'location'
        WHEN 'Rejected by Candidate - Role'     THEN 'other'
        WHEN 'Rejected by Candidate - Timing'   THEN 'other'
        WHEN 'Rejected'                         THEN 'other'
        WHEN 'Rejected by Client'               THEN 'other'
        WHEN '09. Rejected after 1st Client interview' THEN 'other'
        WHEN '09. Rejected after 2nd Client interview' THEN 'other'
        WHEN '09. Rejected after 3+ Client interview'  THEN 'other'
        WHEN '09. Rejected by Candidate after Client Interview' THEN 'other'
        WHEN 'Rejected by Candidate'            THEN 'other'
        WHEN 'Rejected by Consultant'           THEN 'other'
        WHEN 'Inactive'                         THEN 'other'
        WHEN 'Reference'                        THEN 'other'
        WHEN 'Source'                           THEN 'other'
        ELSE 'other'
      END
    ELSE NULL
  END                                                        AS rejection_reason,

  CASE WHEN c.is_rejected = 1
       THEN TO_CHAR(c.ts::timestamp(0),'YYYY-MM-DD"T"00:00:00')
       ELSE NULL
  END                                                        AS rejected_at,

  CASE WHEN c.is_rejected = 1
       THEN COALESCE(ipp.atlas_owner_id,'{{ var("master_id") }}')
       ELSE NULL
  END                                                        AS atlas_rejected_by_id,

  ipp.owner_id                                               AS owner_id,
  COALESCE(ipp.atlas_owner_id, '{{ var("master_id") }}')     AS atlas_owner_id

FROM curr c
INNER JOIN internal_persons  ip  ON ip.person_id   = c.person_id
INNER JOIN internal_projects ipp ON ipp.project_id = c.project_id
LEFT  JOIN prev_nonrej pn        ON pn.person_id   = c.person_id
                                AND pn.project_id  = c.project_id
LEFT  JOIN pl                    ON pl.person_id   = c.person_id
                                AND pl.project_id  = c.project_id
