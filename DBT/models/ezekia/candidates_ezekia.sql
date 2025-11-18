{{ config(
    materialized='table',
    alias='candidates_ezekia'
) }}

WITH briefs_candidates_data AS (
    SELECT
        bc.id,
        {{ atlas_uuid('bc.id') }} AS atlas_id,
        bc.brief_id AS project_id,
        bc.person_id,
        COALESCE(NULLIF(TRIM(bc.added_by), ''), '1') AS owner_name_or_id,
        TO_CHAR(bc.created_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(bc.updated_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at
    FROM {{ var("source_database") }}.briefs_candidates bc
),

owner_resolved AS (
    SELECT
        bcd.id,
        bcd.atlas_id,
        bcd.project_id,
        bcd.person_id,
        COALESCE(u_id.id, u_name.id, '1') AS owner_id,
        COALESCE(u_id.atlas_id, u_name.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
        bcd.created_at,
        bcd.updated_at
    FROM briefs_candidates_data bcd
    LEFT JOIN {{ ref('users_ezekia') }} u_id
           ON u_id.id = bcd.owner_name_or_id
    LEFT JOIN {{ ref('users_ezekia') }} u_name
           ON u_name.name = bcd.owner_name_or_id
),

status_history AS (
    SELECT
        s.statusable_id AS person_id,
        s.constraint_id AS project_id,
        st.tag_text     AS status_text,
        COALESCE(s.updated_at, s.created_at)::timestamp AS status_ts,
        s.id AS statusable_pk
    FROM {{ var("source_database") }}.statusables s
    JOIN {{ var("source_database") }}.search_firms_status_tags st
      ON st.id = s.status_tag_id
     AND st.tag_type = 'candidate'
    WHERE s.statusable_type = 'person'
      AND s.constraint_type = 'assignment'
),

status_ordered AS (
    SELECT
        sh.*,
        ROW_NUMBER() OVER (
            PARTITION BY sh.person_id, sh.project_id
            ORDER BY sh.status_ts DESC, sh.statusable_pk DESC
        ) AS rn
    FROM status_history sh
),

latest_event AS (
    SELECT
        so.person_id,
        so.project_id,
        so.status_text,
        so.status_ts,
        so.statusable_pk
    FROM status_ordered so
    WHERE so.rn = 1
),

prev_ok AS (
    SELECT
        sh.person_id,
        sh.project_id,
        sh.status_text,
        sh.status_ts,
        sh.statusable_pk,
        ROW_NUMBER() OVER (
            PARTITION BY sh.person_id, sh.project_id
            ORDER BY sh.status_ts DESC, sh.statusable_pk DESC
        ) AS r2
    FROM status_history sh
    JOIN latest_event le
      ON le.person_id  = sh.person_id
     AND le.project_id = sh.project_id
    WHERE (sh.status_ts, sh.statusable_pk) < (le.status_ts, le.statusable_pk)
      AND sh.status_text NOT IN (
          'Do not approach - Client in contact / has contacted',
          'Client Rejected',
          'Not Interested',
          'Not Responded'
      )
),

prev_non_reject AS (
    SELECT
        person_id,
        project_id,
        status_text AS prev_status_text,
        status_ts   AS prev_status_ts
    FROM prev_ok
    WHERE r2 = 1
),

effective_event AS (
    SELECT
        le.person_id,
        le.project_id,
        le.status_text AS latest_status_text,
        le.status_ts   AS latest_status_ts,
        CASE
            WHEN le.status_text IN (
                'Do not approach - Client in contact / has contacted',
                'Client Rejected',
                'Not Interested',
                'Not Responded'
            ) THEN COALESCE(pnr.prev_status_text, 'Added')
            ELSE le.status_text
        END AS status_for_atlas_text
    FROM latest_event le
    LEFT JOIN prev_non_reject pnr
      ON pnr.person_id  = le.person_id
     AND pnr.project_id = le.project_id
),

status_map_status AS (
    SELECT 'Identified'                        AS src_status, 'Added'       AS atlas_status
    UNION ALL SELECT 'Dryden Search Contacted',               'Added'
    UNION ALL SELECT 'Interested',                            'Added'
    UNION ALL SELECT 'Dryden Search Interview',               'Internal IV'
    UNION ALL SELECT 'Shortlisted: Presented to Client',      'Presented'
    UNION ALL SELECT 'Client Interview: 1st round',           'Client IV'
    UNION ALL SELECT 'Client Interview: 2nd round',           'Client IV'
    UNION ALL SELECT 'Client Interview: 3rd /4th round',      'Client IV'
    UNION ALL SELECT 'Offered',                               'Offer'
    UNION ALL SELECT 'Accepted / Contract Signed',            'Hired'
),

status_map_reject AS (
    SELECT 'Not Responded'                                    AS src_status, 'by_us'     AS rejection_type
    UNION ALL SELECT 'Not Interested',                                         'by_us'
    UNION ALL SELECT 'Client Rejected',                                       'by_client'
    UNION ALL SELECT 'Already in process - direct with Client',               'by_client'
    UNION ALL SELECT 'Candidate or Client put on hold',                        'by_us'
    UNION ALL SELECT 'Dryden Rejected',                                        'by_us'
    UNION ALL SELECT 'Do not approach - Client in contact / has contacted',   'by_client'
    UNION ALL SELECT 'Suggested: Client contact directly',                    'by_client'
    UNION ALL SELECT 'Withdrew',                                               'self'
),

mapped AS (
    SELECT
        ee.person_id,
        ee.project_id,
        COALESCE(sm.atlas_status, 'Added') AS status,
        rj.rejection_type AS rejection_type,
        CASE WHEN rj.rejection_type IS NOT NULL THEN 'other' ELSE NULL END AS rejection_reason,
        CASE WHEN rj.rejection_type IS NOT NULL THEN '{{ var("master_id") }}' ELSE NULL END AS atlas_rejected_by_id,
        ee.latest_status_ts AS latest_status_ts
    FROM effective_event ee
    LEFT JOIN status_map_status sm
      ON sm.src_status = ee.status_for_atlas_text
    LEFT JOIN status_map_reject rj
      ON rj.src_status = ee.latest_status_text
),

people_map AS (
    SELECT id, atlas_id AS atlas_person_id
    FROM {{ ref('people_ezekia') }}
),

projects_map AS (
    SELECT id, atlas_id AS atlas_project_id
    FROM {{ ref('projects_ezekia') }}
),

joined AS (
    SELECT
        orr.id,
        orr.atlas_id,
        orr.person_id,
        pm.atlas_person_id,
        orr.project_id,
        pjm.atlas_project_id,
        orr.owner_id,
        orr.atlas_owner_id,
        orr.created_at,
        orr.updated_at,
        COALESCE(mp.status, 'Added')       AS status,
        mp.rejection_type,
        mp.rejection_reason,
        mp.atlas_rejected_by_id,
        CASE
            WHEN mp.rejection_type IS NOT NULL
            THEN TO_CHAR(mp.latest_status_ts::timestamp, 'YYYY-MM-DD"T"00:00:00')
            ELSE NULL
        END AS rejected_at
    FROM owner_resolved orr
    LEFT JOIN mapped       mp  ON mp.person_id  = orr.person_id AND mp.project_id = orr.project_id
    LEFT JOIN people_map   pm  ON pm.id         = orr.person_id
    LEFT JOIN projects_map pjm ON pjm.id        = orr.project_id
)

SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    project_id,
    atlas_project_id,
    owner_id,
    atlas_owner_id,
    created_at,
    updated_at,
    status,
    rejection_type,
    rejection_reason,
    atlas_rejected_by_id,
    rejected_at
FROM joined
