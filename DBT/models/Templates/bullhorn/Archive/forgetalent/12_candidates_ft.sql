{{ config(
    materialized = 'table',       
    alias        = 'candidates_ft',
    tags         = ['bullhorn']
) }}

WITH base_data AS (
    SELECT
        r.jobresponseid                                         AS id,
        TO_CHAR(r.dateadded::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
        TO_CHAR(r.dateadded::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS updated_at,
        r.jobpostingid                                          AS project_id,
        r.userid                                                AS person_id,
        r.sendinguserid                                         AS owner_id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || r.jobresponseid::text") }} AS atlas_id,
        CASE
            WHEN lower(r.status) IN ('6 placed', 'placed') THEN 'Hired'
            WHEN lower(r.status) IN (
                'interview rejected', '1st interview', 'interview/rejected', '2nd interview', '3rd interview', 'final interview'
            ) THEN 'Client IV'
            WHEN lower(r.status) IN (
                'shortlisted', 'rejected', 'new lead', 'shortlist', 'longlist', 'long list', 
                'candidate rejected', 'rejected not submitted', 'of interest', 'longlist - screened'
            ) THEN 'Added'
            WHEN lower(r.status) IN ('offer extended', 'offer rejected') THEN 'Offer'
            WHEN lower(r.status) IN (
                'submitted to client', 'cv sent', 'submitted', 'rejected by client', 'client rejected', 'rejected by client', 'new submission'
            ) THEN 'Presented'
            WHEN lower(r.status) IN ('screened', 'candidate interested') THEN 'Internal IV'
        ELSE 'Added'     END AS status,
        CASE 
            WHEN lower(r.status) IN ('rejected', 'rejected not submitted', 'consultant rejected') THEN 'by_us'
            WHEN lower(r.status) IN ('candidate rejected', 'offer rejected')  THEN 'self'
            WHEN lower(r.status) IN (
                'interview rejected', 'interview/rejected', 'rejected by client', 'client rejected', 'rejected by client'
            )THEN 'by_client'
            ELSE NULL
        END AS rejection_type,
        CASE 
            WHEN rejection_type IS NOT NULL THEN 'other'
            ELSE NULL
        END AS rejection_reason,
        CASE
            WHEN rejection_type IS NOT NULL THEN TO_CHAR(r.dateadded::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"')
            ELSE NULL
        END AS rejected_at,
        'Candidate' AS class_type,
        r.sendinguserid AS rejected_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_rejected_by_id
    FROM {{ var('source_database') }}."bh_jobresponse" r
    LEFT JOIN {{ ref('users_ft') }} u ON r.sendinguserid = u.id
),
placed AS (
    SELECT
        jobresponseid,
        TO_CHAR(MAX(dateadded)::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS hired_at
    FROM {{ var('source_database') }}."bh_jobresponsehistory"
    WHERE lower(status) = 'placed'
    GROUP BY jobresponseid
),
with_placed AS (
    SELECT 
        b.*, 
        p.hired_at
    FROM base_data b
    LEFT JOIN placed p ON b.id = p.jobresponseid AND b.status = 'Hired'
),
with_project_ids AS (
    SELECT
        wp.*,
        pr.atlas_id AS atlas_project_id
    FROM with_placed wp
    INNER JOIN {{ ref('10_projects_ft') }} pr ON wp.project_id = pr.id
),
with_owner_ids AS (
    SELECT
        wpi.*,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id
    FROM with_project_ids wpi
    LEFT JOIN {{ ref('users_ft') }} u ON wpi.owner_id = u.id
),
with_person_ids AS (
    SELECT
        woi.*,
        pe.atlas_id AS atlas_person_id
    FROM with_owner_ids woi
    INNER JOIN {{ ref('1_people_ft') }} pe ON woi.person_id = pe.id
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
    class_type,
    rejected_at,
    rejection_type,
    rejection_reason,
    rejected_by_id,
    atlas_rejected_by_id,
    owner_id,
    atlas_owner_id,
    status,
    hired_at
FROM (SELECT
    id,
    atlas_id,
    created_at,
    updated_at,
    project_id,
    atlas_project_id,
    person_id,
    atlas_person_id,
    class_type,
    rejected_at,
    rejection_type,
    rejection_reason,
    CASE
        WHEN rejection_type IS NOT NULL THEN rejected_by_id
    END AS rejected_by_id,
    CASE
        WHEN rejection_type IS NOT NULL THEN atlas_rejected_by_id
    END AS atlas_rejected_by_id,
    owner_id,
    atlas_owner_id,
    status,
    hired_at,
    ROW_NUMBER() OVER (PARTITION BY atlas_project_id, atlas_person_id ORDER BY created_at DESC) AS rn
FROM with_person_ids
) deduped
WHERE rn = 1
ORDER BY atlas_project_id