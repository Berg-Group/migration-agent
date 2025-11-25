{{ config(
    materialized = 'table',       
    alias        = 'candidates_bh',
    tags         = ['bullhorn']
) }}

WITH base_data AS (
    SELECT
        r.jobresponseid                                         AS id,
        TO_CHAR(r.dateadded::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(r.dateadded::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        r.jobpostingid                                          AS project_id,
        r.userid                                                AS person_id,
        r.sendinguserid                                         AS owner_id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || r.jobresponseid::text") }} AS atlas_id,
        CASE
            WHEN LOWER(TRIM(r.status)) IN ('6 placed', 'placed') THEN 'Hired'
            WHEN LOWER(TRIM(r.status)) IN (
                '1st interview', '2nd interview', '3rd interview', 'final interview',
                '1st phone screen', '2nd phone screen', '1st onsite interview', '2nd onsite interview',
                'technical test', 'reference check',
                'interview', 'further interview', 'interview rejected', 'interview/rejected',
                '1st stage', 'first stage', 'mid stage', 'second interview', 'technical interview'
            ) THEN 'Client IV'
            WHEN LOWER(TRIM(r.status)) IN (
                'rejected', 'new lead', 'shortlist', 'longlist', 'long list',
                'candidate rejected', 'rejected not submitted', 'of interest', 'longlist - screened', 'consultant rejected',
                'client rejected', 'rejected by client', 'online response',
                'candidate interested', 'candidate not interested', 'sales rep rejected'
            ) THEN 'Added'
            WHEN LOWER(TRIM(r.status)) IN ('offer extended', 'offer rejected') THEN 'Offer'
            WHEN LOWER(TRIM(r.status)) IN (
                'submitted to client', 'cv sent', 'submitted', 'new submission', 'submission', 'client submission'
            ) THEN 'Presented'
            WHEN LOWER(TRIM(r.status)) IN ('screened', 'shortlisted', 'internally submitted', 'interview scheduled') THEN 'Internal IV'
            WHEN LOWER(TRIM(r.status)) IN ('candidate interested') THEN 'Interested'
        END AS status,
        CASE 
            WHEN LOWER(TRIM(r.status)) IN ('rejected', 'rejected not submitted', 'consultant rejected', 'sales rep rejected') THEN 'by_us'
            WHEN LOWER(TRIM(r.status)) IN ('candidate rejected', 'offer rejected', 'candidate not interested')  THEN 'self'
            WHEN LOWER(TRIM(r.status)) IN (
                'interview rejected', 'interview/rejected', 'rejected by client', 'client rejected', 'rejected by client'
            )THEN 'by_client'
            ELSE NULL
        END AS rejection_type,
        CASE 
            WHEN LOWER(TRIM(r.status)) IN ('candidate rejected') THEN 'not_qualified'
            WHEN LOWER(TRIM(r.status)) IN ('offer rejected') THEN 'accepted_another_offer'
            WHEN LOWER(TRIM(r.status)) IN ('client rejected') THEN 'not_qualified'
            WHEN rejection_type IS NOT NULL THEN 'other'
            ELSE NULL
        END AS rejection_reason,
        CASE
            WHEN rejection_type IS NOT NULL THEN TO_CHAR(r.dateadded::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS')
            ELSE NULL
        END AS rejected_at,
        'Candidate' AS class_type,
        r.sendinguserid AS rejected_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_rejected_by_id
    FROM {{ var('source_database') }}."bh_jobresponse" r
    LEFT JOIN {{ ref('0_users_bh') }} u ON r.sendinguserid = u.id
),
placed AS (
    SELECT
        jobresponseid,
        TO_CHAR(MAX(dateadded)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS hired_at
    FROM {{ var('source_database') }}."bh_jobresponsehistory"
    WHERE LOWER(TRIM(status)) = 'placed'
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
    INNER JOIN {{ ref('11_projects_bh') }} pr ON wp.project_id = pr.id
),
with_owner_ids AS (
    SELECT
        wpi.*,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id
    FROM with_project_ids wpi
    LEFT JOIN {{ ref('0_users_bh') }} u ON wpi.owner_id = u.id
),
with_person_ids AS (
    SELECT
        woi.*,
        pe.atlas_id AS atlas_person_id
    FROM with_owner_ids woi
    INNER JOIN {{ ref('1_people_bh') }} pe ON woi.person_id = pe.id
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