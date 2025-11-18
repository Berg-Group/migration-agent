{{ config(
    materialized='table',
    alias='candidates_ff',
    tags=["filefinder"]
) }}

WITH candidates AS (
    SELECT 
        c.idassignmentcandidate AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || c.idassignmentcandidate::text") }} AS atlas_id,
        TO_CHAR(c.createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(c.modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        p.id AS project_id,
        p.atlas_id AS atlas_project_id,
        pf.id AS person_id,
        pf.atlas_id AS atlas_person_id,
        'Candidate' AS class_type,
        CASE
            WHEN LOWER(cp.value) IN ('research', 'research 1', 'research 2', 'research 3',  
                'rejected by consult.', 'rejected by client', 'candidate withdrew'
            ) OR LOWER(cp.value) IS NULL THEN 'Added'
            WHEN LOWER(cp.value) IN ('contacted by cons') THEN 'Reached out'
            WHEN LOWER(cp.value) IN ('turned down approach') THEN 'Not interested'
            WHEN LOWER(cp.value) IN ('longlist', 'shortlist', 'longlist reject', 
                'shortlist reject', 'longlist withdrew', 'shortlist withdrew'
            ) THEN 'Presented'
            WHEN LOWER(cp.value) IN ('client interview', 'client interview reject', 'client interview withdrew') THEN 'Client IV'
            WHEN LOWER(cp.value) IN ('internal interview', 'interviewed by cons', 
                'internal interview reject', 'internal interview withdrew'
            ) THEN 'Internal IV'
            WHEN LOWER(cp.value) IN ('offer', 'offer accept', 'offer reject', 'offer withdrew') THEN 'Offer'
            WHEN LOWER(cp.value) IN ('placed') THEN 'Hired'
        END AS status,
        CASE
            WHEN LOWER(cp.value) IN (
                'turned down approach', 'candidate withdrew', 'offer reject',
                'client interview withdrew', 'internal interview withdrew', 'shortlist withdrew',
                'longlist withdrew', 'offer withdrew'
            ) THEN 'self'
            WHEN LOWER(cp.value) IN ('rejected by consult.', 'internal interview reject') THEN 'by_us'
            WHEN LOWER(cp.value) IN ('rejected by client', 'client interview reject', 
                'longlist reject', 'shortlist reject', 'internal interview reject'
            ) THEN 'by_client'
            ELSE NULL
        END AS rejection_type,
        CASE 
            WHEN LOWER(cp.value) IN ('rejected by consult.', 'rejected by client', 'client interview reject', 
                'longlist reject', 'shortlist reject', 'internal interview reject'
            ) THEN 'not_qualified'
            WHEN LOWER(cp.value) IN ('turned down approach') THEN 'other'
            WHEN LOWER(cp.value) IN ('candidate withdrew', 'offer reject', 'client interview withdrew', 
                'internal interview withdrew', 'shortlist withdrew', 'longlist withdrew', 'offer withdrew'
            ) THEN 'accepted_another_offer'
            ELSE NULL
        END AS rejection_reason,
        CASE 
            WHEN LOWER(cp.value) IN (
                'rejected by consult.', 'rejected by client', 'client interview reject',
                'longlist reject', 'shortlist reject', 'internal interview reject',
                'turned down approach', 'candidate withdrew', 'offer reject',
                'client interview withdrew', 'internal interview withdrew', 'shortlist withdrew',
                'longlist withdrew', 'offer withdrew'
            ) THEN TO_CHAR(c.contactedon::date, 'YYYY-MM-DD') || 'T00:00:00Z'
            ELSE NULL
        END AS rejected_at,
        CASE 
            WHEN LOWER(cp.value) IN ('placed') THEN TO_CHAR(c.contactedon::date, 'YYYY-MM-DD') || 'T00:00:00Z'
            ELSE NULL
        END AS hired_at,
        CASE 
            WHEN LOWER(cp.value) IN (
                'rejected by consult.', 'rejected by client', 'client interview reject',
                'longlist reject', 'shortlist reject', 'internal interview reject',
                'turned down approach', 'candidate withdrew', 'offer reject',
                'client interview withdrew', 'internal interview withdrew', 'shortlist withdrew',
                'longlist withdrew', 'offer withdrew'
            ) THEN uf.id
            ELSE NULL
        END AS rejected_by_id,
        CASE 
            WHEN LOWER(cp.value) IN (
                'rejected by consult.', 'rejected by client', 'client interview reject',
                'longlist reject', 'shortlist reject', 'internal interview reject',
                'turned down approach', 'candidate withdrew', 'offer reject',
                'client interview withdrew', 'internal interview withdrew', 'shortlist withdrew',
                'longlist withdrew', 'offer withdrew'
            ) THEN COALESCE(uf.atlas_id, '{{ var("master_id") }}')
            ELSE NULL
        END AS atlas_rejected_by_id,
        uf2.id AS owner_id,
        COALESCE(uf2.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
        '{{ var('agency_id')}}' AS agency_id
    FROM {{ var('source_database') }}.assignmentcandidate c 
    INNER JOIN {{ ref('12_projects_ff') }} p ON p.id = c.idassignment 
    INNER JOIN {{ ref('2_people_ff') }} pf ON pf.id = c.idperson 
    LEFT JOIN {{ var('source_database') }}.candidateprogress cp ON cp.idcandidateprogress = c.idcandidateprogress
    LEFT JOIN {{ this.schema }}.users_ff uf ON LOWER(uf.name) = LOWER(c.contactedby)
    LEFT JOIN {{ this.schema }}.users_ff uf2 ON LOWER(uf2.name) = LOWER(c.createdby)
    WHERE c.isexcluded != 1
),
sources_as_candidates AS (
    SELECT 
        c.idassignmentsource AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || 'SRC-' || c.idassignmentsource::text") }} AS atlas_id,
        TO_CHAR(c.createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(c.createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        p.id AS project_id,
        p.atlas_id AS atlas_project_id,
        pf.id AS person_id,
        pf.atlas_id AS atlas_person_id,
        'Candidate' AS class_type,
        'Sources' AS status,
        NULL AS rejection_type,
        NULL AS rejection_reason,
        NULL AS rejected_at,
        NULL AS hired_at,
        NULL AS rejected_by_id,
        NULL AS atlas_rejected_by_id,
        uf.id AS owner_id,
        COALESCE(uf.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
        '{{ var('agency_id')}}' AS agency_id
    FROM {{ var('source_database') }}.assignmentsource c 
    INNER JOIN {{ ref('12_projects_ff') }} p ON p.id = c.idassignment 
    INNER JOIN {{ ref('2_people_ff') }} pf ON pf.id = c.idperson 
    LEFT JOIN {{ var('source_database') }}.assignmentsourceprogress cp ON cp.idassignmentsourceprogress = c.idassignmentsourceprogress
    LEFT JOIN {{ this.schema }}.users_ff uf ON LOWER(uf.name) = LOWER(c.createdby)
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
    status,
    rejection_type,
    rejection_reason,
    rejected_at,
    hired_at,
    rejected_by_id,
    atlas_rejected_by_id,
    owner_id,
    atlas_owner_id,
    agency_id
FROM (
    SELECT 
        final.*,
        ROW_NUMBER() OVER (PARTITION BY final.project_id, final.person_id ORDER BY final.created_at DESC) AS rn
    FROM (
        SELECT * FROM candidates
        UNION ALL
        SELECT * FROM sources_as_candidates
    ) final
) ranked
WHERE rn = 1
ORDER BY atlas_project_id