{{ config(
    materialized='table',
    alias='candidates_720',
    tags=["seven20"]
) }}

WITH base AS (
    SELECT 
        c.id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || c.id") }} AS atlas_id,
        TO_CHAR(c.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(c.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        j.id AS project_id,
        j.atlas_id AS atlas_project_id,
        CASE
            WHEN c.seven20__stage__c IN ('Rejected') THEN 'Added'
            WHEN c.seven20__stage__c IN ('Submitted') THEN 'CV Submitted'
            WHEN c.seven20__stage__c IN ('Third Stage Interview') THEN 'Further Interviews'
            WHEN c.seven20__stage__c IN ('Second Stage Interview') THEN '2nd Interview'
            WHEN c.seven20__stage__c IN ('First Stage Interview') THEN '1st Interview'
            WHEN c.seven20__stage__c IN ('Longlist') THEN 'Longlist'
            WHEN c.seven20__stage__c IN ('Placement') OR h.seven20__application__c IS NOT NULL THEN 'Hired'
            WHEN c.seven20__stage__c IN ('Shortlist') THEN 'Shortlist'
            WHEN c.seven20__stage__c IN ('Offer') THEN 'Offer'
        END AS status,
        CASE
            WHEN c.seven20__rejected_reason__c IN ('Communication Skills') THEN 'cultural_fit'
            WHEN c.seven20__rejected_reason__c IN ('Experience', 'Skill Set') THEN 'not_qualified'
            WHEN c.seven20__rejected_reason__c IN ('Rejected Offer') THEN 'accepted_another_offer'
            ELSE 'other'
        END AS rejection_reason,
        CASE
            WHEN c.seven20__rejected_reason__c IN ('Communication Skills', 'Experience', 'Skill Set') THEN 'by_us'
            WHEN c.seven20__rejected_reason__c IN ('Job Closed', 'No Feedback Provided') THEN 'by_client'
            WHEN c.seven20__rejected_reason__c IN ('Rejected Offer', 'No Show') THEN 'self'
        END AS rejection_type,
        CASE
            WHEN rejection_type IS NOT NULL THEN TO_CHAR(c.seven20__date_in_stage__c::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"')
            ELSE NULL
        END AS rejected_at,
        CASE
            WHEN status = 'Hired' THEN TO_CHAR(c.seven20__date_in_stage__c::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"')
            ELSE NULL
        END AS hired_at,
        CASE
            WHEN rejection_type IS NOT NULL THEN c.ownerid
            ELSE NULL
        END AS rejected_by_id,
        CASE
            WHEN rejection_type IS NOT NULL THEN u.atlas_id
            ELSE NULL
        END AS atlas_rejected_by_id,
        c.ownerid AS owner_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
        '{{ var('agency_id')}}' AS agency_id
    FROM 
        {{ var('source_database') }}.seven20__application__c c 
    INNER JOIN 
        {{ ref('2_people_720') }} p ON p.id = c.seven20__candidate__c
    INNER JOIN 
        {{ ref('10_projects_720') }} j ON j.id = c.seven20__job__c
    LEFT JOIN 
        {{ ref('1_users_720') }} u ON u.id = c.ownerid
    LEFT JOIN 
        {{ var('source_database') }}.seven20__placement__c h ON h.seven20__application__c = c.id
    WHERE 
        c.isdeleted = 0
)
SELECT 
    id,
    atlas_id,
    created_at,
    updated_at,
    person_id,
    atlas_person_id,
    project_id,
    atlas_project_id,
    status,
    rejection_reason,
    rejection_type,
    rejected_at,
    hired_at,
    rejected_by_id,
    atlas_rejected_by_id,
    owner_id,
    atlas_owner_id,
    agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM base
) ranked_base
WHERE rn = 1