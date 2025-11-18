{{ config(
    materialized='table',
    alias='candidates_fr',
    tags=["seven20"]
) }}

with internal_persons AS (
SELECT 
    id AS person_id,
    atlas_id AS atlas_person_id
FROM 
    {{ref('people_fr')}}
),

internal_projects AS (
SELECT 
    id AS project_id,
    atlas_id AS atlas_project_id
FROM 
    {{ref('projects_fr')}}
)

SELECT 
    pc.id, 
    {{atlas_uuid('ipj.project_id || ip.person_id')}} AS atlas_id, 
    ip.person_id,
    ip.atlas_person_id,
    ipj.project_id,
    ipj.atlas_project_id,
    COALESCE(u.atlas_id, '{{var('master_id')}}') AS atlas_owner_id,
    'candidate' AS class_type,
    CASE
        WHEN cj.seven20__candidate_status__c = 'New' THEN 'Added'
        WHEN cj.seven20__candidate_status__c = 'Initial Screen' THEN 'Internal IV' 
        WHEN cj.seven20__candidate_status__c = 'Sent out' THEN 'Presented'
        WHEN cj.seven20__candidate_status__c = 'Interviewing' THEN 'Client IV'
        WHEN cj.seven20__candidate_status__c = 'Offer' THEN 'Offer'
        WHEN cj.seven20__candidate_status__c = 'Placed' THEN 'Hired'
        ELSE 'Added'
    END AS status
FROM 
    {{ var('source_database') }}."seven20__placement__c" pc 
LEFT JOIN 
    {{ var('source_database') }}."seven20__job__c" j ON pc.seven20__job__c = j.id
LEFT JOIN 
    {{ var('source_database')}}.contact cj ON cj.id = pc.seven20__candidate__c
INNER JOIN 
    internal_persons ip ON ip.person_id = pc.seven20__candidate__c
INNER JOIN 
    internal_projects ipj ON ipj.project_id = j.id
LEFT JOIN 
     {{ref('1_users_720')}} u ON u.id = j.createdbyid 
 WHERE 
    ip.person_id NOTNULL 