{{ config(
    materialized='table',
    alias='candidates_720',
    tags=["seven20"]
) }}

with internal_persons AS (
SELECT 
    id AS person_id,
    atlas_id AS atlas_person_id
FROM 
    "{{ this.schema }}"."people"
),

internal_projects AS (
SELECT 
    id AS project_id,
    atlas_id AS atlas_project_id
FROM 
    "{{ this.schema }}"."projects"
)

SELECT 
    pc.id, 
    lower(
            substring(md5(j.id::text || '-' || coalesce(pc.seven20__candidate__c::text, 'NULL')), 1, 8) || '-' ||
            substring(md5(j.id::text || '-' || coalesce(pc.seven20__candidate__c::text, 'NULL')), 9, 4) || '-' ||
            substring(md5(j.id::text || '-' || coalesce(pc.seven20__candidate__c::text, 'NULL')), 13, 4) || '-' ||
            substring(md5(j.id::text || '-' || coalesce(pc.seven20__candidate__c::text, 'NULL')), 17, 4) || '-' ||
            substring(md5(j.id::text || '-' || coalesce(pc.seven20__candidate__c::text, 'NULL')), 21, 12)
        ) AS atlas_id,
    ip.person_id,
    ip.atlas_person_id,
    ipj.project_id,
    ipj.atlas_project_id,
    u.atlas_id AS atlas_owner_id,
    'candidate' AS class_type,
    CASE
        WHEN j.seven20__status__c = 'No CVs' THEN 'Added'
        WHEN j.seven20__status__c = 'Longlist' THEN 'Added'
        WHEN j.seven20__status__c = 'CV' THEN 'Presented'
        WHEN j.seven20__status__c = 'Interview' THEN 'Client IV'
        WHEN j.seven20__status__c = 'Offer' THEN 'Offer'
        WHEN j.seven20__status__c = 'Placed' THEN 'Offer'
        ELSE j.seven20__status__c
    END AS status
FROM 
    {{ var('source_database') }}."seven20__placement__c" pc 
LEFT JOIN 
    {{ var('source_database') }}."seven20__job__c" j ON pc.seven20__job__c = j.id
LEFT JOIN 
    internal_persons ip ON ip.person_id = pc.seven20__candidate__c
LEFT JOIN 
    internal_projects ipj ON ipj.project_id = j.id
LEFT JOIN 
    "{{ this.schema }}"."users" u ON u.id = j.ownerid
LEFT JOIN 
 "{{ this.schema }}"."users" u1 ON u1.id = j.createdbyid 
 WHERE 
    ip.person_id NOTNULL 