{{ config(
    materialized='table',
    alias='project_company_contacts_invenias',
    tags=["invenias"]
) }}

with internal_projects AS (
SELECT 
    id AS project_id,
    atlas_id AS atlas_project_id
FROM 
    {{ref('projects_invenias')}}
),

internal_persons AS (
SELECT
    atlas_id AS atlas_company_contact_id,
    person_id
FROM 
    {{ref('company_contacts_invenias')}}
),

t AS (
SELECT 
    ip.project_id,
    ip.atlas_project_id,
    internal_persons.person_id AS company_contact_id,
    internal_persons.atlas_company_contact_id 
FROM 
    {{ var('source_database') }}."assignments" AS project
LEFT JOIN {{ var('source_database') }}."relation_assignmenttoassignmentclient" AS acc 
    ON acc.assignmentid = project.itemid 
LEFT JOIN {{ var('source_database') }}."relation_persontoassignmentclient" AS ac 
    ON ac.assignmentclientid = acc.assignmentclientid
LEFT JOIN
    internal_projects ip ON ip.project_id = project."itemid"
LEFT JOIN 
    internal_persons ON internal_persons.person_id =  ac.personid )

SELECT project_id,
    company_contact_id, 
    atlas_project_id, 
    atlas_company_contact_id 
FROM t
WHERE 
    atlas_project_id NOTNULL 
    AND atlas_company_contact_id  NOTNULL
GROUP BY 1,2,3,4

