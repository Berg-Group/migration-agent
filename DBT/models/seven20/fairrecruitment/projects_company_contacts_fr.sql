{{ config(
    materialized='table',
    alias='project_company_contacts_fr',
    tags=["seven20"]
) }}

with internal_projects AS (
SELECT 
    id AS project_id,
    atlas_id AS atlas_project_id
FROM 
    {{ref('projects_fr')}}
),

internal_persons AS (
SELECT
    person_id,
    atlas_id AS atlas_company_contact_id
FROM 
    {{ref('company_contacts_fr')}}
)

SELECT 
    ij.project_id,
    ij.atlas_project_id,
    ip.person_id AS company_contact_id,
    ip.atlas_company_contact_id
FROM 
    {{ var('source_database') }}."seven20__job__c" j
INNER JOIN internal_projects ij ON ij.project_id = j.id 
INNER JOIN internal_persons ip ON ip.person_id = j.seven20__hiring_manager__c
