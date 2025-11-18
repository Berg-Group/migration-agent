{{ config(
    materialized='table',
    alias='project_company_contacts_fix',
    tags=["seven20"]
) }}

WITH internal_projects AS (
    SELECT 
        id AS project_id,
        atlas_id AS atlas_project_id
    FROM "{{ this.schema }}"."projects"
),

internal_persons AS (
    SELECT
        person_id AS company_contact_id,
        atlas_id AS atlas_company_contact_id
    FROM "{{ this.schema }}"."sjt_company_contacts_fix"
)

SELECT 
    ij.project_id,
    ij.atlas_project_id,
    ip.company_contact_id,
    ip.atlas_company_contact_id
FROM {{ var('source_database') }}."seven20__job__c" j
INNER JOIN internal_persons ip ON ip.company_contact_id = j.seven20__hiring_manager__c
LEFT JOIN internal_projects ij ON ij.project_id = j.id 
