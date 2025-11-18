{{ config(
    materialized='table',
    alias='project_company_contacts_720',
    tags=["seven20"]
) }}

WITH base AS (
    SELECT 
        j.seven20__hiring_manager__c AS person_id,
        p.id AS project_id,
        p.atlas_id AS atlas_project_id
    FROM 
        {{ var('source_database') }}.seven20__job__c j
    INNER JOIN {{ ref('10_projects_720') }} p ON p.id = j.id
    WHERE j.seven20__hiring_manager__c IS NOT NULL 

    UNION ALL

    SELECT 
        j.seven20__hiring_manager__c AS person_id,
        p.id AS project_id, 
        p.atlas_id AS atlas_project_id
    FROM 
        {{ var('source_database') }}.seven20__job_lead__c j
    INNER JOIN {{ ref('10_projects_720') }} p ON p.id = j.id
    WHERE j.seven20__hiring_manager__c IS NOT NULL 
)
SELECT 
    b.project_id,
    b.atlas_project_id,
    b.person_id,
    cc.atlas_id AS atlas_company_contact_id
FROM base b
INNER JOIN {{ ref('2_people_720') }} p ON p.id = b.person_id
INNER JOIN {{ ref('6_company_contacts_720') }} cc ON cc.person_id = b.person_id