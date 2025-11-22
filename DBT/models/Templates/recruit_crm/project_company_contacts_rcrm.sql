{{ config(materialized='table', alias='project_company_contacts_rcrm') }}

{% set db = var('source_database') %}

WITH internal_persons AS (
    SELECT 
        id as person_id,
        atlas_id as atlas_person_id,
        contact_slug
    FROM {{ ref('people_rcrm') }}
),

internal_company_contacts AS (
    SELECT 
        id as contact_id,
        atlas_id as atlas_company_contact_id,
        person_id,
        atlas_person_id
    FROM {{ ref('company_contacts_rcrm') }}
),

internal_projects AS (
    SELECT 
        id as project_id,
        atlas_id as atlas_project_id
    FROM {{ ref('projects_rcrm') }}
)

SELECT 
    job_data.slug AS project_id,
    p.atlas_project_id,
    cc.contact_id AS company_contact_id,
    cc.atlas_company_contact_id
FROM   
    {{db}}.job_data job_data
JOIN 
    internal_persons AS ip 
    ON ip.contact_slug = job_data.contact_slug
JOIN 
    internal_company_contacts AS cc 
    ON cc.atlas_person_id = ip.atlas_person_id
LEFT JOIN 
    internal_projects AS p 
    ON p.project_id = job_data.slug