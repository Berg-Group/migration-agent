{{ config(
    materialized='table',
    alias='project_company_contacts_ezekia',
    tags=["seven20"]
) }}

WITH internal_projects AS (
    SELECT
        id::text AS project_id,
        atlas_id AS atlas_project_id
    FROM {{ ref('projects_ezekia') }}
),
internal_contacts AS (
    SELECT
        person_id::text AS company_contact_id,
        atlas_id        AS atlas_company_contact_id,
        company_id::text
    FROM {{ ref('company_contacts_ezekia') }}
),
project_company AS (
    SELECT
        b.brief_id::text       AS project_id,
        b.client_id::text AS company_id
    FROM {{ var("source_database") }}.search_firms_briefs b
)

SELECT
    ip.project_id,
    ip.atlas_project_id,
    ic.company_contact_id,
    ic.atlas_company_contact_id
FROM internal_projects ip
JOIN project_company pc USING (project_id)
JOIN internal_contacts ic USING (company_id)
