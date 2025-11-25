{{ config(
    materialized='table',
    alias='project_company_contacts_ja'
) }}

WITH base AS (
    SELECT
        -- Map s.joborderid to d.project_id
        s.joborderid AS project_id,

        -- Map s.joborderid to d.atlas_project_id using projects_ja
        pj.atlas_id AS atlas_project_id,

        -- Map s.contactid to d.company_contact_id
        s.contactid AS company_contact_id,

        -- Map s.contactid to d.atlas_company_contact_id using company_contacts_ja
        cc.atlas_id AS atlas_company_contact_id

    FROM
        {{ var('source_database') }}.joborder s
    INNER JOIN
        {{ref('10_projects_ja')}} pj ON s.joborderid = pj.id
    INNER JOIN
        {{ref('5_company_contacts_ja')}} cc ON s.contactid = cc.person_id
)
SELECT *
FROM base
WHERE company_contact_id IS NOT NULL
