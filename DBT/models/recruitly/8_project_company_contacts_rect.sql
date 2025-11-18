{{ config(
    materialized='table',
    alias='project_company_contacts_rect',
    tags=['recruitly']
) }}

WITH base AS (
    SELECT
        j.job_id AS project_id,
        p.atlas_id AS atlas_project_id,
        j.contact_id AS person_id,
        pe.atlas_id AS atlas_person_id,
        j.company_id AS company_id
    FROM {{ var('source_database') }}.jobs j
    INNER JOIN {{ ref('7_projects_rect') }} p ON p.id = j.job_id
    LEFT JOIN {{ ref('2_people_rect') }} pe ON pe.id = j.contact_id
    WHERE j.contact_id IS NOT NULL
),
mapped AS (
    SELECT
        b.project_id,
        b.atlas_project_id,
        b.person_id,
        b.atlas_person_id,
        cc.id AS company_contact_id,
        cc.atlas_id AS atlas_company_contact_id
    FROM base b
    INNER JOIN {{ ref('6_company_contacts_rect') }} cc
        ON cc.person_id = b.person_id
       AND cc.company_id = b.company_id
)
SELECT DISTINCT
    project_id,
    atlas_project_id,
    person_id,
    atlas_person_id,
    company_contact_id,
    atlas_company_contact_id
FROM mapped

