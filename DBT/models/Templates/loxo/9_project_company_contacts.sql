{{ config(
    materialized='table',
    alias='project_company_contacts_loxo',
    tags=["loxo"]
) }}

WITH base AS (
    SELECT
        pl.id AS project_id,
        pl.atlas_id AS atlas_project_id,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        cc.atlas_id AS atlas_company_contact_id
    FROM {{ var('source_database') }}.jobs_contacts c
    INNER JOIN {{ ref('8_projects_loxo') }} pl ON pl.id = c.root_id
    INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = c.value
    INNER JOIN {{ ref('5_company_contacts_loxo') }} cc ON cc.person_id = p.id
)
SELECT
    project_id,
    atlas_project_id,
    person_id,
    atlas_person_id,
    atlas_company_contact_id
FROM base
