{{ config(
    materialized = 'table',
    alias = 'project_company_contacts_bh',
    tags=["bullhorn"]
) }}

WITH source_data AS (
    SELECT
        jo.JobPostingID AS project_id,
        jo.ClientUserID AS person_id
    FROM {{ var('source_database') }}."bh_jobopportunity" jo
),
mapped_company_contacts AS (
    SELECT
        sd.project_id,
        sd.person_id,
        cc.id AS company_contact_id,
        cc.atlas_id AS atlas_company_contact_id
    FROM source_data sd
    INNER JOIN {{ ref('5_company_contacts_bh') }} cc ON sd.person_id = cc.person_id
),
mapped_projects AS (
    SELECT
        mcc.project_id,
        p.atlas_id AS atlas_project_id,
        mcc.person_id,
        mcc.company_contact_id,
        mcc.atlas_company_contact_id
    FROM mapped_company_contacts mcc
    INNER JOIN {{ ref('10_projects_bh') }} p ON mcc.project_id = p.id
)
SELECT
    project_id,
    atlas_project_id,
    person_id,
    company_contact_id,
    atlas_company_contact_id
FROM mapped_projects
