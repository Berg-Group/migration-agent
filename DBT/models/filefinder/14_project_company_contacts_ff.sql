{{ config(
    materialized='table',
    alias='project_company_contacts_ff',
    tags=["filefinder"]
) }}

SELECT
    a.idassignment AS project_id,
    pf.atlas_id AS atlas_project_id,
    TO_CHAR(a.createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
    TO_CHAR(a.modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
    p.idperson AS person_id,
    ccf.id AS company_contact_id,
    ccf.atlas_id AS atlas_company_contact_id
FROM {{ var('source_database') }}.assignmentcontact a
INNER JOIN {{ var('source_database') }}.person p ON p.idperson = a.idperson
INNER JOIN {{ var('source_database') }}."assignment" a2 ON a2.idassignment = a.idassignment
INNER JOIN {{ this.schema }}.projects_ff pf ON pf.id = a.idassignment
INNER JOIN {{ this.schema }}.company_contacts_ff ccf ON ccf.person_id = a.idperson