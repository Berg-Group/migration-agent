-- File: models/intercity/company_contacts_ja.sql

{{ config(
    materialized='table',
    alias='company_contacts_ja'
) }}

WITH base AS (
    SELECT
        c.contactid AS base_contact_id,
        c.companyid AS company_id,
        COALESCE(NULLIF(TRIM(c.position), ''), 'Contact') AS title,
        c.datecreated AS raw_created_at,
        c.dateupdated AS raw_updated_at,
        c.iscandidateonly AS iscandidateonly,
        c.contactid::text || 'companycontact' || '{{ var('clientName') }}' AS uuid_input
    FROM 
        {{ var('source_database') }}."contact" c
    WHERE iscandidateonly = false
    AND c.deleted = false
    AND c.companyid IS NOT NULL
),

people_ja_lookup AS (
    SELECT
        id AS person_id,
        atlas_id AS atlas_person_id
    FROM {{ ref('1_people_ja') }}
),

companies_ja_lookup AS (
    SELECT
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM {{ ref('3_companies_ja') }}
),

company_contacts AS (
    SELECT
        base.base_contact_id AS person_id,
        {{ atlas_uuid('uuid_input') }} AS atlas_id,
        to_char(base.raw_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS created_at,
        to_char(base.raw_updated_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS updated_at,
        '{{ var('agency_id') }}' AS agency_id,
        p.atlas_person_id,
        base.company_id,
        c.atlas_company_id,
        'prospect' AS relationship,
        base.title
    FROM base
    INNER JOIN people_ja_lookup p
    ON base.base_contact_id = p.person_id
    INNER JOIN companies_ja_lookup c USING (company_id)
)

SELECT *
FROM company_contacts
