{{ config(materialized='table', alias='company_contacts_manatal') }}

{% set db = var('source_database') %}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id 
    FROM 
        {{ ref('3_companies_manatal') }}
),

internal_persons AS (
    SELECT
        id AS person_id, 
        atlas_id AS atlas_person_id,
        contact_id
    FROM {{ ref('1_people_manatal') }}
),

contacts_with_companies AS (
    SELECT 
        c.id AS id,
        {{ atlas_uuid('c.id') }} AS atlas_id,
        ip.person_id,
        ip.atlas_person_id,
        ic.company_id,
        ic.atlas_company_id,
        'prospect' AS relationship,
        'contact' AS title,  -- Default title as 'contact' since role isn't in DB
        TO_CHAR(DATE_TRUNC('day', c.created_at::timestamp), 'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(DATE_TRUNC('day', c.updated_at::timestamp), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
        ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY c.updated_at DESC) AS rn
    FROM 
        {{ db }}.contact c
    LEFT JOIN
        internal_companies AS ic
        ON ic.company_id = c.organization_id
    LEFT JOIN 
        internal_persons ip 
        ON (
            -- Join on the contact ID itself for standalone contacts
            ip.person_id = 'cc' || c.id
            -- Also include contacts that are matched to candidates through people_manatal
            OR ip.contact_id = c.id
        )
    WHERE
        ip.atlas_person_id IS NOT NULL
        AND ic.company_id IS NOT NULL
)

SELECT 
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    company_id,
    atlas_company_id,
    relationship,
    title,
    created_at,
    updated_at
FROM 
    contacts_with_companies
WHERE 
    rn = 1


