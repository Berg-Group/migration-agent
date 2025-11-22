{{ config(materialized='table', alias='company_contacts_rcrm') }}

{% set db = var('source_database') %}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id 
    FROM 
        {{ ref('companies_rcrm') }}
),

internal_persons AS (
    SELECT
        COALESCE(contact_slug, id)                  AS person_id, 
        atlas_id               AS atlas_person_id
    FROM {{ ref('people_rcrm') }}
),

contacts_with_companies AS (
    SELECT 
        contact_data.slug AS id,
        {{ atlas_uuid('slug') }} AS atlas_id,
        ip.person_id,
        ip.atlas_person_id,
        ic.company_id,
        ic.atlas_company_id,
        'prospect' AS relationship,
        COALESCE(designation, 'contact') AS title,
        to_char(date_trunc('day', timestamp 'epoch' + (created_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"') AS created_at,
        to_char(date_trunc('day', timestamp 'epoch' + (updated_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
        ROW_NUMBER() OVER (PARTITION BY contact_data.slug ORDER BY updated_on DESC) AS rn
    FROM 
        {{ db }}.contact_data
    LEFT JOIN
        internal_companies AS ic
        ON ic.company_id = contact_data.company_slug 
    LEFT JOIN 
        internal_persons ip 
        ON ip.person_id = contact_data.slug  
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


