{{ config(
    materialized='table',
    alias='company_contacts_rect',
    tags=['recruitly']
) }}

WITH contact_people AS (
    SELECT
        c.contact_id AS person_id,
        p.atlas_id AS atlas_person_id,
        c.company_id AS company_id,
        co.atlas_id AS atlas_company_id,
        co.relationship,
        NULLIF(TRIM(c.headline), '') AS title,
        {{ string_to_timestamp('c.createdon') }} AS created_at,
        {{ string_to_timestamp('c.modifiedon') }} AS updated_at
    FROM {{ var('source_database') }}.contacts c
    INNER JOIN {{ ref('2_people_rect') }} p ON p.id = c.contact_id
    INNER JOIN {{ ref('4_companies_rect') }} co ON co.id = c.company_id
    WHERE c.company_id IS NOT NULL
)
SELECT
    person_id || 'contact' || company_id AS id,
    {{ atlas_uuid("person_id || 'contact' || company_id") }} AS atlas_id,
    person_id,
    atlas_person_id,
    company_id,
    atlas_company_id,
    created_at,
    updated_at,
    CASE
        WHEN relationship = 'client' THEN 'client'
        WHEN relationship IN ('target', 'prospect') THEN 'prospect'
        ELSE 'none'
    END AS relationship,
    COALESCE(title, 'Missing Title') AS title,
    '{{ var("agency_id") }}' AS agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY person_id, company_id
            ORDER BY CASE WHEN title IS NULL OR title = '' THEN 1 ELSE 0 END,
                     updated_at DESC NULLS LAST
        ) AS rn
    FROM contact_people
) d
WHERE d.rn = 1