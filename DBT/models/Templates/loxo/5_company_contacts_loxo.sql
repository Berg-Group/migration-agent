{{ config(
    materialized='table',
    alias='company_contacts_loxo',
    tags=["loxo"]
) }}

WITH contact_people AS (
    SELECT DISTINCT
        p.id AS person_id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || p.id::text") }} AS atlas_person_id
    FROM 
        {{ var('source_database') }}.people p
    INNER JOIN 
        {{ ref('1_people_loxo') }} cp ON cp.id = p.id
    INNER JOIN 
        {{ var('source_database') }}.people_types pt ON pt.root_id = p.id AND pt.value = 'Contact'
),
current_experiences AS (
    SELECT
        p.id AS person_id,
        CAST(pe.companyid AS VARCHAR) AS company_id,
        NULLIF(TRIM(pe.title), '') AS title,
        c.atlas_id AS atlas_company_id
    FROM 
        {{ var('source_database') }}.people p
    INNER JOIN 
        {{ var('source_database') }}.people_experience pe ON pe.root_id = p.id AND pe."current" = 'true'
    INNER JOIN 
        {{ ref('3_companies_loxo') }} c ON c.id = pe.companyid
)
SELECT
    {{ atlas_uuid("person_id || 'contact' || company_id") }} AS atlas_id,
    person_id,
    atlas_person_id,
    company_id AS company_id,
    atlas_company_id AS atlas_company_id,
    '{{ var("agency_id") }}' AS agency_id,
    COALESCE(NULLIF(TRIM(title), ''), 'Title Missing') AS title,
    'client' AS relationship,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
FROM (
    SELECT
        cp.person_id,
        cp.atlas_person_id,
        ce.company_id,
        ce.atlas_company_id,
        ce.title,
        ROW_NUMBER() OVER (
            PARTITION BY cp.person_id, ce.company_id
            ORDER BY CASE WHEN ce.title IS NOT NULL AND ce.title <> '' THEN 0 ELSE 1 END,
                     cp.person_id, ce.company_id
        ) AS rn
    FROM contact_people cp
    LEFT JOIN current_experiences ce ON cp.person_id = ce.person_id
    WHERE ce.company_id IS NOT NULL
) deduped
WHERE rn = 1