
{{ config(
    materialized='table',
    alias='company_contacts_720',
    tags=["seven20"]
) }}

WITH regular_company_contacts AS (
    SELECT
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || c.id || '-' || COALESCE(co.id, 'NULL')") }} AS atlas_id,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        co.id AS company_id,
        co.atlas_id AS atlas_company_id,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
        CASE
            WHEN a.seven20__status__c = 'New' THEN 'prospect'
            WHEN a.seven20__status__c IN ('Live', 'Terms Sent', 'Prospecting') THEN 'client'
            ELSE 'none'
        END relationship,
        c.title,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}."contact" c 
    INNER JOIN {{ ref('2_people_720') }} p ON p.id = c.id
    INNER JOIN {{ ref('4_companies_720') }} co ON co.id = c."accountId"
    INNER JOIN {{ var('source_database') }}."account" a ON a.id = co.id
    LEFT JOIN {{ var('source_database') }}."recordtype" r ON r.id = c.recordtypeid
    WHERE c.title IS NOT NULL
        AND c.title != ''
        AND COALESCE(r.name, c.seven20__record_type_name__c) = 'Client Contact'
),
dupe_people_company_contacts AS (
    SELECT
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || c.id || '-' || COALESCE(co.id, 'NULL')") }} AS atlas_id,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        co.id AS company_id,
        co.atlas_id AS atlas_company_id,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
        CASE
            WHEN a.seven20__status__c = 'New' THEN 'prospect'
            WHEN a.seven20__status__c IN ('Live', 'Terms Sent', 'Prospecting') THEN 'client'
            ELSE 'none'
        END relationship,
        c.title,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}."contact" c 
    INNER JOIN {{ ref('people_dupes_720') }} pd ON pd.contact_id = c.id
    INNER JOIN {{ ref('2_people_720') }} p ON p.id = pd.candidate_id
    INNER JOIN {{ ref('4_companies_720') }} co ON co.id = c."accountId"
    INNER JOIN {{ var('source_database') }}."account" a ON a.id = co.id
    LEFT JOIN {{ var('source_database') }}."recordtype" r ON r.id = c.recordtypeid
    WHERE c.title IS NOT NULL
        AND c.title != ''
        AND COALESCE(r.name, c.seven20__record_type_name__c) = 'Client Contact'
)
SELECT 
    atlas_id,
    person_id,
    atlas_person_id,
    company_id,
    atlas_company_id,
    created_at,
    updated_at,
    relationship,
    title,
    agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY atlas_id, person_id, company_id ORDER BY created_at DESC) AS rn
    FROM (
        SELECT * FROM regular_company_contacts
        UNION ALL
        SELECT * FROM dupe_people_company_contacts
    ) combined
) final
WHERE rn = 1