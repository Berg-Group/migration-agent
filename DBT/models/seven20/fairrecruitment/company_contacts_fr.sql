{{ config(
    materialized='table',
    alias='company_contacts_fr',
    tags=["seven20"]
) }}

WITH internal_persons AS (
    SELECT DISTINCT id AS person_id, atlas_id AS atlas_person_id
    FROM {{ ref('people_fr') }}
),
internal_companies AS (
    SELECT DISTINCT id AS company_id, atlas_id AS atlas_company_id
    FROM {{ ref('companies_fr') }}
),
base AS (
    SELECT
        c.id::text AS person_id,
        {{ atlas_uuid('c.id || a.id') }} AS atlas_id,
        to_char(c.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        to_char(c.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        '{{ var('agency_id') }}' AS agency_id,
        a.id AS company_id,
        CASE WHEN a.type = 'Prospect' THEN 'prospect' ELSE 'client'  END AS relationship,
        c.title
    FROM {{ var('source_database') }}."contact" c
    JOIN {{ var('source_database') }}."account" a ON a.id = c."accountId"
    WHERE c.isdeleted = 0
      AND c.recordtypeid = '0124L000000tya8'
      AND a.name <> 'Candidate Pool'
)

SELECT b.*, ip.atlas_person_id, ic.atlas_company_id
FROM base b
JOIN internal_persons ip USING (person_id)
LEFT JOIN internal_companies ic USING (company_id)
