{{ config(
    materialized='table',
    alias='company_contacts_ezekia',
    tags=["seven20"]
) }}

WITH internal_persons AS (
    SELECT  id::text AS person_id, 
            atlas_id AS atlas_person_id
    FROM {{ ref('people_ezekia') }}
),
internal_companies AS (
    SELECT  id::text AS company_id, 
        atlas_id AS atlas_company_id,
        relationship
    FROM {{ ref('companies_ezekia') }}
),
base AS (
    SELECT
        pp.id AS id,
        p.id::text AS person_id,
        {{ atlas_uuid('pp.id::text || c.client_id::text') }} AS atlas_id,
        TO_CHAR(COALESCE(pp.created_at, p.created_at)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(COALESCE(pp.updated_at, p.updated_at, pp.created_at)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        c.client_id::text AS company_id,
        pp.title,
        ROW_NUMBER() OVER (PARTITION BY p.id, c.client_id ORDER BY end_date DESC)
    FROM {{ var("source_database") }}.search_firms_clients c
    JOIN {{ var("source_database") }}.people_positions pp ON pp.company_id = c.client_id
    JOIN {{ var("source_database") }}.people p ON p.id = pp.person_id
    WHERE pp.end_date > CURRENT_DATE
)

SELECT b.*, ip.atlas_person_id, ic.atlas_company_id, ic.relationship
FROM base b
INNER JOIN internal_persons ip USING (person_id)
INNER JOIN internal_companies ic USING (company_id)
WHERE NULLIF(trim(title),'') NOTNULL
    AND row_number = 1 