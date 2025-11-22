{{ config(
    materialized='table',
    alias='company_contacts_blackwood',
    tags=["blackwood"]
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id, 
        atlas_id AS atlas_person_id
    FROM {{ ref('people_blackwood') }}
),

internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM 
        {{ ref('companies_blackwood') }}
    ),

title AS (
SELECT 
    h.cref AS person_id,
    'l2-' || h.level2companyid::varchar AS company_id,
    hr.notes AS title 
FROM 
    {{ var('source_database') }}."candidate_employmenthistory" h
LEFT JOIN   
    {{ var('source_database') }}."candidate_employmenthistory_role" hr ON hr.employmenthistoryuid = h.uid AND hr.roleorder = 1 ),

t AS (SELECT 
    cref AS person_id,
    'l2-' || currentcompany_level2companyid::varchar AS company_id,
    '{{ var("agency_id") }}' AS agency_id,
    TO_CHAR(current_date, 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(current_date, 'YYYY-MM-DD"T00:00:00"') AS updated_at
FROM 
     {{ var('source_database') }}.candidate 
WHERE 
    currentcompany_level2companyid NOTNULL)

SELECT 
    {{ atlas_uuid("ip.person_id::text || ic.company_id::text") }} AS atlas_id,
    ic.company_id,
    ic.atlas_company_id,
    ip.person_id,
    ip.atlas_person_id,
    t.agency_id,
    t.created_at,
    t.updated_at,
    'prospect' AS role,
    title.title
FROM 
    t 
LEFT JOIN internal_persons ip USING (person_id)
LEFT JOIN internal_companies ic USING (company_id)
LEFT JOIN title USING (person_id, company_id)