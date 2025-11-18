{{ config(
    materialized='table',
    alias='experiences_fr',
    tags=["seven20"]
) }}

with internal_persons AS (
SELECT
    DISTINCT id AS person_id,
    atlas_id AS atlas_person_id
FROM
    {{ref('people_fr')}}
),

internal_companies AS (
SELECT 
    DISTINCT id AS company_id,
    atlas_id AS atlas_company_id,
    name,
    ROW_NUMBER() OVER (PARTITION BY name)
FROM 
     {{ref('companies_fr')}} 
)

SELECT 
    h.id,
    {{atlas_uuid('h.id || ip.person_id || h.seven20__account__c')}} AS atlas_id,
    to_char(h.createddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
    h.seven20__start_date__c::DATE AS started_at,
    h.seven20__end_date__c::DATE AS finished_at,
    ip.person_id,
    ip.atlas_person_id,
    ic.company_id,
    ic.atlas_company_id,
    h.seven20__account__c AS company_name,
    h.seven20__description__c AS description,
    h.seven20__job_title__c AS title,
    u.atlas_id AS created_by_atlas_id,
    'migration' AS source,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    {{ var('source_database') }}."seven20__employment_history__c" h 
LEFT JOIN 
    internal_persons ip ON ip.person_id = h.seven20__candidate__c
LEFT JOIN 
    internal_companies ic ON trim(lower(ic.name)) = trim(lower(h."seven20__account__c"))
    AND row_number = 1 
LEFT JOIN
    {{ref('1_users_720')}} u ON u.id = h.createdbyid
WHERE
    (h.seven20__account__c NOTNULL OR ic.atlas_company_id NOTNULL) 
    AND h.seven20__start_date__c NOTNULL