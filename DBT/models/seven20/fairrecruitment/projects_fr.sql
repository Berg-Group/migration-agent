{{ config(
    materialized='table',
    alias='projects_fr',
    tags=["seven20"]
) }}

with internal_companies AS (
SELECT 
    DISTINCT id AS company_id,
    atlas_id AS atlas_company_id,
    name
FROM 
     {{ref('companies_fr')}}
)

SELECT
    DISTINCT j.id,
     {{atlas_uuid('j.id')}} AS atlas_id,
    to_char(j.createddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
    to_char(j.lastmodifieddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS updated_at,
    j.name AS job_role,
    j.seven20__client_description__c AS description,
    CASE WHEN seven20__status__c IN ('Lost', 'Filled') THEN 'closed' 
         WHEN seven20__status__c = 'Live' THEN 'active' 
         WHEN seven20__status__c = 'On Hold' THEN 'on_hold' END AS state,
    CASE WHEN seven20__status__c = 'Filled' THEN 'won' 
         WHEN seven20__status__c = 'Lost' THEN 'worked_lost' END AS close_reason,
    CASE WHEN seven20__status__c IN ('Lost', 'Filled') THEN to_char(j.lastmodifieddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS')
         END AS closed_at,
    'contract' AS contract_type,
    seven20__city__c AS location_locality,
    '1' AS hire_targed,
    ic.company_id,
    ic.atlas_company_id,
    '{{ var('agency_id')}}' AS agency_id,
    j.seven20__hiring_manager__c AS owner_id,
    COALESCE(u.atlas_id, '{{var('master_id')}}') AS atlas_owner_id,
    false as public,
    'project' as class_type 
FROM 
    {{ var('source_database') }}."seven20__job__c" j 
LEFT JOIN 
     {{ var('source_database') }}."seven20__placement__c" c ON c.seven20__job__c = j.id 
LEFT JOIN 
    internal_companies ic ON ic.company_id = j.seven20__client__c 
LEFT JOIN 
    {{ref('1_users_720')}} u ON u.id = j.createdbyid