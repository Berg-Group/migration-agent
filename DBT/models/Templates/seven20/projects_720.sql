{{ config(
    materialized='table',
    alias='projects_720',
    tags=["seven20"]
) }}

with internal_companies AS (
SELECT 
    DISTINCT id AS company_id,
    atlas_id AS atlas_company_id,
    name
FROM 
     "{{ this.schema }}"."companies"   
)

SELECT
    DISTINCT j.id,
     lower(
            substring(md5(j.id::text), 1, 8) || '-' ||
            substring(md5(j.id::text), 9, 4) || '-' ||
            substring(md5(j.id::text), 13, 4) || '-' ||
            substring(md5(j.id::text), 17, 4) || '-' ||
            substring(md5(j.id::text), 21, 12)
        ) AS atlas_id,
    to_char(j.createddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
    to_char(j.lastmodifieddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS updated_at,
    j.name AS job_role,
    j.seven20__client_description__c AS description,
    CASE WHEN seven20__job_closed__c THEN 'closed' ELSE 'active' END AS state,
    CASE WHEN seven20__job_closed__c THEN 
        CASE WHEN seven20__status__c = 'Placed' THEN 'won' ELSE 'worked_lost' END END AS close_reason,
    CASE WHEN seven20__job_closed__c THEN to_char(j.lastmodifieddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS')
         END AS closed_at,
    'contract' AS contract_type,
    seven20__city__c AS location_locality,
    '1' AS hire_targed,
    ic.company_id,
    ic.atlas_company_id,
    '{{ var('agency_id')}}' AS agency_id,
    j.ownerid AS owner_id,
    u.atlas_id AS atlas_owner_id,
    false as public,
    'project' as class_type 
FROM 
    {{ var('source_database') }}."seven20__job__c" j 
LEFT JOIN 
     {{ var('source_database') }}."seven20__placement__c" c ON c.seven20__job__c = j.id 
LEFT JOIN 
    internal_companies ic ON ic.company_id = j.seven20__account__c 
LEFT JOIN 
    "{{ this.schema }}"."users" u ON u.id = j.ownerid