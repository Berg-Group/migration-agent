{{ config(
    materialized='table',
    alias='companies_fr',
    tags=["seven20"]
) }}

WITH internal_projects AS (
SELECT id AS project_id, 
       seven20__client__c as company_id,
       row_number() over (partition by seven20__client__c) AS rn
FROM  {{ var('source_database') }}.seven20__job__c),

base AS (
    SELECT
        id,
        {{atlas_uuid('id')}} AS atlas_id,
        name,
        to_char(createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        billingpostalcode || ' ' || billingcountry || ' ' || billingcity || ' ' || billingstreet AS location_locality,
        '{{ var('agency_id') }}' AS agency_id

    FROM 
        {{ var('source_database') }}."account"
    WHERE isdeleted = 0
)

SELECT base.*,
    CASE WHEN ip.project_id NOTNULL THEN 'client' 
          ELSE 'target' END AS relationship
FROM base
LEFT JOIN internal_projects ip ON ip.company_id = base.id
AND rn = 1