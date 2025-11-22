{{ config(
    materialized='table',
    alias='companies_720',
    tags=["seven20"]
) }}

WITH internal_projects AS (
SELECT id AS project_id, 
       seven20__account__c as company_id 
FROM sjt_public.seven20__job__c),

base AS (
    SELECT
        id,
        lower(
            substring(md5(id::text), 1, 8) || '-' ||
            substring(md5(id::text), 9, 4) || '-' ||
            substring(md5(id::text), 13, 4) || '-' ||
            substring(md5(id::text), 17, 4) || '-' ||
            substring(md5(id::text), 21, 12)
        ) AS atlas_id,
        name,
        to_char(createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        billingpostalcode || ' ' || billingcountry || ' ' || billingcity || ' ' || billingstreet AS location_locality,
        '{{ var('agency_id') }}' AS agency_id

    FROM 
        {{ var('source_database') }}."account"
)

SELECT base.*,
CASE WHEN ip.project_id NOTNULL THEN 'client' ELSE 'target' END AS relationship
FROM base
LEFT JOIN internal_projects ip ON ip.company_id = base.id
