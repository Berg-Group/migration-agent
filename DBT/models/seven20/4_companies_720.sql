{{ config(
    materialized='table',
    alias='companies_720',
    tags=["seven20"]
) }}

SELECT
    id,
    {{ atlas_uuid("'" ~ var('clientName') ~ "' || id::text") }} AS atlas_id,
    name,
    {{ clean_html('description') }} AS summary,
    BTRIM(REGEXP_REPLACE(COALESCE(billingstreet,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_street_address, 
    BTRIM(REGEXP_REPLACE(COALESCE(billingcity,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_metro, 
    BTRIM(REGEXP_REPLACE(COALESCE(billingstate,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_region, 
    BTRIM(REGEXP_REPLACE(COALESCE(billingpostalcode,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_postal_code, 
    BTRIM(REGEXP_REPLACE(COALESCE(billingcountry,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_country,
    {{ build_location_locality
        ('billingstreet', 'NULL', 'billingcity', 'billingstate', 'billingpostalcode', 'billingcountry')
    }} AS location_locality,
    CASE
        WHEN seven20__status__c IN ('Live') THEN 'client'
        WHEN seven20__status__c IN ('New', 'Terms Sent', 'Prospecting') THEN 'target'
        ELSE 'none'
    END AS relationship,
    TO_CHAR(createddate::timestamp(0), 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    createdbyid AS created_by_id,
    '{{ var('agency_id') }}' AS agency_id,
    {{ number_range('numberofemployees') }} AS size
FROM {{ var('source_database') }}.account
WHERE name IS NOT NULL
    AND TRIM(name) <> ''