-- File: models/intercity/companies_ja.sql

{{ config(
    materialized='table',
    alias='companies_ja'
) }}


with company_loc_dedup AS (
    SELECT 
        companyid,
        suburb,
        line1,
        state,
        country,
        row_number() OVER (PARTITION BY companyid)
    FROM
        {{ var('source_database') }}."companyaddress" 
    WHERE 
        name = 'Head Office'
)
SELECT
    c.companyid AS id,
    {{ atlas_uuid('c.companyid') }} AS atlas_id,
    to_char(c.datecreated, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS created_at,
    to_char(c.dateupdated, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS updated_at,
    TRIM(c.name) AS name,
    ca.line1 AS location_street_address,
    ca.suburb AS location_metro,
    ca.state AS location_region,
    ca.country AS location_country,
    {{ concat('ca.line1,ca.suburb,ca.state,ca.country') }} AS location_locality,
    CASE WHEN statusid = 5452 THEN 'client' 
             WHEN statusid = 5455 THEN 'prospect'
            ELSE 'target' END AS relationship
FROM 
    {{ var('source_database') }}."company" c
LEFT JOIN 
    company_loc_dedup ca ON ca.companyid = c.companyid AND row_number = 1
WHERE 
    c.deleted = FALSE
