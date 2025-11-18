{{ config(
    materialized='table',
    alias='companies_rz'
) }}

with company_address AS (
    SELECT 
        companyid,
        suburb,
        state,
        country,
        line1,
        postcode,
        row_number() over (partition by companyid) AS rn
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
    'target' AS relationship,
    ca.line1 AS location_street_address,
    ca.suburb AS location_metro,
    ca.state AS location_region,
    ca.country AS location_country,
    {{ concat('ca.postcode,ca.line1,ca.suburb,ca.state,ca.country') }} AS location_locality
FROM 
    {{ var('source_database') }}."company" c
LEFT JOIN 
    company_address ca ON c.companyid = ca.companyid AND rn = 1
WHERE 
    c.deleted = FALSE
