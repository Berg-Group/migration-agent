{{ config(
    materialized='table',
    alias='company_identities_ff',
    tags=["filefinder"]
) }}

WITH source_company_identities AS (
    SELECT 
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || ce.idcompany_eaddress::text") }} AS atlas_id,
        cf.id AS company_id,
        cf.atlas_id AS atlas_company_id, 
        'phone' AS type,
        {{ phone_norm('e.CommValue') }} AS value, 
        TO_CHAR(ce.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(ce.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        'migration' AS source,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.Company c
    INNER JOIN {{ this.schema }}.companies_ff cf ON cf.id = c.idCompany
    INNER JOIN {{ var('source_database') }}.Company_EAddress ce ON ce.idCompany = c.idCompany
    INNER JOIN {{ var('source_database') }}.EAddress e ON e.idEAddress = ce.idEAddress
    WHERE LENGTH(e.CommValue) > 7 AND e.CommValue SIMILAR TO '%[0-9]{3}%'
    AND e.CommValue NOT LIKE '%@%.%' AND e.CommValue NOT LIKE '%www.%' AND 
    e.CommValue NOT LIKE '%http%' AND e.CommValue NOT LIKE '%.com%'

    UNION ALL

    SELECT 
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || ce.idcompany_eaddress::text") }} AS atlas_id,
        cf.id AS company_id,
        cf.atlas_id AS atlas_company_id,
        'linkedin' AS type,
        {{ linkedin_norm('e.CommValue') }} AS value,
        TO_CHAR(ce.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(ce.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        'migration' AS source,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.Company c 
    INNER JOIN {{ this.schema }}.companies_ff cf ON cf.id = c.idCompany
    INNER JOIN {{ var('source_database') }}.Company_EAddress ce ON ce.idCompany = c.idCompany
    INNER JOIN {{ var('source_database') }}.EAddress e ON e.idEAddress = ce.idEAddress
    WHERE e.CommValue IS NOT NULL AND e.CommValue NOT LIKE '%@%.%' AND e.CommValue ILIKE '%linkedin.com%'

    UNION ALL

    SELECT 
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || ce.idcompany_eaddress::text") }} AS atlas_id,
        cf.id AS company_id,
        cf.atlas_id AS atlas_company_id,
        'website' AS type,
        {{ website_norm('e.CommValue') }} AS value,
        TO_CHAR(ce.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(ce.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        'migration' AS source,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.Company c 
    INNER JOIN {{ this.schema }}.companies_ff cf ON cf.id = c.idCompany
    INNER JOIN {{ var('source_database') }}.Company_EAddress ce ON ce.idCompany = c.idCompany
    INNER JOIN {{ var('source_database') }}.EAddress e ON e.idEAddress = ce.idEAddress
    WHERE e.CommValue IS NOT NULL AND LOWER(e.CommValue) NOT LIKE '%linkedin.com%' AND e.CommValue NOT LIKE '%@%.%' AND
    (LOWER(e.CommValue) LIKE 'http%' OR LOWER(e.CommValue) LIKE 'www.%' OR LOWER(e.CommValue) LIKE '%.com%' OR LOWER(e.CommValue) LIKE '%.in%') 
)
SELECT
    atlas_id,
    company_id,
    atlas_company_id,
    type,
    value,
    created_at,
    updated_at,
    source,
    agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY type, value
            ORDER BY
                created_at
        ) AS rn
    FROM source_company_identities
) deduped
WHERE rn = 1
ORDER BY atlas_company_id