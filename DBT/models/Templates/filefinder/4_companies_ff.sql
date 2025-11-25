{{ config(
    materialized='table',
    alias='companies_ff',
    tags=["filefinder"]
) }}

WITH source_companies AS (
    SELECT 
        idcompany AS id,
        companyname AS name,
        companycomment AS summary,
        clientrelationshipnotes AS overview,
        'none' AS relationship,
        {{ number_range('noofemployees')}} AS size,
        TO_CHAR(createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        'migration' AS source,
        iduser AS owner_id,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.Company 
    WHERE isdeleted != 1
),
company_locations AS (
    SELECT * FROM (
        SELECT 
            c.idcompany AS company_id,
            TRIM(
                CASE 
                    WHEN TRIM(COALESCE(a.addressline1, '')) != '' 
                         AND TRIM(COALESCE(a.addressline2, '')) != '' 
                         AND TRIM(COALESCE(a.addressline3, '')) != '' 
                         AND TRIM(COALESCE(a.addressline4, '')) != ''
                    THEN TRIM(a.addressline1) || ', ' || TRIM(a.addressline2) || ', ' || TRIM(a.addressline3) || ', ' || TRIM(a.addressline4)
                    WHEN TRIM(COALESCE(a.addressline1, '')) != '' 
                         AND TRIM(COALESCE(a.addressline2, '')) != '' 
                         AND TRIM(COALESCE(a.addressline3, '')) != ''
                    THEN TRIM(a.addressline1) || ', ' || TRIM(a.addressline2) || ', ' || TRIM(a.addressline3)
                    WHEN TRIM(COALESCE(a.addressline1, '')) != '' 
                         AND TRIM(COALESCE(a.addressline2, '')) != ''
                    THEN TRIM(a.addressline1) || ', ' || TRIM(a.addressline2)
                    WHEN TRIM(COALESCE(a.addressline1, '')) != ''
                    THEN TRIM(a.addressline1)
                    ELSE ''
                END
            ) AS location_street_address,
            a.city AS location_locality,
            a.countystate AS location_region,
            a.postcode AS location_postal_code,
            c2.value AS location_country,
            ROW_NUMBER() OVER (
                PARTITION BY c.idcompany 
                ORDER BY 
                    CASE 
                        WHEN LOWER(t.value) = 'postal address' THEN 1
                        WHEN LOWER(t.value) = 'invoice' THEN 2
                        ELSE 3
                    END
            ) AS rn
        FROM {{ var('source_database') }}.Company c 
        INNER JOIN {{ var('source_database') }}.Company_PAddress p ON p.idCompany = c.idCompany 
        INNER JOIN {{ var('source_database') }}.PAddress a ON a.idPAddress = p.idPAddress 
        LEFT JOIN {{ var('source_database') }}.Country c2 ON c2.idCountry = a.idCountry
        LEFT JOIN {{ var('source_database') }}.companyaddresstype t ON t.idcompanyaddresstype = p.idcompanyaddresstype
        WHERE (a.addressline1 IS NOT NULL AND a.addressline1 != '') OR
              (a.addressline2 IS NOT NULL AND a.addressline2 != '') OR
              (a.addressline3 IS NOT NULL AND a.addressline3 != '') OR
              (a.addressline4 IS NOT NULL AND a.addressline4 != '') OR
              (a.City IS NOT NULL AND a.City != '') OR
              (a.CountyState IS NOT NULL AND a.CountyState != '') OR
              (a.PostCode IS NOT NULL AND a.PostCode != '') OR
              (c2.Value IS NOT NULL AND c2.Value != '')
    ) WHERE rn = 1
),
final_companies AS (
SELECT
    c.id,
    {{ atlas_uuid("'" ~ var('clientName') ~ "' || c.id::text") }} AS atlas_id,
    c.name,
    c.summary,
    c.overview,
    c.relationship,
    c.size,
    c.created_at,
    c.updated_at,
    c.source,
    l.location_street_address,
    l.location_locality,
    l.location_region,
    l.location_postal_code,
    l.location_country,
    c.owner_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS atlas_owner_id,
    c.agency_id,
    ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(c.id))
        ORDER BY c.created_at ASC
    ) AS rn
FROM source_companies c
LEFT JOIN company_locations l ON l.company_id = c.id
LEFT JOIN {{ this.schema }}.users_ff u ON u.id = c.owner_id
)
SELECT
    id,
    atlas_id,
    name,
    summary,
    overview,
    relationship,
    size,
    created_at,
    updated_at,
    source,
    location_street_address,
    location_locality,
    location_region,
    location_postal_code,
    location_country,
    owner_id,
    atlas_owner_id,
    agency_id
FROM final_companies
WHERE rn = 1