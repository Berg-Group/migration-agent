{{ config(
    materialized='table',
    alias='people_ff',
    tags=["filefinder"]
) }}

WITH source_people AS (
    SELECT 
        idPerson AS id,
        FirstName AS first_name,
        MiddleName AS middle_name,
        LastName AS last_name,
        TO_CHAR(createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        'active' AS responsiveness,
        'regular' AS candidate_status,
        TRIM(
            CASE 
                WHEN biography IS NOT NULL AND biography != '' 
                THEN 'Biography: ' || CHR(13) || CHR(10) || biography::text || CHR(13) || CHR(10) || CHR(13) || CHR(10)
                ELSE '' 
            END ||
            CASE 
                WHEN personcomment IS NOT NULL AND personcomment != '' 
                THEN 'Person Comment: ' || CHR(13) || CHR(10) || personcomment::text
                ELSE '' 
            END
        ) AS overview,
        iduser,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.person
    WHERE isdeleted != 1
),
people_locations AS (
    SELECT * FROM (
        SELECT 
            pp.idperson AS person_id,
            TRIM(
                CASE 
                    WHEN TRIM(COALESCE(p.addressline1, '')) != '' 
                         AND TRIM(COALESCE(p.addressline2, '')) != '' 
                         AND TRIM(COALESCE(p.addressline3, '')) != '' 
                         AND TRIM(COALESCE(p.addressline4, '')) != ''
                    THEN TRIM(p.addressline1) || ', ' || TRIM(p.addressline2) || ', ' || TRIM(p.addressline3) || ', ' || TRIM(p.addressline4)
                    WHEN TRIM(COALESCE(p.addressline1, '')) != '' 
                         AND TRIM(COALESCE(p.addressline2, '')) != '' 
                         AND TRIM(COALESCE(p.addressline3, '')) != ''
                    THEN TRIM(p.addressline1) || ', ' || TRIM(p.addressline2) || ', ' || TRIM(p.addressline3)
                    WHEN TRIM(COALESCE(p.addressline1, '')) != '' 
                         AND TRIM(COALESCE(p.addressline2, '')) != ''
                    THEN TRIM(p.addressline1) || ', ' || TRIM(p.addressline2)
                    WHEN TRIM(COALESCE(p.addressline1, '')) != ''
                    THEN TRIM(p.addressline1)
                    ELSE ''
                END
            ) AS location_street_address,
            p.city AS location_locality,
            p.countystate AS location_region,
            p.postcode AS location_postal_code,
            c.value AS location_country,
            ROW_NUMBER() OVER (
                PARTITION BY pp.idperson 
                ORDER BY 
                    CASE 
                        WHEN LOWER(p2.value) = 'private' THEN 1
                        WHEN LOWER(p2.value) = 'second home' THEN 2
                        ELSE 3
                    END
            ) AS rn
        FROM {{ var('source_database') }}.person_paddress pp 
        INNER JOIN {{ var('source_database') }}.paddress p ON p.idpaddress = pp.idpaddress
        LEFT JOIN {{ var('source_database') }}.country c ON c.idcountry = p.idcountry
        LEFT JOIN {{ var('source_database') }}.personaddresstype p2 ON p2.idpersonaddresstype = pp.idpersonaddresstype
        WHERE (
            (p.addressline1 IS NOT NULL AND p.addressline1 != '') OR
            (p.addressline2 IS NOT NULL AND p.addressline2 != '') OR
            (p.addressline3 IS NOT NULL AND p.addressline3 != '') OR
            (p.addressline4 IS NOT NULL AND p.addressline4 != '') OR
            (p.city IS NOT NULL AND p.city != '') OR
            (p.countystate IS NOT NULL AND p.countystate != '') OR
            (p.postcode IS NOT NULL AND p.postcode != '') OR
            (c.value IS NOT NULL AND c.value != '')
        )
        AND LOWER(p2.value) != 'business'
    ) WHERE rn = 1
)
SELECT
    p.id,
    {{ atlas_uuid("'" ~ var('clientName') ~ "' || p.id::text") }} AS atlas_id,
    p.first_name,
    p.middle_name,
    p.last_name,
    p.created_at,
    p.updated_at,
    p.responsiveness,
    p.candidate_status,
    p.overview,
    l.location_street_address,
    l.location_locality,
    l.location_region,
    l.location_postal_code,
    l.location_country,
    p.agency_id,
    p.iduser AS created_by_id,
    p.iduser AS updated_by_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS updated_by_atlas_id
FROM source_people p
LEFT JOIN people_locations l ON l.person_id = p.id
LEFT JOIN {{ this.schema }}.users_ff u ON u.id = p.iduser