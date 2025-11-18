{{ config(
    materialized = 'table',
    alias = 'people_ja',
    tags = ['jobadder', 'people']
) }}

WITH location_source AS (
    SELECT DISTINCT
        can.contactid,
        COALESCE(TRIM(can.addresscountry), TRIM(rp.addresscountry)) AS location_country,
        COALESCE(TRIM(can.addresspostcodesearch), TRIM(rp.addresspostcode)) AS location_postal_code,
        COALESCE(TRIM(can.addressstate), TRIM(rp.addressstate)) AS location_region,
        COALESCE(TRIM(can.addressline1), TRIM(rp.addressline1)) AS location_street_address,
        COALESCE(TRIM(can.addressline2), TRIM(rp.addressline2)) AS location_address_line_2,
        COALESCE(TRIM(can.addresssuburb), TRIM(rp.addressstate)) AS location_metro,
        TRIM(
            COALESCE(TRIM(can.addressline1), TRIM(rp.addressline1), '') || ',' ||
            COALESCE(TRIM(can.addressline2), TRIM(rp.addressline2), '') || ',' ||
            COALESCE(TRIM(can.addresssuburb), TRIM(rp.addressstate), '') || ',' ||
            COALESCE(TRIM(can.addresspostcodesearch), TRIM(rp.addresspostcode), '') || ',' ||
            COALESCE(TRIM(can.addresscountry), TRIM(rp.addresscountry), '')
        ) AS location_locality
    FROM 
        {{ var('source_database') }}."candidate" can
    LEFT JOIN 
        {{ var('source_database') }}."candidateresumeparsing" rp ON can.contactid = rp.contactid
    WHERE 
        can.deleted = FALSE
),
contact_source AS (
    SELECT DISTINCT
        C.contactid AS id,
        to_char(C.datecreated,'YYYY-MM-DD"T"HH24:MI:SS.MS') AS created_at,
        to_char(C.dateupdated,'YYYY-MM-DD"T"HH24:MI:SS.MS') AS updated_at,
        NULLIF(TRIM(C.lastname), '') AS last_name,
        NULLIF(TRIM(C.firstname), '') AS first_name,
        C.createdbyuserid AS created_by_id,
        C.updatedbyuserid AS updated_by_id
    FROM
        {{ var('source_database') }}."contact" C
    WHERE
        C.deleted = FALSE
),
company_address_source AS (
    SELECT DISTINCT
        c.contactid,
        NULLIF(TRIM(ca.country), '') AS company_location_country,
        NULLIF(TRIM(ca.postcodesearch), '') AS company_location_postal_code,
        NULLIF(TRIM(ca.state), '') AS company_location_region,
        NULLIF(TRIM(ca.line1), '') AS company_location_street_address,
        CAST(NULL AS TEXT) AS company_location_address_line_2,
        NULLIF(TRIM(ca.suburb), '') AS company_location_metro
    FROM {{ var('source_database') }}."contact" c
    LEFT JOIN {{ var('source_database') }}."companyaddress" ca
        ON c.companyaddressid = ca.addressid
    WHERE c.deleted = FALSE
),
user_lookup AS (
    SELECT
        id AS user_id,
        atlas_id AS user_atlas_id
    FROM
        {{ ref('users_ja') }}
)
SELECT
    cs.id,
    {{ atlas_uuid('cs.id::TEXT') }} AS atlas_id,
    cs.created_at,
    cs.updated_at,
    cs.last_name,
    cs.first_name,
    cs.created_by_id,
    COALESCE(cr.user_atlas_id,'{{ var("master_id") }}') AS created_by_atlas_id,
    cs.updated_by_id,
    COALESCE(up.user_atlas_id,'{{ var("master_id") }}') AS updated_by_atlas_id,
    '{{ var("agency_id") }}' AS agency_id,
    'active' AS responsiveness,
    'regular' AS candidate_status,
    COALESCE(NULLIF(ls.location_country, ''), cas.company_location_country) AS location_country,
    COALESCE(NULLIF(ls.location_postal_code, ''), cas.company_location_postal_code) AS location_postal_code,
    COALESCE(NULLIF(ls.location_region, ''), cas.company_location_region) AS location_region,
    COALESCE(NULLIF(ls.location_street_address, ''), cas.company_location_street_address) AS location_street_address,
    COALESCE(NULLIF(ls.location_address_line_2, ''), cas.company_location_address_line_2) AS location_address_line_2,
    COALESCE(NULLIF(ls.location_metro, ''), cas.company_location_metro) AS location_metro,
    TRIM(
        COALESCE(NULLIF(ls.location_street_address, ''), cas.company_location_street_address, '') || ',' ||
        COALESCE(NULLIF(ls.location_address_line_2, ''), cas.company_location_address_line_2, '') || ',' ||
        COALESCE(NULLIF(ls.location_metro, ''), cas.company_location_metro, '') || ',' ||
        COALESCE(NULLIF(ls.location_postal_code, ''), cas.company_location_postal_code, '') || ',' ||
        COALESCE(NULLIF(ls.location_country, ''), cas.company_location_country, '')
    ) AS location_locality,
    CASE WHEN ls.contactid IS NOT NULL THEN TRUE ELSE FALSE END AS has_candidate_record
FROM
    contact_source cs
LEFT JOIN 
    location_source ls ON cs.id = ls.contactid
LEFT JOIN 
    company_address_source cas ON cs.id = cas.contactid
LEFT JOIN 
    user_lookup cr ON cs.created_by_id = cr.user_id
LEFT JOIN 
    user_lookup up ON cs.updated_by_id = up.user_id