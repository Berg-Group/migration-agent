{{ config(
    materialized = 'table',
    alias        = 'companies_bh',
    tags         = ['bullhorn']
) }}

WITH base AS (
    SELECT
        bc."ClientCorporationID" AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || bc.\"ClientCorporationID\"::text") }} AS atlas_id,
        bc."name" AS name,
        {{ html_to_markdown('bc.CompanyDescription') }} AS summary,
		CASE
			WHEN LOWER(TRIM(bc."status")) IN ('active', 'terms signed') THEN 'client'
			WHEN LOWER(TRIM(bc."status")) IN ('archive', 'passive', 'prospect') THEN 'none'
			ELSE 'none'
		END AS relationship,
        bc."address1" AS address1,
        bc."address2" AS address2,
        bc.city AS city,
        bc.state AS state,
        bc.zip AS zip,
        bc.countryid AS country_id,
        TO_CHAR(bc."DateAdded"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(bc."DateLastModified"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        CASE
            WHEN bc.customtext1 IN ('NO', 'DO NOT SOURCE') THEN 'hard'
            WHEN bc.customtext1 = 'SOURCE WITH CARE' THEN 'soft'
            ELSE 'none'
        END AS restriction_type,
        {{ number_range('bc.numemployees') }} AS size
    FROM {{ var('source_database') }}."bh_clientcorporation" bc
    WHERE bc."name" <> 'Imported Contacts'
        AND bc."name" IS NOT NULL
        AND TRIM(bc."name") <> ''
)
SELECT
    atlas_id,
    id,
    name,
    summary,
    relationship,
    btrim(regexp_replace(coalesce(address1,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_street_address,
    btrim(regexp_replace(coalesce(city,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_metro,
    btrim(regexp_replace(coalesce(state,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_region,
    btrim(regexp_replace(coalesce(zip,''), '[^a-zA-Z0-9 ]+', ' ')) AS location_postal_code,
    btrim(regexp_replace(coalesce({{ country_bh('country_id') }}, ''), '[^a-zA-Z0-9 ]+', ' ')) AS location_country,
    {{ build_location_locality
        ('address1', 'address2', 'city', 'state', 'zip', 'location_country')
    }} AS location_locality,
    created_at,
    updated_at,
    restriction_type,
    size
FROM base
INNER JOIN {{ ref('companies_to_import') }} ci ON ci.company_id = base.id