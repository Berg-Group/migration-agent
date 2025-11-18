{{ config(
    materialized='table',
    alias='companies_rect',
    tags=['recruitly']
) }}

SELECT
    c.company_id AS id,
    {{ atlas_uuid("'" ~ var('clientName') ~ "' || c.company_id::text") }} AS atlas_id,
    NULLIF(TRIM(c.company_name), '') AS name,
    CASE
        WHEN LOWER(NULLIF(TRIM(c.status), '')) IN ('is live', 'live', 'active')
            OR LOWER(NULLIF(TRIM(c.company_type), '')) LIKE '%client%'
            THEN 'client'
        WHEN LOWER(NULLIF(TRIM(c.company_type), '')) LIKE '%prospect%'
            OR LOWER(NULLIF(TRIM(c.status), '')) LIKE '%prospect%'
            THEN 'target'
        ELSE 'none'
    END AS relationship,
    {{ html_to_markdown('c.description') }} AS summary,
    NULLIF(TRIM(c.address_line), '') AS location_street_address,
    NULLIF(TRIM(c.city), '') AS location_locality,
    NULLIF(TRIM(c.region), '') AS location_region,
    NULLIF(TRIM(c.post_code), '') AS location_postal_code,
    NULLIF(TRIM(c.country), '') AS location_country,
    {{ string_to_timestamp('c.createdon') }}  AS created_at,
    {{ string_to_timestamp('c.modifiedon') }} AS updated_at
FROM {{ var('source_database') }}.companies c
WHERE NULLIF(TRIM(c.company_name), '') IS NOT NULL