{{ config(
    materialized='table',
    alias='company_identities_rect',
    tags=['recruitly']
) }}

WITH base AS (
    SELECT
        c.company_id AS company_id,
        r.atlas_id AS atlas_company_id,
        {{ phone_norm('c.land_phone') }} AS phone,
        {{ website_norm('c.website') }}  AS website,
        {{ linkedin_norm('c.linkedin') }} AS linkedin,
        {{ string_to_timestamp('c.createdon') }} AS created_at,
        {{ string_to_timestamp('c.modifiedon') }} AS updated_at
    FROM {{ var('source_database') }}.companies c
    INNER JOIN {{ ref('4_companies_rect') }} r ON r.id = c.company_id
),
raw_identities AS (
    SELECT 
        company_id, 
        atlas_company_id, 
        'phone' AS type, 
        phone AS value, 
        created_at, 
        updated_at 
    FROM base
    WHERE phone IS NOT NULL AND LENGTH(TRIM(phone)) > 3

    UNION ALL

    SELECT 
        company_id, 
        atlas_company_id, 
        'website' AS type, 
        website AS value, 
        created_at, 
        updated_at 
    FROM base
    WHERE website IS NOT NULL AND POSITION('.' IN website) > 0 AND POSITION('linkedin.com' IN website) = 0

    UNION ALL

    SELECT 
        company_id, 
        atlas_company_id, 
        'linkedin' AS type, 
        linkedin AS value, 
        created_at, 
        updated_at 
    FROM base
    WHERE linkedin IS NOT NULL AND POSITION('linkedin.com' IN linkedin) > 0
)
SELECT
    {{ atlas_uuid("( '" ~ var('clientName') ~ "' || company_id || type || value )") }} AS atlas_id,
    company_id,
    atlas_company_id,
    type,
    value,
    created_at,
    updated_at
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY type, value
            ORDER BY created_at
        ) AS rn
    FROM raw_identities
) deduped
WHERE rn = 1
ORDER BY company_id
