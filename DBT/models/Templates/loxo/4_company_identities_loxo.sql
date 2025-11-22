{{ config(
    materialized='table',
    alias='company_identities_loxo',
    tags=["loxo"]
) }}

WITH parsed_urls AS (
    SELECT
        c.id AS company_id,
        lox.atlas_id AS atlas_company_id,
        {{ linkedin_norm("c.url") }} AS linkedin_url,
        {{ website_norm("c.url") }} AS website_url
    FROM kloud_public.companies c
    INNER JOIN {{ ref('3_companies_loxo') }} lox ON lox.id = c.id
    WHERE c.url IS NOT NULL
      AND LENGTH(TRIM(c.url)) > 0
),
raw_identities AS (
    SELECT
        company_id,
        atlas_company_id,
        'website' AS type,
        website_url AS value,
        {{ atlas_uuid("( '" ~ var('clientName') ~ "' || company_id || 'website' || website_url )") }} AS atlas_id,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
    FROM parsed_urls
    WHERE website_url IS NOT NULL
      AND POSITION('linkedin.com' IN website_url) = 0
      AND POSITION('.' IN website_url) > 0

    UNION ALL

    SELECT
        company_id,
        atlas_company_id,
        'linkedin' AS type,
        linkedin_url AS value,
        {{ atlas_uuid("( '" ~ var('clientName') ~ "' || company_id || 'linkedin' || linkedin_url )") }} AS atlas_id,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
    FROM parsed_urls
    WHERE linkedin_url IS NOT NULL
      AND POSITION('linkedin.com' IN linkedin_url) > 0
)

SELECT
    atlas_id,
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
) dupe
WHERE rn = 1
ORDER BY company_id