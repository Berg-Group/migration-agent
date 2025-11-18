{{ config(
    materialized='table',
    alias='company_identities_720',
    tags=["seven20"]
) }}

WITH source_data AS (
    SELECT
        id AS company_id,
        {{ phone_norm("phone") }}AS phone,
        NULLIF(
            REGEXP_REPLACE(
                REGEXP_REPLACE(LOWER(website),
                              '^https?://(www\\.)?', ''),
                '/$', ''
            ), ''
        ) AS website,
        {{ linkedin_norm("plaunch__linkedin__c") }} AS linkedin
    FROM {{ var('source_database') }}.account
),
raw_identities AS (
    SELECT
        company_id,
        'phone' AS type,
        phone AS value
    FROM source_data
    WHERE phone IS NOT NULL AND LENGTH(phone) > 5 AND TRIM(phone) != ''

    UNION ALL

    SELECT
        company_id,
        'website',
        website
    FROM source_data
    WHERE website IS NOT NULL AND website LIKE '%.%' AND website NOT LIKE '%linkedin.com%'

    UNION ALL

    SELECT
        company_id,
        'linkedin',
        linkedin
    FROM source_data
    WHERE linkedin IS NOT NULL AND linkedin LIKE '%linkedin.com%'
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
        ri.company_id,
        ri.type,
        ri.value,
        c.atlas_id AS atlas_company_id,
        {{ atlas_uuid("( '" ~ var('clientName') ~ "' || ri.company_id || ri.type || ri.value )") }} AS atlas_id,
        c.created_at AS created_at,
        c.updated_at AS updated_at,
        ROW_NUMBER() OVER (PARTITION BY type, value ORDER BY c.created_at ) AS rn
    FROM raw_identities ri
    INNER JOIN {{ ref('4_companies_720') }} c ON ri.company_id = c.id
) deduped
WHERE rn = 1

