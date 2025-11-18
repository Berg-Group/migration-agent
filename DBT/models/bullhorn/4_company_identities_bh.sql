{{ config(
    materialized = 'table',
    alias        = 'company_identities_bh',
    tags         = ['bullhorn']
) }}

WITH source_data AS (
    SELECT
        bc."ClientCorporationID" AS company_id,
        NULLIF(TRIM(bc."phone"), '') AS phone,
        {{ website_norm("bc.companyURL") }} AS website,
        {{ linkedin_norm("bc.linkedinprofilename") }} AS linkedin
    FROM {{ var('source_database') }}."bh_clientcorporation" bc
),
raw_identities AS (
    SELECT
        company_id,
        'phone' AS type,
        phone AS value
    FROM source_data
    WHERE phone IS NOT NULL AND LENGTH(phone) > 6 AND TRIM(phone) != ''

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
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        ROW_NUMBER() OVER (PARTITION BY type, value ORDER BY company_id) AS rn
    FROM raw_identities ri
    INNER JOIN {{ ref('3_companies_bh') }} c ON ri.company_id = c.id
) deduped
WHERE rn = 1