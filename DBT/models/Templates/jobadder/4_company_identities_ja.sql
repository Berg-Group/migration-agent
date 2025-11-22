-- File: models/intercity/company_identities_ja.sql

{{ config(
    materialized='table',
    alias='company_identities_ja'
) }}

WITH companies_ja_lookup AS (
    SELECT
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM {{ ref('3_companies_ja') }}
),
linkedins AS (
    SELECT *
    FROM (
        SELECT 
            c.companyid AS company_id,
            regexp_replace(c.linkedinurl, '^.*linkedin\.com', 'linkedin.com') AS linkedin_url,
            c.datecreated AS raw_created_at,
            c.dateupdated AS raw_updated_at,
            ROW_NUMBER() OVER (PARTITION BY regexp_replace(c.linkedinurl, '^.*linkedin\.com', 'linkedin.com') 
                             ORDER BY c.dateupdated DESC) as rn
        FROM {{ var('source_database') }}."company" c
        WHERE c.linkedinurl IS NOT NULL AND c.linkedinurl <> '' AND c.linkedinurl <> ' ' AND c.deleted = FALSE
    ) ranked
    WHERE rn = 1
),
twitters AS (
    SELECT *
    FROM (
        SELECT 
            c.companyid AS company_id,
            regexp_replace(c.twitterurl, '^.*twitter\.com', 'x.com') AS twitter_url,
            c.datecreated AS raw_created_at,
            c.dateupdated AS raw_updated_at,
            ROW_NUMBER() OVER (PARTITION BY regexp_replace(c.twitterurl, '^.*twitter\.com', 'x.com')
                             ORDER BY c.dateupdated DESC) as rn
        FROM {{ var('source_database') }}."company" c
        WHERE c.twitterurl IS NOT NULL AND c.twitterurl <> '' AND c.twitterurl <> ' ' AND c.deleted = FALSE
    ) ranked
    WHERE rn = 1
),
websites AS (
    SELECT *
    FROM (
        SELECT 
            ca.companyid AS company_id,
            {{ linkedin_norm('url') }} AS website_value,
            ROW_NUMBER() OVER (
                PARTITION BY {{ linkedin_norm('url') }}
                ORDER BY ca.companyid
            ) as rn
        FROM {{ var('source_database') }}."companyaddress" ca
        WHERE ca.url IS NOT NULL AND ca.url <> '' AND ca.url <> ' '
    ) ranked
    WHERE rn = 1
)
SELECT
    {{ atlas_uuid('twitter_url') }} AS atlas_id,
    twitter_url AS value,
    to_char(raw_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS created_at,
    to_char(raw_updated_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS updated_at,
    'twitter' AS type,
    '{{ var('agency_id') }}' AS agency_id,
    t.company_id,
    c.atlas_company_id,
    FALSE AS is_primary
FROM twitters t
INNER JOIN companies_ja_lookup c ON t.company_id = c.company_id
UNION ALL
SELECT
    {{ atlas_uuid('linkedin_url') }} AS atlas_id,
    linkedin_url AS value,
    to_char(raw_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS created_at,
    to_char(raw_updated_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS updated_at,
    'linkedin' AS type,
    '{{ var('agency_id') }}' AS agency_id,
    l.company_id,
    c.atlas_company_id,
    FALSE AS is_primary
FROM linkedins l
INNER JOIN companies_ja_lookup c ON l.company_id = c.company_id
UNION ALL 
SELECT
    {{ atlas_uuid('website_value') }} AS atlas_id,
    website_value AS value,
    to_char(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS created_at,
    to_char(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS updated_at,
    'website' AS type,
    '{{ var("agency_id") }}' AS agency_id,
    w.company_id,
    c.atlas_company_id,
    TRUE as is_primary
FROM websites w
INNER JOIN companies_ja_lookup c ON w.company_id = c.company_id
