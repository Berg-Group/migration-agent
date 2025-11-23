{{ config(materialized='table', alias='company_identities_rcrm') }}

{% set db = var('source_database') %}

WITH internal_companies AS (
    SELECT 
        id,
        atlas_id AS atlas_company_id 
    FROM 
        {{ ref('5_companies_rcrm') }}
),

website_identities AS (
    SELECT 
        slug AS company_id,
        ic.atlas_company_id AS atlas_company_id,
        'website' AS type,
        REGEXP_REPLACE(REGEXP_REPLACE(LOWER(website), '^https?://(www\\.)?|^www\\.', ''),'/$', '') AS value,
        REGEXP_REPLACE(REGEXP_REPLACE(LOWER(website), '^https?://(www\\.)?|^www\\.', ''),'/$', '') AS original_url,
        TO_CHAR(DATE_TRUNC('day',  TIMESTAMP 'epoch' + created_on::bigint * INTERVAL '1 second'), 'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(DATE_TRUNC('day',  TIMESTAMP 'epoch' + updated_on::bigint * INTERVAL '1 second'), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
        '{{var('agency_id')}}' AS agency_id,
        TRUE AS is_primary,
        ROW_NUMBER() OVER (PARTITION BY REGEXP_REPLACE(REGEXP_REPLACE(LOWER(website), '^https?://(www\\.)?|^www\\.', ''),'/$', '') ORDER BY created_on) AS rn
    FROM 
        {{ db }}.company_data 
    LEFT JOIN
        internal_companies AS ic
        ON ic.id = company_data.slug
    WHERE 
        website IS NOT NULL
        AND TRIM(website) <> ''
),

linkedin_identities AS (
    SELECT 
        slug AS company_id,
        ic.atlas_company_id AS atlas_company_id,
        'linkedin' AS type,
        REGEXP_REPLACE(REGEXP_REPLACE(LOWER(profile_linkedin), '^https?://(www\\.)?|^www\\.', ''),'/$', '') AS value,
        REGEXP_REPLACE(REGEXP_REPLACE(LOWER(profile_linkedin), '^https?://(www\\.)?|^www\\.', ''),'/$', '') AS original_url,
        TO_CHAR(DATE_TRUNC('day',  TIMESTAMP 'epoch' + created_on::bigint * INTERVAL '1 second'), 'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(DATE_TRUNC('day',  TIMESTAMP 'epoch' + updated_on::bigint * INTERVAL '1 second'), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
        '{{var('agency_id')}}' AS agency_id,
        FALSE AS is_primary,
        ROW_NUMBER() OVER (PARTITION BY REGEXP_REPLACE(REGEXP_REPLACE(LOWER(profile_linkedin), '^https?://(www\\.)?|^www\\.', ''),'/$', '') ORDER BY created_on) AS rn
    FROM 
        {{ db }}.company_data
    LEFT JOIN
        internal_companies AS ic
        ON ic.id = company_data.slug
    WHERE 
        profile_linkedin IS NOT NULL
        AND TRIM(profile_linkedin) <> ''
)

SELECT 
    {{ atlas_uuid('value') }} AS atlas_id,
    company_id,
    atlas_company_id,
    type,
    value,
    original_url,
    created_at,
    updated_at,
    agency_id,
    is_primary
FROM 
    website_identities
WHERE 
    rn = 1

UNION ALL 

SELECT 
    {{ atlas_uuid('value') }} AS atlas_id,
    company_id,
    atlas_company_id,
    type,
    value,
    original_url,
    created_at,
    updated_at,
    agency_id,
    is_primary
FROM 
    linkedin_identities
WHERE 
    rn = 1
