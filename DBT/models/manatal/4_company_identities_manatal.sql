{{ config(materialized='table', alias='company_identities_manatal') }}

{% set db = var('source_database') %}

WITH internal_companies AS (
    SELECT 
        id,
        atlas_id AS atlas_company_id 
    FROM 
        {{ ref('3_companies_manatal') }}
),

website_identities AS (
    SELECT 
        o.id AS company_id,
        ic.atlas_company_id AS atlas_company_id,
        'website' AS type,
        REGEXP_REPLACE(REGEXP_REPLACE(LOWER(o.website), '^https?://(www\\.)?|^www\\.', ''),'/$', '') AS value,
        o.website AS original_url,
        TO_CHAR(DATE_TRUNC('day', o.created_at::timestamp), 'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(DATE_TRUNC('day', o.updated_at::timestamp), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
        '{{var('agency_id')}}' AS agency_id,
        TRUE AS is_primary,
        ROW_NUMBER() OVER (PARTITION BY REGEXP_REPLACE(REGEXP_REPLACE(LOWER(o.website), '^https?://(www\\.)?|^www\\.', ''),'/$', '') ORDER BY o.created_at) AS rn
    FROM 
        {{ db }}.organization o
    LEFT JOIN
        internal_companies AS ic
        ON ic.id = o.id
    WHERE 
        o.website IS NOT NULL
        AND TRIM(o.website) <> ''
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
