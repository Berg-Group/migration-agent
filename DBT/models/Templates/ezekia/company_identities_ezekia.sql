{{ config(
    materialized='table',
    alias='company_identities_ezekia'
) }}

WITH internal_companies AS (
    SELECT
        c.id AS company_id,
        c.atlas_id AS atlas_company_id
    FROM {{ ref('companies_ezekia') }} c
),

links_data AS (
    SELECT
        l.id,
        {{atlas_uuid('l.id::text')}} AS atlas_id,
        l.linkable_id AS company_id,
        cm.atlas_company_id,
        TO_CHAR(l.created_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(l.updated_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        {{linkedin_norm('l.url')}} AS value,
        l.type AS type,
        CASE WHEN l.type = 'website' THEN TRUE
             ELSE FALSE END AS "is_primary",
        ROW_NUMBER() OVER (PARTITION BY {{linkedin_norm('l.url')}} ORDER BY created_at)
    FROM {{ var("source_database") }}.links l
    INNER JOIN internal_companies cm ON l.linkable_id = cm.company_id
    WHERE l.linkable_type = 'client'
)

SELECT
    id,
    atlas_id,
    company_id,
    atlas_company_id,
    created_at,
    updated_at,
    value,
    type,
    "is_primary"
FROM links_data
WHERE row_number = 1
