{{ config(materialized='table', alias='company_identities_blackwood', tags=['blackwood']) }}

WITH cleaned_websites AS (
    SELECT
        CONCAT('l2-', w.level2companyid) AS original_company_id,
        LOWER(
            TRIM(
                BOTH '/'
                FROM REPLACE(REPLACE(REPLACE(REPLACE(w.websiteurl, 'https://', ''), 'http://', ''), 'www.', ''), ' ', '')
            )
        ) AS value,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        'website' AS type,
        '{{ var("agency_id") }}' AS agency_id
    FROM {{ var('source_database') }}.level2company_website w
    WHERE w.websiteurl !~ '^[0-9+ ./()\\-]+$'
      AND POSITION('.' IN w.websiteurl) > 0
),
mapped AS (
    SELECT
        cw.*,
        COALESCE(mp.new_company_id, cw.original_company_id) AS company_id_final
    FROM cleaned_websites cw
    LEFT JOIN {{ ref('companies_mapping_blackwood') }} mp USING (original_company_id)
),
cb AS (
    SELECT id, atlas_id, name, parent_company_id
    FROM {{ ref('companies_blackwood') }}
),
linked AS (
    SELECT
        COALESCE(par.id, ch.id) AS company_id,
        COALESCE(par.atlas_id, ch.atlas_id) AS atlas_company_id,
        COALESCE(par.name, ch.name) AS company_name,
        m.value,
        m.type,
        m.created_at,
        m.updated_at,
        m.agency_id
    FROM mapped m
    JOIN cb ch ON ch.id = m.company_id_final
    LEFT JOIN cb par ON par.id = ch.parent_company_id
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY value ORDER BY company_id) AS rn_value
    FROM linked
)
SELECT
    {{atlas_uuid('value')}} AS atlas_id,
    company_id,
    atlas_company_id,
    company_name,
    value,
    type,
    created_at,
    updated_at,
    agency_id,
    TRUE AS is_primary,
    value AS domain
FROM ranked
WHERE rn_value = 1
  AND atlas_company_id IS NOT NULL
