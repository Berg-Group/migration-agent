{{ config(
    materialized='table',
    alias='company_custom_text_values_sainty',
    tags=["saintyhird"]
) }}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM {{ ref('4_companies_ff') }}
),
internal_attributes AS (
    SELECT 
        atlas_id,
        alias
    FROM {{ ref('custom_attributes_text_sainty') }}
    WHERE of = 'company'
),
company_aliases_values AS (
    SELECT DISTINCT c.idcompany AS company_id, a.aliasname AS value, ia.alias AS attribute_alias
    FROM {{ var('source_database') }}.company c
    INNER JOIN {{ var('source_database') }}.company_alias ca ON ca.idcompany = c.idcompany 
    INNER JOIN {{ var('source_database') }}.alias a ON a.idalias = ca.idalias 
    INNER JOIN internal_attributes ia ON ia.alias = 'company_aliases'
    WHERE a.aliasname IS NOT NULL AND TRIM(a.aliasname) != ''
)
SELECT DISTINCT
    {{ atlas_uuid('ic.company_id::text || ca.value::text || ia.atlas_id::text') }} AS atlas_id,
    ic.company_id,
    ic.atlas_company_id,
    ia.atlas_id AS atlas_custom_attribute_id,
    ca.value AS value,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM company_aliases_values ca
INNER JOIN internal_companies ic ON ic.company_id = ca.company_id
INNER JOIN internal_attributes ia ON ia.alias = ca.attribute_alias
ORDER BY ic.company_id, ia.atlas_id
