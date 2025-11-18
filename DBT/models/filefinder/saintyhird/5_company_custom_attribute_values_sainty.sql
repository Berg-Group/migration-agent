{{ config(
    materialized='table',
    alias='company_custom_attribute_values_sainty',
    tags=["saintyhird"]
) }}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM {{ ref('4_companies_ff') }}
),
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        ca.alias AS attribute_alias,
        cao.atlas_id AS option_id,
        cao.value AS option_value
    FROM {{ ref('2_custom_attribute_options_sainty') }} cao
    INNER JOIN {{ ref('1_custom_attributes_sainty') }} ca 
        ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'company'
),
company_industry_values AS (
    SELECT DISTINCT c.idcompany AS company_id, i.value AS value, 'company_industry' AS attribute_alias
    FROM {{ var('source_database') }}.company c
    INNER JOIN {{ var('source_database') }}.companycode cc ON cc.idcompany = c.idcompany
    INNER JOIN {{ var('source_database') }}.industry i ON i.idindustry = cc.codeid
    WHERE i.isactive = 1 AND i.value IS NOT NULL AND TRIM(i.value) != ''
)
SELECT DISTINCT
    {{ atlas_uuid('ic.company_id::text || io.atlas_attribute_id::text || io.option_id::text') }} AS atlas_id,
    ic.company_id,
    ic.atlas_company_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM company_industry_values civ
INNER JOIN internal_companies ic ON ic.company_id = civ.company_id
INNER JOIN internal_options io 
    ON io.attribute_alias = civ.attribute_alias 
    AND io.option_value = civ.value
ORDER BY ic.company_id, io.atlas_attribute_id


