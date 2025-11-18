{{ config(
    materialized='table',
    alias='company_custom_attribute_values_inte',
    tags = ["bullhorn"]
) }}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM 
        {{ ref('3_companies_bh') }}
),  
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        cao.atlas_id AS option_id,
        cao.id AS external_id,
        ca.alias AS attribute_type,
        cao.value AS option_value
    FROM 
        {{ ref('2_custom_attribute_options_inte') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_inte') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'company'
),
account_manager_mappings AS (
    SELECT DISTINCT
        c.ClientCorporationID AS company_id,
        TRIM(c.customtextblock2) AS value
    FROM {{ var('source_database') }}.bh_clientcorporation c
    WHERE c.customtextblock2 IS NOT NULL 
      AND TRIM(c.customtextblock2) != ''
),
combined_mappings AS (
    SELECT 
        company_id,
        'account_manager'::text AS attribute_type,
        value
    FROM account_manager_mappings
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
FROM 
    combined_mappings cm
INNER JOIN 
    internal_companies ic ON ic.company_id = cm.company_id
INNER JOIN 
    internal_options io ON io.option_value = cm.value AND io.attribute_type = cm.attribute_type
ORDER BY
    ic.company_id,
    io.atlas_attribute_id 


