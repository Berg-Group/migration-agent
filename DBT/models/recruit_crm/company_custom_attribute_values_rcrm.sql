{{ config(
    materialized='table',
    alias='company_custom_attribute_values_rcrm',
    tags = ["recruit_crm"]
) }}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM 
        {{ ref('companies_rcrm') }}
),  
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        cao.atlas_id AS option_id,
        cao.id AS external_id
    FROM 
        {{ ref('custom_attribute_options_rcrm') }} cao
    INNER JOIN 
        {{ ref('custom_attributes_rcrm') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE 
        ca.name IN ('Company Hotlist')
),
company_hotlist_results AS (
    SELECT 
        {{ atlas_uuid('ch.company_slug') }} AS atlas_id,
        ic.company_id,
        ic.atlas_company_id,
        io.atlas_attribute_id AS atlas_custom_attribute_id,
        io.option_id AS atlas_option_id,
        '2025-06-03T00:00:00' AS created_at,
        '2025-06-03T00:00:00' AS updated_at    
    FROM 
        {{ var('source_database') }}."company_hotlist_data" ch
    INNER JOIN 
        internal_companies ic ON ic.company_id = ch.company_slug 
    INNER JOIN 
        internal_options io ON io.external_id = ch.hotlist_id
    WHERE 
        ic.company_id IS NOT NULL
)
SELECT 
    {{ atlas_uuid('company_id::text || atlas_custom_attribute_id::text || atlas_option_id::text') }} AS atlas_id,
    company_id,
    atlas_company_id,
    atlas_custom_attribute_id,
    atlas_option_id,
    created_at,
    updated_at
FROM company_hotlist_results 