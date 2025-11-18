{{ config(
    materialized='table',
    alias='company_custom_attribute_values_clarity',
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
        cao.id AS external_id,
        ca.alias AS attribute_type,
        cao.value AS option_value
    FROM 
        {{ ref('custom_attribute_options_clarity') }} cao
    INNER JOIN 
        {{ ref('custom_attributes_clarity') }} ca ON ca.atlas_id = cao.atlas_attribute_id
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
specialization_mappings AS (
    SELECT DISTINCT
        cd.slug AS company_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(cd.specialization, ';', ','), ',,', ','), ',', numbers.n)) AS specialization_value
    FROM {{ var('source_database') }}.company_data cd
    CROSS JOIN numbers
    WHERE cd.specialization IS NOT NULL
        AND TRIM(cd.specialization) != ''
        AND SPLIT_PART(REPLACE(REPLACE(cd.specialization, ';', ','), ',,', ','), ',', numbers.n) != ''
),
sub_industry_mappings AS (
    SELECT DISTINCT
        cd.slug AS company_id,
        TRIM(SPLIT_PART(REPLACE(REPLACE(cd.sub_industry, ';', ','), ',,', ','), ',', numbers.n)) AS sub_industry_value
    FROM {{ var('source_database') }}.company_data cd
    CROSS JOIN numbers
    WHERE cd.sub_industry IS NOT NULL
        AND TRIM(cd.sub_industry) != ''
        AND SPLIT_PART(REPLACE(REPLACE(cd.sub_industry, ';', ','), ',,', ','), ',', numbers.n) != ''
),
combined_mappings AS (
    SELECT 
        company_id,
        'Specialization' AS attribute_type,
        specialization_value AS value
    FROM specialization_mappings
    UNION ALL
    SELECT 
        company_id,
        'Sub Industry' AS attribute_type,
        sub_industry_value AS value
    FROM sub_industry_mappings
)
SELECT DISTINCT
    {{ atlas_uuid('ic.company_id::text || io.atlas_attribute_id::text || io.option_id::text') }} AS atlas_id,
    ic.company_id,
    ic.atlas_company_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    '2025-06-03T00:00:00' AS created_at,
    '2025-06-03T00:00:00' AS updated_at
FROM 
    combined_mappings cm
INNER JOIN 
    internal_companies ic ON ic.company_id = cm.company_id
INNER JOIN 
    internal_options io ON io.attribute_type = cm.attribute_type AND io.option_value = cm.value
ORDER BY
    company_id,
    atlas_custom_attribute_id 