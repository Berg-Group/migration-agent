{{ config(
    materialized='table',
    alias='custom_attribute_options_clarity',
    tags = ["recruit_crm"]
) }}

WITH internal_attributes AS (
    SELECT
        atlas_id,
        alias
    FROM
        {{ ref('custom_attributes_clarity') }}
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
specializations AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(cd.specialization, ';', ','), ',,', ','), ',', numbers.n)) AS specialization
    FROM {{ var('source_database') }}.company_data cd
    CROSS JOIN numbers
    WHERE cd.specialization IS NOT NULL
        AND TRIM(cd.specialization) != ''
        AND SPLIT_PART(REPLACE(REPLACE(cd.specialization, ';', ','), ',,', ','), ',', numbers.n) != ''
),
sub_industries AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(cd.sub_industry, ';', ','), ',,', ','), ',', numbers.n)) AS sub_industry
    FROM {{ var('source_database') }}.company_data cd
    CROSS JOIN numbers
    WHERE cd.sub_industry IS NOT NULL
        AND TRIM(cd.sub_industry) != ''
        AND SPLIT_PART(REPLACE(REPLACE(cd.sub_industry, ';', ','), ',,', ','), ',', numbers.n) != ''
),
combined_values AS (
    SELECT 'Specialization' AS entity_name, specialization AS value
    FROM specializations
    WHERE specialization != ''
    UNION ALL
    SELECT 'Sub Industry' AS entity_name, sub_industry AS value
    FROM sub_industries
    WHERE sub_industry != ''
)
SELECT
    entity_name || '_' || value AS id,
    {{ atlas_uuid("entity_name || value") }} AS atlas_id,
    ia.atlas_id AS atlas_attribute_id,
    value AS value,
    ROW_NUMBER() OVER (PARTITION BY ia.atlas_id ORDER BY value ASC) AS position,
    '2025-06-03T00:00:00' AS created_at,
    '2025-06-03T00:00:00' AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    combined_values cv
INNER JOIN 
    internal_attributes ia ON ia.alias = cv.entity_name
ORDER BY
    atlas_attribute_id,
    position