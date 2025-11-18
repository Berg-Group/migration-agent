{{ config(
    materialized='table',
    alias='custom_attribute_options_exe',
    tags = ["bullhorn"]
) }}

WITH internal_attributes AS (
    SELECT
        atlas_id,
        alias
    FROM
        {{ ref('1_custom_attributes_exe') }}
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
specialisms AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.skillset, ';', ','), ',,', ','), ',', numbers.n)) AS skillset
    FROM {{ var('source_database') }}.bh_usercontact c
    CROSS JOIN numbers
    WHERE c.skillset IS NOT NULL
        AND TRIM(c.skillset) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.skillset, ';', ','), ',,', ','), ',', numbers.n) != ''
),
company_sectors AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.businesssectorlist, ';', ','), ',,', ','), ',', numbers.n)) AS businesssectorlist
    FROM {{ var('source_database') }}.bh_clientcorporation c
    CROSS JOIN numbers
    WHERE c.businesssectorlist IS NOT NULL 
        AND c.businesssectorlist != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.businesssectorlist, ';', ','), ',,', ','), ',', numbers.n) != ''
),
combined_values AS (
    SELECT 'specialism' AS alias, skillset AS value
    FROM specialisms
    UNION ALL
    SELECT 'company_sector' AS alias, businesssectorlist AS value
    FROM company_sectors
)
SELECT
    cv.alias || '_' || value AS id,
    {{ atlas_uuid("cv.alias || value") }} AS atlas_id,
    ia.atlas_id AS atlas_attribute_id,
    value AS value,
    ROW_NUMBER() OVER (PARTITION BY ia.atlas_id ORDER BY value ASC) AS position,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    combined_values cv
INNER JOIN 
    internal_attributes ia ON ia.alias = cv.alias
ORDER BY
    atlas_attribute_id,
    position