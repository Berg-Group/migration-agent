{{ config(
    materialized='table',
    alias='custom_attribute_options_bh',
    tags = ["bullhorn"]
) }}

WITH internal_attributes AS (
    SELECT
        atlas_id,
        alias
    FROM
        {{ ref('14_custom_attributes_bh') }}
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
genders AS (
    SELECT DISTINCT c.gender
    FROM {{ var('source_database') }}.bh_usercontact c
    WHERE c.gender IS NOT NULL AND c.gender != ''
),
job_titles AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.customtext2, ';', ','), ',,', ','), ',', numbers.n)) AS customtext2
    FROM {{ var('source_database') }}.bh_usercontact c
    CROSS JOIN numbers
    WHERE c.customtext2 IS NOT NULL
        AND TRIM(c.customtext2) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.customtext2, ';', ','), ',,', ','), ',', numbers.n) != ''
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
desired_locations AS (
    SELECT DISTINCT c.desiredlocations
    FROM {{ var('source_database') }}.bh_usercontact c
    WHERE c.desiredlocations IS NOT NULL AND c.desiredlocations != ''
),
business_areas AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.division, ';', ','), ',,', ','), ',', numbers.n)) AS division
    FROM {{ var('source_database') }}.bh_client c
    CROSS JOIN numbers
    WHERE c.division IS NOT NULL
        AND TRIM(c.division) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.division, ';', ','), ',,', ','), ',', numbers.n) != ''
),
project_job_titles AS (
    SELECT DISTINCT p.customText1
    FROM {{ var('source_database') }}.bh_jobopportunity p
    WHERE LENGTH(TRIM(p.customText1)) > 1
),
project_specialisms AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(p.customText2, ';', ','), ',,', ','), ',', numbers.n)) AS customText2
    FROM {{ var('source_database') }}.bh_jobopportunity p
    CROSS JOIN numbers
    WHERE p.customText2 IS NOT NULL
        AND TRIM(p.customText2) != ''
        AND SPLIT_PART(REPLACE(REPLACE(p.customText2, ';', ','), ',,', ','), ',', numbers.n) != ''
),
company_types AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.businesssectorlist, ';', ','), ',,', ','), ',', numbers.n)) AS companytype
    FROM {{ var('source_database') }}.bh_clientcorporation c
    CROSS JOIN numbers
    WHERE c.businesssectorlist IS NOT NULL
        AND TRIM(c.businesssectorlist) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.businesssectorlist, ';', ','), ',,', ','), ',', numbers.n) != ''
),
combined_values AS (
    SELECT 'gender' AS alias, gender AS value
    FROM genders
    UNION ALL
    SELECT 'job_title' AS alias, customtext2 AS value
    FROM job_titles
    UNION ALL
    SELECT 'specialism' AS alias, skillset AS value
    FROM specialisms
    UNION ALL
    SELECT 'desired_location' AS alias, desiredlocations AS value
    FROM desired_locations
    UNION ALL
    SELECT 'business_area' AS alias, division AS value
    FROM business_areas
    UNION ALL
    SELECT 'project_job_title' AS alias, customText1 AS value
    FROM project_job_titles
    UNION ALL
    SELECT 'project_specialism' AS alias, customText2 AS value
    FROM project_specialisms
    UNION ALL
    SELECT 'company_type' AS alias, companytype AS value
    FROM company_types
)
SELECT
    cv.alias || '_' || value AS id,
    {{ atlas_uuid("cv.alias || value") }} AS atlas_id,
    ia.atlas_id AS atlas_attribute_id,
    value AS value,
    ROW_NUMBER() OVER (PARTITION BY ia.atlas_id ORDER BY value ASC) AS position,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    combined_values cv
INNER JOIN 
    internal_attributes ia ON ia.alias = cv.alias
ORDER BY
    atlas_attribute_id,
    position