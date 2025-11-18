{{ config(
    materialized='table',
    alias='custom_attribute_options_inte',
    tags = ["bullhorn"]
) }}

WITH internal_attributes AS (
    SELECT
        atlas_id,
        alias
    FROM
        {{ ref('1_custom_attributes_inte') }}
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
pronouns AS (
    SELECT DISTINCT
        TRIM(c.customtext14) AS pronouns
    FROM {{ var('source_database') }}.bh_usercontact c
    WHERE c.customtext14 IS NOT NULL
        AND TRIM(c.customtext14) != ''
        AND TRIM(c.customtext14) != '0'
),
primary_skills AS (
    SELECT DISTINCT
        CASE
            WHEN l.categoryid = 2000027 THEN 'Energy - ' || TRIM(s.name)
            WHEN l.categoryid = 2000012 THEN 'Mobile - ' || TRIM(s.name)
            WHEN l.categoryid = 2000018 THEN 'Data - ' || TRIM(s.name)
            WHEN l.categoryid IN (2000013, 2000028) THEN 'BD - ' || TRIM(s.name)
            WHEN l.categoryid = 2000030 THEN 'Health Care - ' || TRIM(s.name)
            ELSE TRIM(s.name)
        END AS primary_skills
    FROM {{ var('source_database') }}.bh_categoryskillassociation c
    INNER JOIN {{ var('source_database') }}.bh_skilllist s ON s.skillid = c.skillid
    INNER JOIN {{ var('source_database') }}.bh_categorylist l ON l.categoryid = c.categoryid
    WHERE l.categoryid IN (2000027, 2000012, 2000018, 2000013, 2000028, 2000030)
        AND s.name IS NOT NULL
        AND TRIM(s.name) != ''
),
education_levels AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.degreelist, ';', ','), ',,', ','), ',', numbers.n)) AS education_levels
    FROM {{ var('source_database') }}.bh_usercontact c
    CROSS JOIN numbers
    WHERE c.degreelist IS NOT NULL
        AND TRIM(c.degreelist) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.degreelist, ';', ','), ',,', ','), ',', numbers.n) != ''
),
current_locations AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(p.desiredlocations, ';', ','), ',,', ','), ',', numbers.n)) AS current_locations
    FROM {{ var('source_database') }}.bh_usercontact p
    CROSS JOIN numbers
    WHERE p.desiredlocations IS NOT NULL
        AND TRIM(p.desiredlocations) != ''
        AND SPLIT_PART(REPLACE(REPLACE(p.desiredlocations, ';', ','), ',,', ','), ',', numbers.n) != ''
),
work_authorities AS (
    SELECT DISTINCT
        TRIM(c.employeetype) AS work_authorities
    FROM {{ var('source_database') }}.bh_candidate c
    WHERE c.employeetype IS NOT NULL
        AND TRIM(c.employeetype) NOT IN ('', '"To Re-qualify"')
),
standard_job_titles AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(f.valuelist, ';', ','), ',,', ','), ',', numbers.n)) AS standard_job_titles
    FROM {{ var('source_database') }}.bh_fieldmaplist f
    CROSS JOIN numbers
    WHERE f.display ILIKE '%standard job titl%'
        AND f.isrequired = 1
        AND f.valuelist IS NOT NULL
        AND TRIM(f.valuelist) != ''
        AND SPLIT_PART(REPLACE(REPLACE(f.valuelist, ';', ','), ',,', ','), ',', numbers.n) != ''
),
visa_sponsor_transfer AS (
    SELECT 'Yes' AS visa_sponsor_transfer
    UNION ALL
    SELECT 'No' AS visa_sponsor_transfer
),
account_managers AS (
    SELECT DISTINCT
        TRIM(c.customtextblock2) AS account_managers
    FROM {{ var('source_database') }}.bh_clientcorporation c
    WHERE c.customtextblock2 IS NOT NULL
        AND TRIM(c.customtextblock2) != ''
),
combined_values AS (
    SELECT 'pronouns' AS alias, pronouns AS value
    FROM pronouns
    UNION ALL
    SELECT 'primary_skill' AS alias, primary_skills AS value
    FROM primary_skills
    UNION ALL
    SELECT 'education_level' AS alias, education_levels AS value
    FROM education_levels
    UNION ALL
    SELECT 'current_location' AS alias, current_locations AS value
    FROM current_locations
    UNION ALL
    SELECT 'work_authority' AS alias, work_authorities AS value
    FROM work_authorities
    UNION ALL
    SELECT 'standard_job_title' AS alias, standard_job_titles AS value
    FROM standard_job_titles
    UNION ALL
    SELECT 'visa_sponsor_transfer' AS alias, visa_sponsor_transfer AS value
    FROM visa_sponsor_transfer
    UNION ALL
    SELECT 'account_manager' AS alias, account_managers AS value
    FROM account_managers
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