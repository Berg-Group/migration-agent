{{ config(
    materialized='table',
    alias='custom_attribute_options_source',
    tags = ["seven20"]
) }}

WITH internal_attributes AS (
    SELECT
        atlas_id,
        alias
    FROM
        {{ ref('1_custom_attributes_source') }}
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
employer_types AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.employer_type2__c, ';', ','), ',,', ','), ',', numbers.n)) AS employer_type
    FROM {{ var('source_database') }}.contact c
    CROSS JOIN numbers
    WHERE c.employer_type2__c IS NOT NULL
        AND TRIM(c.employer_type2__c) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.employer_type2__c, ';', ','), ',,', ','), ',', numbers.n) != ''
),
seniorities AS (
    SELECT DISTINCT c.seniority__c AS seniority
    FROM {{ var('source_database') }}.contact c
    WHERE c.seniority__c IS NOT NULL AND c.seniority__c != ''
),
candidate_sources AS (
    SELECT DISTINCT c.seven20__candidate_source__c AS candidate_source
    FROM {{ var('source_database') }}.contact c
    WHERE c.seven20__candidate_source__c IS NOT NULL AND c.seven20__candidate_source__c != ''
),
candidate_source_detail AS (
    SELECT DISTINCT c.seven20__candidate_source_detail__c AS candidate_source_detail
    FROM {{ var('source_database') }}.contact c
    WHERE c.seven20__candidate_source_detail__c IS NOT NULL AND c.seven20__candidate_source_detail__c != ''
),
preferred_work_location AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.preferred_work_location__c, ';', ','), ',,', ','), ',', numbers.n)) AS preferred_work_location
    FROM {{ var('source_database') }}.contact c
    CROSS JOIN numbers
    WHERE c.preferred_work_location__c IS NOT NULL
        AND TRIM(c.preferred_work_location__c) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.preferred_work_location__c, ';', ','), ',,', ','), ',', numbers.n) != ''
),
open_to_relocation AS (
    SELECT DISTINCT c.open_to_relocation__c AS open_to_relocation
    FROM {{ var('source_database') }}.contact c
    WHERE c.open_to_relocation__c IS NOT NULL AND c.open_to_relocation__c != ''
),
project_employer_type AS (
    SELECT DISTINCT s.employer_type_picklist__c AS project_employer_type
    FROM {{ var('source_database') }}.seven20__job__c s
    WHERE s.employer_type_picklist__c IS NOT NULL AND s.employer_type_picklist__c != ''
),
project_employment_type AS (
    SELECT DISTINCT s.employment_type__c AS project_employment_type
    FROM {{ var('source_database') }}.seven20__job__c s
    WHERE s.employment_type__c IS NOT NULL AND s.employment_type__c != ''
),
project_seniority AS (
    SELECT DISTINCT s.seniority__c AS project_seniority
    FROM {{ var('source_database') }}.seven20__job__c s
    WHERE s.seniority__c IS NOT NULL AND s.seniority__c != ''
),
company_employer_type AS (
    SELECT DISTINCT a.employer_type__c AS company_employer_type
    FROM {{ var('source_database') }}.account a
    WHERE a.employer_type__c IS NOT NULL AND a.employer_type__c != ''
),
functions AS (
    SELECT DISTINCT s."name" AS function
    FROM {{ var('source_database') }}.seven20__code__c s
    WHERE s.seven20__category__c ILIKE 'FUNC%'
        AND s."name" IS NOT NULL AND s."name" != '' AND s.seven20__inactive__c = 0
),
coverages AS (
    SELECT DISTINCT s."name" AS coverage
    FROM {{ var('source_database') }}.seven20__code__c s
    WHERE s.seven20__category__c ILIKE 'COV%'
        AND s."name" IS NOT NULL AND s."name" != '' AND s.seven20__inactive__c = 0
),
project_functions AS (
    SELECT DISTINCT s."name" AS project_function
    FROM {{ var('source_database') }}.seven20__code__c s
    WHERE s.seven20__category__c ILIKE 'FUNC%'
        AND s."name" IS NOT NULL AND s."name" != '' AND s.seven20__inactive__c = 0
),
project_coverages AS (
    SELECT DISTINCT s."name" AS project_coverage
    FROM {{ var('source_database') }}.seven20__code__c s
    WHERE s.seven20__category__c ILIKE 'COV%'
        AND s."name" IS NOT NULL AND s."name" != '' AND s.seven20__inactive__c = 0
),
company_functions AS (
    SELECT DISTINCT s."name" AS company_function
    FROM {{ var('source_database') }}.seven20__code__c s
    WHERE s.seven20__category__c ILIKE 'FUNC%'
        AND s."name" IS NOT NULL AND s."name" != '' AND s.seven20__inactive__c = 0
),
company_coverages AS (
    SELECT DISTINCT s."name" AS company_coverage
    FROM {{ var('source_database') }}.seven20__code__c s
    WHERE s.seven20__category__c ILIKE 'COV%'
        AND s."name" IS NOT NULL AND s."name" != '' AND s.seven20__inactive__c = 0
),
meeting_actions AS (
    SELECT alias, value
    FROM {{ ref('meeting_attributes_720') }}
),
combined_values AS (
    SELECT 'employer_type' AS alias, employer_type AS value
    FROM employer_types
    UNION ALL
    SELECT 'seniority' AS alias, seniority AS value
    FROM seniorities
    UNION ALL
    SELECT 'candidate_source' AS alias, candidate_source AS value
    FROM candidate_sources
    UNION ALL
    SELECT 'candidate_source_detail' AS alias, candidate_source_detail AS value
    FROM candidate_source_detail
    UNION ALL
    SELECT 'preferred_work_location' AS alias, preferred_work_location AS value
    FROM preferred_work_location
    UNION ALL
    SELECT 'open_to_relocation' AS alias, open_to_relocation AS value
    FROM open_to_relocation
    UNION ALL
    SELECT 'project_employer_type' AS alias, project_employer_type AS value
    FROM project_employer_type
    UNION ALL
    SELECT 'project_employment_type' AS alias, project_employment_type AS value
    FROM project_employment_type
    UNION ALL
    SELECT 'project_seniority' AS alias, project_seniority AS value
    FROM project_seniority
    UNION ALL
    SELECT 'company_employer_type' AS alias, company_employer_type AS value
    FROM company_employer_type
    UNION ALL
    SELECT 'function' AS alias, function AS value
    FROM functions
    UNION ALL
    SELECT 'coverage' AS alias, coverage AS value
    FROM coverages
    UNION ALL
    SELECT 'project_function' AS alias, project_function AS value
    FROM project_functions
    UNION ALL
    SELECT 'project_coverage' AS alias, project_coverage AS value
    FROM project_coverages
    UNION ALL
    SELECT 'company_function' AS alias, company_function AS value
    FROM company_functions
    UNION ALL
    SELECT 'company_coverage' AS alias, company_coverage AS value
    FROM company_coverages
    UNION ALL
    SELECT alias, value
    FROM meeting_actions
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