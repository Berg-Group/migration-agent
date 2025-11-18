{{ config(
    materialized='table',
    alias='project_custom_attribute_values_source',
    tags = ["seven20"]
) }}

WITH internal_projects AS (
    SELECT 
        id AS project_id,
        atlas_id AS atlas_project_id
    FROM 
        {{ ref('10_projects_720') }}
),  
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        cao.atlas_id AS option_id,
        cao.id AS external_id,
        ca.alias AS attribute_type,
        cao.value AS option_value
    FROM 
        {{ ref('2_custom_attribute_options_source') }} cao
    INNER JOIN 
        {{ ref('1_custom_attributes_source') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'project'
),
project_employer_type AS (
    SELECT DISTINCT s.employer_type_picklist__c AS project_employer_type,
        s.id AS project_id
    FROM {{ var('source_database') }}.seven20__job__c s
    WHERE s.employer_type_picklist__c IS NOT NULL AND s.employer_type_picklist__c != ''
),
project_employment_type AS (
    SELECT DISTINCT s.employment_type__c AS project_employment_type,
        s.id AS project_id
    FROM {{ var('source_database') }}.seven20__job__c s
    WHERE s.employment_type__c IS NOT NULL AND s.employment_type__c != ''
),
project_seniority AS (
    SELECT DISTINCT s.seniority__c AS project_seniority,
        s.id AS project_id
    FROM {{ var('source_database') }}.seven20__job__c s
    WHERE s.seniority__c IS NOT NULL AND s.seniority__c != ''
),
project_functions AS (
    SELECT DISTINCT s."name" AS project_function, r.seven20__job__c AS project_id
    FROM {{ var('source_database') }}.seven20__code__c s 
    INNER JOIN {{ var('source_database') }}.seven20__record_code__c r ON r.seven20__code__c = s.id
    WHERE s.seven20__category__c ILIKE 'FUNC%' 
        AND s.seven20__inactive__c = 0 
        AND r.seven20__job__c IS NOT NULL
        AND s."name" IS NOT NULL AND s."name" != ''
    UNION ALL
    SELECT DISTINCT s."name" AS project_function, r.seven20__job_lead__c AS project_id
    FROM {{ var('source_database') }}.seven20__code__c s 
    INNER JOIN {{ var('source_database') }}.seven20__record_code__c r ON r.seven20__code__c = s.id
    WHERE s.seven20__category__c ILIKE 'FUNC%' 
        AND s.seven20__inactive__c = 0 
        AND r.seven20__job_lead__c IS NOT NULL
        AND s."name" IS NOT NULL AND s."name" != ''
),
project_coverages AS (
    SELECT DISTINCT s."name" AS project_coverage, r.seven20__job__c AS project_id
    FROM {{ var('source_database') }}.seven20__code__c s 
    INNER JOIN {{ var('source_database') }}.seven20__record_code__c r ON r.seven20__code__c = s.id
    WHERE s.seven20__category__c ILIKE 'COV%' 
        AND s.seven20__inactive__c = 0 
        AND r.seven20__job__c IS NOT NULL
        AND s."name" IS NOT NULL AND s."name" != ''
    UNION ALL
    SELECT DISTINCT s."name" AS project_coverage, r.seven20__job_lead__c AS project_id
    FROM {{ var('source_database') }}.seven20__code__c s 
    INNER JOIN {{ var('source_database') }}.seven20__record_code__c r ON r.seven20__code__c = s.id
    WHERE s.seven20__category__c ILIKE 'COV%' 
        AND s.seven20__inactive__c = 0 
        AND r.seven20__job_lead__c IS NOT NULL
        AND s."name" IS NOT NULL AND s."name" != ''
),
combined_mappings AS (
    SELECT 
        project_id,
        'project_employer_type'::text AS attribute_type,
        project_employer_type AS value
    FROM project_employer_type
    UNION ALL
    SELECT 
        project_id,
        'project_employment_type'::text AS attribute_type,
        project_employment_type AS value
    FROM project_employment_type
    UNION ALL
    SELECT 
        project_id,
        'project_seniority'::text AS attribute_type,
        project_seniority AS value
    FROM project_seniority
    UNION ALL
    SELECT 
        project_id,
        'project_function'::text AS attribute_type,
        project_function AS value
    FROM project_functions
    UNION ALL
    SELECT 
        project_id,
        'project_coverage'::text AS attribute_type,
        project_coverage AS value
    FROM project_coverages
)
SELECT DISTINCT
    {{ atlas_uuid('ip.project_id::text || io.atlas_attribute_id::text || io.option_id::text') }} AS atlas_id,
    ip.project_id,
    ip.atlas_project_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    combined_mappings cm
INNER JOIN 
    internal_projects ip ON ip.project_id = cm.project_id
INNER JOIN 
    internal_options io ON io.attribute_type = cm.attribute_type AND io.option_value = cm.value
ORDER BY
    project_id,
    atlas_custom_attribute_id 