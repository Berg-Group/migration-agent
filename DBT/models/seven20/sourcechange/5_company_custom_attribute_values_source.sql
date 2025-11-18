{{ config(
    materialized='table',
    alias='company_custom_attribute_values_source',
    tags = ["seven20"]
) }}

WITH internal_companies AS (
    SELECT 
        id AS company_id,
        atlas_id AS atlas_company_id
    FROM 
        {{ ref('4_companies_720') }}
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
    WHERE ca.of = 'company'
),
company_employer_type AS (
    SELECT DISTINCT a.employer_type__c AS company_employer_type, a.id AS company_id
    FROM {{ var('source_database') }}.account a
    WHERE a.employer_type__c IS NOT NULL AND a.employer_type__c != ''
),
company_functions AS (
    SELECT DISTINCT s."name" AS company_function, r.seven20__account__c AS company_id
    FROM {{ var('source_database') }}.seven20__code__c s
    INNER JOIN {{ var('source_database') }}.seven20__record_code__c r ON r.seven20__code__c = s.id
    WHERE s.seven20__category__c ILIKE 'FUNC%'
        AND s.seven20__inactive__c = 0 
        AND r.seven20__account__c IS NOT NULL
        AND s."name" IS NOT NULL AND s."name" != ''
),
company_coverages AS (
    SELECT DISTINCT s."name" AS company_coverage, r.seven20__account__c AS company_id
    FROM {{ var('source_database') }}.seven20__code__c s
    INNER JOIN {{ var('source_database') }}.seven20__record_code__c r ON r.seven20__code__c = s.id
    WHERE s.seven20__category__c ILIKE 'COV%'
        AND s.seven20__inactive__c = 0 
        AND r.seven20__account__c IS NOT NULL
        AND s."name" IS NOT NULL AND s."name" != ''
),
combined_mappings AS (
    SELECT 
        company_id,
        'company_employer_type'::text AS attribute_type,
        company_employer_type AS value
    FROM company_employer_type
    UNION ALL
    SELECT 
        company_id,
        'company_function'::text AS attribute_type,
        company_function AS value
    FROM company_functions
    UNION ALL
    SELECT 
        company_id,
        'company_coverage'::text AS attribute_type,
        company_coverage AS value
    FROM company_coverages
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