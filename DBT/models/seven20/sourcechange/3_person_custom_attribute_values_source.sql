{{ config(
    materialized='table',
    alias='person_custom_attribute_values_source',
    tags = ["seven20"]
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id
    FROM 
        {{ ref('2_people_720') }}
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
    WHERE ca.of = 'person'
),
numbers AS (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
    SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
),
employer_types AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.employer_type2__c, ';', ','), ',,', ','), ',', numbers.n)) AS employer_type,
        c.id AS person_id
    FROM {{ var('source_database') }}.contact c
    CROSS JOIN numbers
    WHERE c.employer_type2__c IS NOT NULL
        AND TRIM(c.employer_type2__c) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.employer_type2__c, ';', ','), ',,', ','), ',', numbers.n) != ''
),
seniorities AS (
    SELECT DISTINCT c.seniority__c AS seniority,
        c.id AS person_id
    FROM {{ var('source_database') }}.contact c
    WHERE c.seniority__c IS NOT NULL AND c.seniority__c != ''
),
candidate_sources AS (
    SELECT DISTINCT c.seven20__candidate_source__c AS candidate_source,
        c.id AS person_id
    FROM {{ var('source_database') }}.contact c
    WHERE c.seven20__candidate_source__c IS NOT NULL AND c.seven20__candidate_source__c != ''
),
candidate_source_detail AS (
    SELECT DISTINCT c.seven20__candidate_source_detail__c AS candidate_source_detail,
        c.id AS person_id
    FROM {{ var('source_database') }}.contact c
    WHERE c.seven20__candidate_source_detail__c IS NOT NULL AND c.seven20__candidate_source_detail__c != ''
),
preferred_work_location AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(REPLACE(REPLACE(c.preferred_work_location__c, ';', ','), ',,', ','), ',', numbers.n)) AS preferred_work_location,
        c.id AS person_id
    FROM {{ var('source_database') }}.contact c
    CROSS JOIN numbers
    WHERE c.preferred_work_location__c IS NOT NULL
        AND TRIM(c.preferred_work_location__c) != ''
        AND SPLIT_PART(REPLACE(REPLACE(c.preferred_work_location__c, ';', ','), ',,', ','), ',', numbers.n) != ''
),
open_to_relocation AS (
    SELECT DISTINCT c.open_to_relocation__c AS open_to_relocation,
        c.id AS person_id
    FROM {{ var('source_database') }}.contact c
    WHERE c.open_to_relocation__c IS NOT NULL AND c.open_to_relocation__c != ''
),
functions AS (
    SELECT DISTINCT s."name" AS function, r.seven20__contact__c AS person_id
    FROM {{ var('source_database') }}.seven20__code__c s 
    INNER JOIN {{ var('source_database') }}.seven20__record_code__c r ON r.seven20__code__c = s.id
    WHERE s.seven20__category__c ILIKE 'FUNC%' 
        AND s.seven20__inactive__c = 0 
        AND r.seven20__contact__c IS NOT NULL
        AND s."name" IS NOT NULL AND s."name" != ''
),
coverages AS (
    SELECT DISTINCT s."name" AS coverage, r.seven20__contact__c AS person_id
    FROM {{ var('source_database') }}.seven20__code__c s 
    INNER JOIN {{ var('source_database') }}.seven20__record_code__c r ON r.seven20__code__c = s.id
    WHERE s.seven20__category__c ILIKE 'COV%' 
        AND s.seven20__inactive__c = 0 
        AND r.seven20__contact__c IS NOT NULL
        AND s."name" IS NOT NULL AND s."name" != ''
),
combined_person_values AS (
    SELECT person_id, 'employer_type' AS attribute_type, employer_type AS value
    FROM employer_types
    UNION ALL
    SELECT person_id, 'seniority' AS attribute_type, seniority AS value
    FROM seniorities
    UNION ALL
    SELECT person_id, 'candidate_source' AS attribute_type, candidate_source AS value
    FROM candidate_sources
    UNION ALL
    SELECT person_id, 'candidate_source_detail' AS attribute_type, candidate_source_detail AS value
    FROM candidate_source_detail
    UNION ALL
    SELECT person_id, 'preferred_work_location' AS attribute_type, preferred_work_location AS value
    FROM preferred_work_location
    UNION ALL
    SELECT person_id, 'open_to_relocation' AS attribute_type, open_to_relocation AS value
    FROM open_to_relocation
    UNION ALL
    SELECT person_id, 'function' AS attribute_type, function AS value
    FROM functions
    UNION ALL
    SELECT person_id, 'coverage' AS attribute_type, coverage AS value
    FROM coverages
)
SELECT DISTINCT
    {{ atlas_uuid('ip.person_id::text || io.atlas_attribute_id::text || io.option_id::text') }} AS atlas_id,
    ip.person_id,
    ip.atlas_person_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    combined_person_values cpv
INNER JOIN 
    internal_persons ip ON ip.person_id = cpv.person_id
INNER JOIN 
    internal_options io ON io.attribute_type = cpv.attribute_type AND io.option_value = cpv.value
ORDER BY
    ip.person_id,
    io.atlas_attribute_id 