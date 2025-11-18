{{ config(
    materialized='table',
    alias='educations_720',
    tags=["seven20"]
) }}

WITH source_educations AS (
    SELECT 
        e.id,
    	{{ atlas_uuid("'" ~ var('clientName') ~ "' || e.id") }} AS atlas_id,
    	e.seven20__school_name__c AS name,
        e.seven20__degree_subject__c AS field_of_study,
        e.seven20__measure_value__c AS grade,
        regexp_replace(
            e.seven20__comments__c,
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS description,
        TO_CHAR(e.seven20__start_date__c::timestamp(0), 'YYYY-MM-DD') AS started_at,
        TO_CHAR(COALESCE(e.seven20__end_date__c, e.seven20__graduation_date__c)::timestamp(0), 'YYYY-MM-DD') AS finished_at,
        TO_CHAR(e.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(e.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    	'migration' AS source,
        e.seven20__candidate__c AS person_id,
        '{{ var('agency_id') }}' AS agency_id
    FROM 
    	{{ var('source_database') }}."seven20__education_history__c" e
    WHERE e.isdeleted = 0 
        AND e.seven20__school_name__c IS NOT NULL 
        AND e.seven20__school_name__c != ''
),
regular_educations AS (
    SELECT
        se.id,
        se.atlas_id,
        se.name,
        se.field_of_study,
        se.grade,
        se.description,
        se.started_at,
        se.finished_at,
        se.created_at,
        se.updated_at,
        se.source,
        se.person_id,
        p.atlas_id AS atlas_person_id,
        se.agency_id
    FROM source_educations AS se
    INNER JOIN {{ ref('2_people_720') }} p ON p.id = se.person_id
),
dupe_people_educations AS (
    SELECT
        se.id,
        se.atlas_id,
        se.name,
        se.field_of_study,
        se.grade,
        se.description,
        se.started_at,
        se.finished_at,
        se.created_at,
        se.updated_at,
        se.source,
        se.person_id,
        p.atlas_id AS atlas_person_id,
        se.agency_id
    FROM source_educations AS se
    INNER JOIN {{ ref('people_dupes_720') }} pd ON pd.contact_id = se.person_id
    INNER JOIN {{ ref('2_people_720') }} p ON p.id = pd.candidate_id
)
SELECT
    id,
    atlas_id,
    name,
    field_of_study,
    grade,
    description,
    started_at,
    finished_at,
    created_at,
    updated_at,
    source,
    person_id,
    atlas_person_id,
    agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM (
        SELECT * FROM regular_educations
        UNION ALL
        SELECT * FROM dupe_people_educations
    ) combined
) final
WHERE rn = 1