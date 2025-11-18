{{ config(
    materialized='table',
    alias='experiences_720',
    tags=["seven20"]
) }}

WITH source_experiences AS (
    SELECT 
        e.id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || e.id") }} AS atlas_id,
        e.seven20__job_title__c AS title,
        e.seven20__account__c AS company_name,
        regexp_replace(
                e.seven20__description__c,
                '<[^>]+>',
                ' ',
                1,
                'i'
            ) AS description,
        TO_CHAR(e.seven20__start_date__c::timestamp(0), 'YYYY-MM-DD') AS started_at,
        TO_CHAR(e.seven20__end_date__c::timestamp(0), 'YYYY-MM-DD') AS finished_at,
        TO_CHAR(e.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(e.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        'migration' AS source,
        e.seven20__candidate__c AS person_id,
        c.id AS company_id,
        c.atlas_id AS atlas_company_id,
        '{{ var('agency_id') }}' AS agency_id
    FROM 
        {{ var('source_database') }}.seven20__employment_history__c e
    LEFT JOIN 
        {{ ref('4_companies_720') }} c ON LOWER(c.name) = LOWER(e.seven20__account__c)
    WHERE
        (e.seven20__account__c IS NOT NULL AND e.seven20__account__c != '')
        AND (e.seven20__job_title__c IS NOT NULL AND e.seven20__job_title__c != '')
        AND (e.seven20__start_date__c IS NOT NULL AND e.seven20__start_date__c != '')
        AND (e.isdeleted = 0)
),
regular_experiences AS (
    SELECT
        se.id,
        se.atlas_id,
        se.title,
        se.company_name,
        se.description,
        se.started_at,
        se.finished_at,
        se.created_at,
        se.updated_at,
        se.source,
        se.person_id,
        p.atlas_id AS atlas_person_id,
        se.company_id,
        se.atlas_company_id,
        se.agency_id
    FROM source_experiences se
    INNER JOIN {{ ref('2_people_720') }} p ON p.id = se.person_id
),
dupe_people_experiences AS (
    SELECT
        se.id,
        se.atlas_id,
        se.title,
        se.company_name,
        se.description,
        se.started_at,
        se.finished_at,
        se.created_at,
        se.updated_at,
        se.source,
        se.person_id,
        p.atlas_id AS atlas_person_id,
        se.company_id,
        se.atlas_company_id,
        se.agency_id
    FROM source_experiences se
    INNER JOIN {{ ref('people_dupes_720') }} pd ON pd.contact_id = se.person_id
    INNER JOIN {{ ref('2_people_720') }} p ON p.id = pd.candidate_id
)
SELECT
    id,
    atlas_id,
    title,
    company_name,
    description,
    started_at,
    finished_at,
    created_at,
    updated_at,
    source,
    person_id,
    atlas_person_id,
    company_id,
    atlas_company_id,
    agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM (
        SELECT * FROM regular_experiences
        UNION ALL
        SELECT * FROM dupe_people_experiences
    ) combined
) final
WHERE rn = 1