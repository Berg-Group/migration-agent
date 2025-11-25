{{ config(
    materialized='table',
    alias='educations_ja'
) }}

WITH source_data AS (
    SELECT
        candidateeducation.educationid AS id,
        candidateeducation.enddate     AS finished_at,
        candidateeducation.course      AS field_of_study,
        candidateeducation.institution AS name,
        candidateeducation.contactid   AS person_id,
        candidateeducation.educationid::TEXT || '{{ var("clientName") }}' AS uuid_input
    FROM {{ var('source_database') }}."candidateeducation"
),

atlas_person_lookup AS (
    SELECT
        people_ja.id        AS person_id,       -- Match person_id
        people_ja.atlas_id  AS atlas_person_id -- Return atlas_id
    FROM {{ ref('1_people_ja') }}
)

SELECT
    sd.id,
    {{ atlas_uuid('sd.uuid_input') }} AS atlas_id,
    sd.finished_at,
    sd.field_of_study,
    sd.name,
    'migration' AS source,
    sd.person_id,
    apl.atlas_person_id, -- Lookup atlas_person_id from atlas_person_lookup
    CURRENT_TIMESTAMP AS created_at, -- Dynamic ISO8601 timestamp
    CURRENT_TIMESTAMP AS updated_at  -- Dynamic ISO8601 timestamp
FROM
    source_data sd
LEFT JOIN
    atlas_person_lookup apl 
    ON sd.person_id = apl.person_id -- Match person_id to get atlas_person_id
WHERE
    sd.name IS NOT NULL -- Exclude rows without an institution name
    AND TRIM(sd.name) != ''
