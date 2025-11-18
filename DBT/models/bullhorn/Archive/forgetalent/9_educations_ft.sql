{{ config(
    materialized = 'table',
    alias = 'educations_ft',
    tags=["bullhorn"]
) }}

WITH source_educations AS (
    SELECT
        s.usereducationid AS id,
        LEFT(s.startdate, 10) AS started_at,
        LEFT(s.enddate, 10) AS finished_at,
        s.school AS name,
        s.degree AS degree,
        s.major AS field_of_study,
        s.gpa AS grade,
        s.userid AS person_id
    FROM {{ var('source_database') }}.bh_usereducation s
    WHERE s.school IS NOT NULL AND s.school != ''
),
regular_educations AS (
    SELECT
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || se.id") }} AS atlas_id,
        se.id,
        'migration' AS source,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS updated_at,
        se.started_at,
        se.finished_at,
        se.name,
        se.degree,
        se.field_of_study,
        se.grade,
        se.person_id,
        p.atlas_id AS atlas_person_id
    FROM source_educations AS se
    INNER JOIN {{ ref('1_people_ft') }} p ON p.id = se.person_id
),
dupe_people_educations AS (
    SELECT
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || se.id") }} AS atlas_id,
        se.id,
        'migration' AS source,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00Z"') AS updated_at,
        se.started_at,
        se.finished_at,
        se.name,
        se.degree,
        se.field_of_study,
        se.grade,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id
    FROM source_educations AS se
    INNER JOIN {{ ref('people_dupes_bh') }} pd ON pd.contact_id = se.person_id
    INNER JOIN {{ ref('1_people_ft') }} p ON p.id = pd.candidate_id
)
SELECT
    id,
    atlas_id,
    created_at,
    updated_at,
    source,
    started_at,
    finished_at,
    name,
    degree,
    field_of_study,
    grade,
    person_id,
    atlas_person_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM (
        SELECT * FROM dupe_people_educations
        UNION ALL
        SELECT * FROM regular_educations
    ) combined
) final
WHERE rn = 1