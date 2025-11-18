{{ config(
    materialized='table',
    alias='educations_ezekia'
) }}

WITH people_map AS (
    SELECT
        p.id,
        p.atlas_id AS atlas_person_id
    FROM {{ ref('people_ezekia') }} p
),

source_data AS (
    SELECT
        e.id,
        {{atlas_uuid('e.id')}} AS atlas_id,
        TO_CHAR(e.created_at::TIMESTAMP, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(e.updated_at::TIMESTAMP, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        e.person_id,
        'migration' AS source,
        CASE
            WHEN e.start = 0 THEN NULL
            ELSE e.start::text || '-01-01'
        END AS started_at,
        CASE
            WHEN e.end = 0 THEN NULL
            ELSE e.end::text || '-01-01'
        END AS finished_at,
        e.degree,
        e.field AS field_of_study,
        e.school AS name,
        COALESCE(education_searchable, e.summary) AS description
    FROM {{ var("source_database") }}.people_educations e
),

joined AS (
    SELECT
        sd.id,
        sd.atlas_id,
        sd.created_at,
        sd.updated_at,
        sd.person_id,
        pm.atlas_person_id,
        sd.source,
        sd.started_at,
        sd.finished_at,
        sd.degree,
        sd.field_of_study,
        sd.name,
        sd.description
    FROM source_data sd
    INNER JOIN people_map pm
           ON sd.person_id = pm.id
)

SELECT
    id,
    atlas_id,
    created_at,
    updated_at,
    person_id,
    atlas_person_id,
    source,
    started_at,
    finished_at,
    degree,
    field_of_study,
    name,
    description
FROM joined
WHERE 
    NULLIF(TRIM(name),'') NOTNULL