{{ config(
    materialized='table',
    alias='educations_ff',
    tags=["filefinder"]
) }}

WITH source_educations AS (
    SELECT 
        e.idEducation AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || e.idEducation::text") }} AS atlas_id,
        p.idPerson AS person_id,
        pf.atlas_id AS atlas_person_id,
        e.EducationSubject AS field_of_study,
        q.Value AS degree,
        e.EducationEstablishment AS name,
        e.notes AS description,
        'migration' AS source,
        CASE 
            WHEN e.EducationFrom IS NOT NULL THEN TO_CHAR(e.EducationFrom::timestamp(0), 'YYYY-MM-DD')
            ELSE NULL
        END AS started_at,
        CASE 
            WHEN e.EducationTo IS NOT NULL THEN TO_CHAR(e.EducationTo::timestamp(0), 'YYYY-MM-DD')
            ELSE NULL
        END AS finished_at,
        TO_CHAR(e.createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(e.modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.Education e
    INNER JOIN {{ var('source_database') }}.Person p ON p.idPerson = e.idPerson
    INNER JOIN {{ this.schema }}.people_ff pf ON pf.id = p.idPerson
    LEFT JOIN {{ var('source_database') }}.Qualification q ON q.idQualification = e.idQualification
    WHERE (e.EducationEstablishment IS NOT NULL AND e.EducationEstablishment <> '')
)
SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    field_of_study,
    degree,
    name,
    description,
    started_at,
    finished_at,
    created_at,
    updated_at,
    source,
    agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY created_at DESC
        ) AS rn
    FROM source_educations
) deduped
WHERE rn = 1