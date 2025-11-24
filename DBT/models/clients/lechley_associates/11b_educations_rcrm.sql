{{ config(materialized='table', alias='educations_rcrm') }}

{% set db = var('source_database') %}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id
    FROM 
        {{ ref('3_people_rcrm') }}
),

education_data AS (
    SELECT 
        candidate_slug,
        institute_name,
        educational_qualification,
        educational_specialization,
        education_description,
        education_start_date,
        education_end_date
    FROM 
        {{ ref('11a_educations_concat_rcrm') }}
    WHERE 
        institute_name IS NOT NULL
        AND TRIM(institute_name) <> ''
),

educations_with_ids AS (
    SELECT 
        {{ atlas_uuid("'{{ var(\"clientName\") }}' || COALESCE(ed.candidate_slug, '1') || COALESCE(ed.institute_name, '1') || COALESCE(ed.educational_qualification, '1')") }} AS atlas_id,
        ip.person_id,
        ip.atlas_person_id,
        ed.institute_name AS name,
        ed.educational_qualification AS degree,
        ed.educational_specialization AS field_of_study,
        ed.education_description AS description,
        'migration' AS source,
        ed.education_start_date::DATE AS started_at,
        ed.education_end_date::DATE AS finished_at,
        ROW_NUMBER() OVER (PARTITION BY {{ atlas_uuid("'{{ var(\"clientName\") }}' || COALESCE(ed.candidate_slug, '1') || COALESCE(ed.institute_name, '1') || COALESCE(ed.educational_qualification, '1')") }} ORDER BY ed.education_start_date) AS rn
    FROM 
        education_data ed
    LEFT JOIN 
        internal_persons AS ip
        ON ip.person_id = ed.candidate_slug
)

SELECT 
    atlas_id,
    person_id,
    atlas_person_id,
    name,
    degree,
    field_of_study,
    description,
    source,
    started_at,
    finished_at
FROM 
    educations_with_ids
WHERE 
    rn = 1
