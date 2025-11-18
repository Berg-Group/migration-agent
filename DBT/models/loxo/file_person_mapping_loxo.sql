{{ config(
    materialized = 'table',
    alias        = 'file_person_mapping_loxo',
    tags         = ['loxo']
) }}

WITH resumes AS (
    SELECT
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        r."path" AS file_name,
        r.filename AS actual_file_name,
        'resume' AS type
    FROM {{ var('source_database') }}.people_resumes r
    INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = r.root_id
    WHERE r."path" != ''
      AND (
            LOWER(r.filename) LIKE '%.pdf'
         OR LOWER(r.filename) LIKE '%.docx'
         OR LOWER(r.filename) LIKE '%.doc'
      )
),
activities_docs AS (
    SELECT
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        'people/' || ad."path" AS file_name,
        ad.filename AS actual_file_name,
        CASE
            WHEN LOWER(ad.filename) LIKE '%resume%' OR LOWER(ad.filename) LIKE '%cv%' THEN 'resume'
            ELSE 'other'
        END AS type
    FROM {{ var('source_database') }}.activities_documents ad
    INNER JOIN {{ var('source_database') }}.activities a ON a.id = ad.root_id
    INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = a.person
    WHERE ad."path" != ''
      AND (
            LOWER(ad.filename) LIKE '%.pdf'
         OR LOWER(ad.filename) LIKE '%.docx'
         OR LOWER(ad.filename) LIKE '%.doc'
      )
),
people_docs AS (
    SELECT
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        pd."path" AS file_name,
        pd.filename AS actual_file_name,
        CASE
            WHEN LOWER(pd.filename) LIKE '%resume%' OR LOWER(pd.filename) LIKE '%cv%' THEN 'resume'
            ELSE 'other'
        END AS type
    FROM {{ var('source_database') }}.people_documents pd
    INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = pd.root_id
    WHERE pd."path" != ''
      AND (
            LOWER(pd.filename) LIKE '%.pdf'
         OR LOWER(pd.filename) LIKE '%.docx'
         OR LOWER(pd.filename) LIKE '%.doc'
      )
)
SELECT *
FROM resumes
UNION ALL
SELECT *
FROM activities_docs
UNION ALL
SELECT *
FROM people_docs