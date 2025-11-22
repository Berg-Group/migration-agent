{{ config(
    materialized='table',
    alias='file_person_mapping_rect',
    tags=['recruitly']
) }}

WITH pd AS (
    SELECT 
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        ('internalcvs/' || c.internal_cv_file_name) AS file_name,
        c.internal_cv_file_name AS actual_file_name,
        CASE
            WHEN LOWER(c.internal_cv_file_name) LIKE '%resume%' OR LOWER(c.internal_cv_file_name) LIKE '%cv%'
                OR LOWER(c.internal_cv_file_name) LIKE '%profile%' THEN 'resume'
            ELSE 'other'
        END AS type
    FROM {{ var('source_database') }}.candidates c 
    INNER JOIN {{ ref('2_people_rect') }} p ON p.id = c.candidate_id 
    WHERE NULLIF(TRIM(c.internal_cv_file_name), '') IS NOT NULL
      AND (
            LOWER(c.internal_cv_file_name) LIKE '%.pdf'
         OR LOWER(c.internal_cv_file_name) LIKE '%.docx'
         OR LOWER(c.internal_cv_file_name) LIKE '%.doc'
      )

    UNION ALL

    SELECT 
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        ('attachments/' || a.attachment_name) AS file_name,
        a.attachment_name AS actual_file_name,
        CASE
            WHEN LOWER(a.attachment_name) LIKE '%resume%' OR LOWER(a.attachment_name) LIKE '%cv%'
                OR LOWER(a.attachment_name) LIKE '%profile%' THEN 'resume'
            ELSE 'other'
        END AS type
    FROM {{ var('source_database') }}.attachments a 
    INNER JOIN {{ ref('2_people_rect') }} p ON p.id = a.linked_to  
    WHERE NULLIF(TRIM(a.attachment_name), '') IS NOT NULL
      AND (
            LOWER(a.attachment_name) LIKE '%.pdf'
         OR LOWER(a.attachment_name) LIKE '%.docx'
         OR LOWER(a.attachment_name) LIKE '%.doc'
      )
)
SELECT 
    person_id,
    atlas_person_id,
    file_name,
    actual_file_name,
    type
FROM pd


