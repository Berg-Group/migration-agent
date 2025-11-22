{{ config(
    materialized='table',
    alias='company_map_files_rect',
    tags=['recruitly']
) }}

SELECT 
    c.id AS company_id,
    c.atlas_id AS atlas_company_id,
    ('attachments/' || a.attachment_name) AS file_name,
    a.attachment_name AS actual_file_name
FROM {{ var('source_database') }}.attachments a 
INNER JOIN {{ ref('4_companies_rect') }} c ON c.id = a.linked_to  
WHERE NULLIF(TRIM(a.attachment_name), '') IS NOT NULL
      AND (
            LOWER(a.attachment_name) LIKE '%.pdf'
         OR LOWER(a.attachment_name) LIKE '%.docx'
         OR LOWER(a.attachment_name) LIKE '%.doc'
      )


