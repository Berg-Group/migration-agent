{{ config(
    materialized = 'table',
    alias        = 'file_person_mapping_ft',
    tags         = ['bullhorn']
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id
    FROM 
        {{ ref('1_people_ft') }}
)
SELECT 
    ip.person_id,
    ip.atlas_person_id,
    f.userworkfileid || f.fileextension AS file_location,
    f.userworkfileid || f.fileextension AS coded_file_name,
    w."name" AS actual_file_name,
    CASE
        WHEN LOWER(w."type") LIKE '%resume%' OR LOWER(w."type") LIKE '%cv%' OR 
            LOWER(w."name") LIKE '%resume%' THEN 'resume'
        ELSE 'other'
    END AS type,
    '{{ var("agency_id") }}' AS agency_id
FROM {{ var('source_database') }}.bh_userworkfile f
INNER JOIN {{ var('source_database') }}.bh_userwork w ON w.userworkid = f.userworkid 
INNER JOIN internal_persons ip ON ip.person_id = w.userid