
{{ config(materialized = 'table', alias = 'person_notes_headstart') }}

{% set src = var('source_database') %}

WITH internal_persons AS (
    SELECT  
        DISTINCT person_id,
        prod_person_id AS atlas_person_id
    FROM
        "{{this.schema}}"."person_prod_mapping"
)

SELECT 
    {{atlas_uuid('ip.person_id || c.note')}}        AS atlas_id,
    {{clean_html("c.note")}}                        AS text,
    note_by                                         AS created_by_id,
    '{{var("master_id")}}'                          AS created_by_atlas_id,
    'manual'                                        AS type,
    '{{ var("agency_id") }}'                        AS agency_id,
    'migration'                                     AS source,
    TO_CHAR(COALESCE(note_on, current_date), 'YYYY-MM-DD"T00:00:00"')       AS created_at,
    TO_CHAR(COALESCE(note_on, current_date), 'YYYY-MM-DD"T00:00:00"')       AS updated_at, 
    c.id                                            AS person_id,
    ip.atlas_person_id   
    
FROM
    {{src}}.candidate c
INNER JOIN internal_persons ip ON ip.person_id = c.id 
WHERE 
    TRIM({{clean_html("c.note")}} ) <> ''
    AND note IS NOT NULL 
