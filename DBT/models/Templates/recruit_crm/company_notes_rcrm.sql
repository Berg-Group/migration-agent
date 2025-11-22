{{ config(materialized='table', alias='company_notes_rcrm') }}

{% set db = var('source_database') %}

WITH internal_companies AS (
SELECT 
    id AS company_id,
    atlas_id AS atlas_company_id
FROM 
    {{ref('companies_rcrm')}}
)

SELECT 
    note_id AS id,
    {{ atlas_uuid('note_id') }} AS atlas_id,
    {{ clean_html('nd.note') }} AS text,
    to_char(date_trunc('day', timestamp 'epoch' + (created_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"') AS created_at,
    to_char(date_trunc('day', timestamp 'epoch' + (updated_on::bigint)*interval '1 second'), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    nd.created_by AS created_by_id,
    nd.updated_by AS updated_by_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
    COALESCE(u2.atlas_id, '{{ var("master_id") }}') AS updated_by_atlas_id,
    CASE WHEN note_type_id IN (58736, 60192) THEN 'phone' ELSE 'manual' END AS type,
    ic.company_id,
    ic.atlas_company_id    
FROM 
    {{ db }}."note_data" nd
LEFT JOIN internal_companies ic 
    ON ic.company_id = nd.related_to
LEFT JOIN 
    {{ ref('user_mapping') }} AS u
    ON u.id = nd.created_by
LEFT JOIN 
    {{ ref('user_mapping') }} AS u2 
    ON u2.id = nd.updated_by
WHERE ic.atlas_company_id NOTNULL