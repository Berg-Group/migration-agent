{{ config(materialized='table', alias='company_notes_invenias') }}

{% set db = var('source_database') %}

WITH internal_companies AS (
SELECT 
    id AS company_id,
    atlas_id AS atlas_company_id
FROM 
    {{ref('companies_invenias')}}
)

SELECT 
    "itemid" AS id,
    {{ atlas_uuid('atlas_company_id || internalcomments') }} AS atlas_id,
    {{ clean_html('internalcomments') }} AS text,
    TO_CHAR(c."datecreated"::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS')         AS created_at,
    TO_CHAR(c."datemodified"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS')         AS updated_at,
    c.creatorid AS created_by_id,
    c.modifierid AS updated_by_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
    COALESCE(u2.atlas_id, '{{ var("master_id") }}') AS updated_by_atlas_id,
    'manual' AS type,
    ic.company_id,
    ic.atlas_company_id    
FROM 
    {{db}}."companies" c
LEFT JOIN internal_companies ic 
    ON ic.company_id = c."itemid"
LEFT JOIN {{ ref('users_invenias') }} u
  ON u."id" = c."creatorid"
LEFT JOIN {{ ref('users_invenias') }} u2
  ON u2."id" = c."modifierid"
WHERE TRIM(internalcomments) <> '' AND internalcomments NOTNULL