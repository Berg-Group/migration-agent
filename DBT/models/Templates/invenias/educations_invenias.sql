{{ config(
    materialized = 'table',
    alias = 'educations_invenias',
    tags=["invenias"]
) }}
with internal_persons AS (
SELECT
    DISTINCT id AS person_id,
    atlas_id AS atlas_person_id
FROM
    {{ref('people_invenias')}}
)

SELECT 
    itemid AS id, 
    {{atlas_uuid("itemid")}} AS atlas_id,
    startdate::DATE AS started_at,
    enddate::DATE AS finished_at,
    subject AS field, 
    placeofstudy AS name,
    'migration' as source,
    pe.personid AS person_id,
    ip.atlas_person_id 
FROM 
    {{ var('source_database') }}."personeducation" p 
LEFT JOIN 
     {{ var('source_database') }}."relation_persontoeducation" pe ON pe.personeducationid = p.itemid 
INNER JOIN 
    internal_persons ip ON ip.person_id = pe.personid
WHERE startdate::date notnull
AND NULLIF(TRIM(placeofstudy),'') notnull