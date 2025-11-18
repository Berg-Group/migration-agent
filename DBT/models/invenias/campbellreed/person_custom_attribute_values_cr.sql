{{ config(
    materialized='table',
    alias='person_custom_attribute_values_cr'
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id 
    FROM 
        {{ref('people_invenias')}}
),
internal_options AS (
    SELECT 
        atlas_attribute_id,
        atlas_id AS option_id,
        id AS external_id
    FROM 
        {{ref('custom_attribute_options_cr')}}

)
SELECT
    {{ atlas_uuid('rp.relationid') }} AS atlas_id,
    ip.person_id,
    ip.atlas_person_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(rp.datecreated::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(rp.datemodified::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
FROM 
    {{ var('source_database') }}."relation_persontocategorylistentry" rp  
INNER JOIN 
    {{ var('source_database') }}."categorylistentries" c ON c.itemid = rp.categorylistentryid
INNER JOIN 
    internal_persons ip ON ip.person_id = rp.person
INNER JOIN 
    internal_options io ON io.external_id = c.itemid