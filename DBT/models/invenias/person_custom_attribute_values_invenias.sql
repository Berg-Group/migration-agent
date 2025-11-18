{{ config(
    materialized='table',
    alias='person_custom_attribute_values_invenias'
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id 
    FROM 
        "{{this.schema}}"."people_invenias"
),
internal_options AS (
    SELECT 
        atlas_attribute_id,
        atlas_id AS option_id,
        id AS external_id
    FROM 
        "{{this.schema}}"."custom_attribute_options_invenias"

)
SELECT
    {{ atlas_uuid('rp.relationid') }} AS atlas_id,
    ip.person_id,
    ip.atlas_person_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    '2025-03-27T00:00:00' AS created_at,
    '2025-03-27T00:00:00' AS updated_at
FROM 
    {{ var('source_database') }}."relation_persontocategorylistentry" rp  
INNER JOIN 
    {{ var('source_database') }}."categorylistentries" c ON c.itemid = rp.categorylistentryid
INNER JOIN 
    internal_persons ip ON ip.person_id = rp.person
INNER JOIN 
    internal_options io ON io.external_id = c.itemid