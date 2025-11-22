{{ config(
    materialized='table',
    alias='person_custom_attribute_values_rcrm',
    tags = ["recruit_crm"]
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id,
        candidate_slug,
        contact_slug
    FROM 
        {{ ref('people_rcrm') }}
),  
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        cao.atlas_id AS option_id,
        cao.id AS external_id
    FROM 
        {{ ref('custom_attribute_options_rcrm') }} cao
    INNER JOIN 
        {{ ref('custom_attributes_rcrm') }} ca ON ca.atlas_id = cao.atlas_attribute_id
    WHERE 
        ca.name IN ('Candidate Hotlist', 'Contact Hotlist')
),
candidate_hotlist_results AS (
    SELECT 
        {{ atlas_uuid('ch.candidate_slug') }} AS atlas_id,
        ip.person_id,
        ip.atlas_person_id,
        io.atlas_attribute_id AS atlas_custom_attribute_id,
        io.option_id AS atlas_option_id,
        '2025-06-03T00:00:00' AS created_at,
        '2025-06-03T00:00:00' AS updated_at    
    FROM 
        {{ var('source_database') }}."candidate_hotlist_data" ch
    INNER JOIN 
        internal_persons ip ON ip.candidate_slug = ch.candidate_slug 
    INNER JOIN 
        internal_options io ON io.external_id = ch.hotlist_id
    WHERE 
        ip.candidate_slug IS NOT NULL
),
contact_hotlist_results AS (
    SELECT 
        {{ atlas_uuid('ch.contact_slug') }} AS atlas_id,
        ip.person_id,
        ip.atlas_person_id,
        io.atlas_attribute_id AS atlas_custom_attribute_id,
        io.option_id AS atlas_option_id,
        '2025-06-03T00:00:00' AS created_at,
        '2025-06-03T00:00:00' AS updated_at    
    FROM 
        {{ var('source_database') }}."contact_hotlist_data" ch
    INNER JOIN 
        internal_persons ip ON ip.contact_slug = ch.contact_slug 
    INNER JOIN 
        internal_options io ON io.external_id = ch.hotlist_id
    WHERE 
        ip.contact_slug IS NOT NULL
),
combined_results AS (
    SELECT * FROM candidate_hotlist_results
    UNION ALL
    SELECT * FROM contact_hotlist_results
)
SELECT 
    {{ atlas_uuid('person_id::text || atlas_custom_attribute_id::text || atlas_option_id::text') }} AS atlas_id,
    person_id,
    atlas_person_id,
    atlas_custom_attribute_id,
    atlas_option_id,
    created_at,
    updated_at
FROM combined_results
