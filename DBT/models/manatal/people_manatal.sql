{{ config(materialized='table', alias='people_manatal_') }}

{% set db = var('source_database') %}


SELECT
    id,
    {{atlas_uuid('id')}} AS atlas_id,
    split_part(full_name, ' ', 1) AS first_name,
    split_part(full_name, ' ', 2) AS last_name,
    to_char(created_at, 'YYYY-MM-DD"T00:00:00"') AS created_at,
    to_char(updated_at, 'YYYY-MM-DD"T00:00:00"') AS uppdated_at,
    creator_id AS created_by,
    '{{var('created_by_id')}}' AS created_by_atlas_id,
    creator_id AS updated_by,    
    '{{var('created_by_id')}}'  AS updated_by_atlas_id,
    'active'  AS responsiveness,
    'regular' AS candidate_status,
    city AS location_locality,
    country     AS location_country,
    '{{var('agency_id')}}' AS agency_id
    FROM {{ db }}.candidate
