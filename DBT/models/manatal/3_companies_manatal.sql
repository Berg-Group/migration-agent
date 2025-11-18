{{ config(materialized='table', alias='companies_manatal') }}

{% set db = var('source_database') %}

WITH src AS (
    -- Pull raw rows from organisation table
    SELECT
        id AS id,
        {{ atlas_uuid('id') }} AS atlas_id,
        name AS name,
        location,
        description,
        creator_id,
        created_at,
        updated_at
    FROM {{ db }}.organization
),

clean AS (
    SELECT
        src.id,
        src.atlas_id,
        src.name,
        'client' AS relationship,  -- Set all relationships to 'client'
        
        -- Map location field to location_locality
        src.location AS location_locality,
        NULL AS location_name,
        
        -- Map description to summary
        src.description AS summary,
        
        -- Format timestamps to match expected format
        TO_CHAR(
            DATE_TRUNC('day', src.created_at::timestamp),
            'YYYY-MM-DD"T00:00:00"'
        ) AS created_at,
        
        TO_CHAR(
            DATE_TRUNC('day', src.updated_at::timestamp),
            'YYYY-MM-DD"T00:00:00"'
        ) AS updated_at,
        
        -- Map creator_id to created_by_id
        src.creator_id AS created_by_id,
        
        -- Use master_id as fallback if no mapping exists
        COALESCE(u.atlas_id, {{ atlas_uuid("'" ~ var('master_id') ~ "'") }}) AS created_by_atlas_id,
        
        -- No company size available
        NULL AS company_size
    FROM src
    LEFT JOIN {{ ref('user_mapping') }} AS u
        ON u.id = src.creator_id
)

SELECT
    clean.id,
    clean.atlas_id,
    clean.name,
    clean.relationship,
    clean.location_locality,
    clean.location_name,
    clean.summary,
    clean.created_at,
    clean.updated_at,
    clean.created_by_id,
    clean.created_by_atlas_id,
    clean.company_size
FROM clean
