{{ config(materialized='table', alias='people_manatal') }}

{% set db = var('source_database') %}

WITH matched_profiles AS (
    -- Find matches between candidates and contacts with normalization for trailing slashes
    SELECT
        ct.id AS contact_id,
        cs.candidate_id
    FROM {{ db }}.contact ct
    JOIN {{ db }}.candidate_social cs
    ON rtrim(ct.linkedin_url, '/') = rtrim(cs.social_media_url, '/')
    WHERE ct.linkedin_url IS NOT NULL
      AND cs.social_media_url IS NOT NULL
),

candidate_people AS (
    -- Process candidates
    SELECT
        c.id AS id, 
        c.id AS candidate_id,
        mp.contact_id,
        
        -- Extract first name (first word) and last name (rest of the name)
        -- Remove content in brackets if present
        SPLIT_PART(
            CASE 
                WHEN POSITION('(' IN REGEXP_REPLACE(c.full_name, '\\(.*\\)', '')) > 0 
                THEN SUBSTRING(REGEXP_REPLACE(c.full_name, '\\(.*\\)', ''), 1, POSITION('(' IN REGEXP_REPLACE(c.full_name, '\\(.*\\)', ''))-1)
                ELSE REGEXP_REPLACE(c.full_name, '\\(.*\\)', '')
            END,
            ' ', 1
        ) AS first_name,
        
        SUBSTRING(
            CASE 
                WHEN POSITION('(' IN REGEXP_REPLACE(c.full_name, '\\(.*\\)', '')) > 0 
                THEN SUBSTRING(REGEXP_REPLACE(c.full_name, '\\(.*\\)', ''), 1, POSITION('(' IN REGEXP_REPLACE(c.full_name, '\\(.*\\)', ''))-1)
                ELSE REGEXP_REPLACE(c.full_name, '\\(.*\\)', '')
            END,
            LENGTH(SPLIT_PART(
                CASE 
                    WHEN POSITION('(' IN REGEXP_REPLACE(c.full_name, '\\(.*\\)', '')) > 0 
                    THEN SUBSTRING(REGEXP_REPLACE(c.full_name, '\\(.*\\)', ''), 1, POSITION('(' IN REGEXP_REPLACE(c.full_name, '\\(.*\\)', ''))-1)
                    ELSE REGEXP_REPLACE(c.full_name, '\\(.*\\)', '')
                END,
                ' ', 1
            )) + 2
        ) AS last_name,

        {{ atlas_uuid('c.id') }} AS atlas_id,

        -- Format timestamps to match expected format
        TO_CHAR(
            DATE_TRUNC('day', c.created_at::timestamp),
            'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(
            DATE_TRUNC('day', c.updated_at::timestamp),
            'YYYY-MM-DD"T00:00:00"') AS updated_at,

        -- Handle creator_id for candidate
        c.creator_id AS created_by_id,
        COALESCE(u.atlas_id, {{ atlas_uuid('c.creator_id') }}) AS created_by_atlas_id,
        c.creator_id AS updated_by_id,
        COALESCE(u.atlas_id, {{ atlas_uuid('c.creator_id') }}) AS updated_by_atlas_id,

        'active' AS responsiveness,
        'regular' AS candidate_status,

        -- Location information
        '' AS location_locality,
        c.city AS location_metro,
        c.country AS location_country

    FROM {{ db }}.candidate c
    LEFT JOIN matched_profiles mp
        ON mp.candidate_id = c.id
    LEFT JOIN {{ ref('user_mapping') }} AS u
        ON u.id = c.creator_id
),

contacts_only AS (
    -- Process contacts that don't match to candidates
    SELECT
        'cc' || ct.id AS id,
        NULL AS candidate_id,
        ct.id AS contact_id,
        
        -- Extract first name (first word) and last name (rest of the name)
        -- Remove content in brackets if present
        SPLIT_PART(
            CASE 
                WHEN POSITION('(' IN REGEXP_REPLACE(ct.full_name, '\\(.*\\)', '')) > 0 
                THEN SUBSTRING(REGEXP_REPLACE(ct.full_name, '\\(.*\\)', ''), 1, POSITION('(' IN REGEXP_REPLACE(ct.full_name, '\\(.*\\)', ''))-1)
                ELSE REGEXP_REPLACE(ct.full_name, '\\(.*\\)', '')
            END,
            ' ', 1
        ) AS first_name,
        
        SUBSTRING(
            CASE 
                WHEN POSITION('(' IN REGEXP_REPLACE(ct.full_name, '\\(.*\\)', '')) > 0 
                THEN SUBSTRING(REGEXP_REPLACE(ct.full_name, '\\(.*\\)', ''), 1, POSITION('(' IN REGEXP_REPLACE(ct.full_name, '\\(.*\\)', ''))-1)
                ELSE REGEXP_REPLACE(ct.full_name, '\\(.*\\)', '')
            END,
            LENGTH(SPLIT_PART(
                CASE 
                    WHEN POSITION('(' IN REGEXP_REPLACE(ct.full_name, '\\(.*\\)', '')) > 0 
                    THEN SUBSTRING(REGEXP_REPLACE(ct.full_name, '\\(.*\\)', ''), 1, POSITION('(' IN REGEXP_REPLACE(ct.full_name, '\\(.*\\)', ''))-1)
                    ELSE REGEXP_REPLACE(ct.full_name, '\\(.*\\)', '')
                END,
                ' ', 1
            )) + 2
        ) AS last_name,

        {{ atlas_uuid("'cc' || ct.id") }} AS atlas_id,

        -- Format timestamps
        TO_CHAR(
            DATE_TRUNC('day', ct.created_at::timestamp),
            'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(
            DATE_TRUNC('day', ct.updated_at::timestamp),
            'YYYY-MM-DD"T00:00:00"') AS updated_at,

        -- Use master_id for contacts since they don't have creator_id
        '{{ var("master_id") }}' AS created_by_id,
        {{ atlas_uuid("'" ~ var('master_id') ~ "'") }} AS created_by_atlas_id,
        '{{ var("master_id") }}' AS updated_by_id,
        {{ atlas_uuid("'" ~ var('master_id') ~ "'") }} AS updated_by_atlas_id,

        'active' AS responsiveness,
        'regular' AS candidate_status,

        -- Location information
        ct.location AS location_locality,
        '' AS location_metro,
        '' AS location_country

    FROM {{ db }}.contact ct
    LEFT JOIN matched_profiles mp
        ON mp.contact_id = ct.id
    WHERE mp.candidate_id IS NULL
),

combined_people AS (
    -- Combine candidates and contacts with explicit column list
    SELECT
        id,
        candidate_id,
        contact_id,
        first_name,
        last_name,
        atlas_id,
        created_at,
        updated_at,
        created_by_id,
        created_by_atlas_id,
        updated_by_id,
        updated_by_atlas_id,
        responsiveness,
        candidate_status,
        location_locality,
        location_metro,
        location_country
    FROM candidate_people

    UNION ALL

    SELECT
        id,
        candidate_id,
        contact_id,
        first_name,
        last_name,
        atlas_id,
        created_at,
        updated_at,
        created_by_id,
        created_by_atlas_id,
        updated_by_id,
        updated_by_atlas_id,
        responsiveness,
        candidate_status,
        location_locality,
        location_metro,
        location_country
    FROM contacts_only
)

-- Final output with location combining logic
SELECT
    id,
    candidate_id,
    contact_id,
    first_name,
    last_name,
    atlas_id,
    created_at,
    updated_at,
    created_by_id,
    created_by_atlas_id,
    updated_by_id,
    updated_by_atlas_id,
    responsiveness,
    candidate_status,
    
    -- Combine location fields if locality is empty
    CASE
        WHEN COALESCE(location_locality, '') = ''
        THEN TRIM(BOTH ' ' FROM (COALESCE(location_metro, '') || ' ' || COALESCE(location_country, '')))
        ELSE location_locality
    END AS location_locality,
    
    location_metro,
    location_country

FROM combined_people