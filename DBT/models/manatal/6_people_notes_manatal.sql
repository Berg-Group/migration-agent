{{ config(materialized='table', alias='people_notes_manatal') }}

{% set db = var('source_database') %}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id,
        candidate_id,
        contact_id
    FROM
        {{ ref('1_people_manatal') }}
),

candidate_notes AS (
    -- Extract notes from candidate_note table
    SELECT
        cn.id AS id,
        cn.candidate_id,
        cn.info AS text, -- Use info column directly
        cn.created_at,
        cn.updated_at,
        cn.creator_id AS created_by_id,
        cn.creator_id AS updated_by_id,
        'manual' AS type -- Default to manual type for candidate notes
    FROM 
        {{ db }}.candidate_note cn
),

contact_notes AS (
    -- Extract notes from contact_note table
    SELECT
        ctn.id AS id,
        ctn.contact_id,
        ctn.info AS text, -- Use info column directly
        ctn.created_at,
        ctn.updated_at,
        ctn.creator_id AS created_by_id,
        ctn.creator_id AS updated_by_id,
        'manual' AS type -- Default to manual type for contact notes
    FROM 
        {{ db }}.contact_note ctn
),

all_notes AS (
    -- Combine notes and link to people through candidate_id
    SELECT
        'cn' || cn.id AS note_id,
        cn.text,
        cn.created_at,
        cn.updated_at,
        cn.created_by_id,
        cn.updated_by_id,
        cn.type,
        ip.person_id,
        ip.atlas_person_id
    FROM 
        candidate_notes cn
    JOIN 
        internal_persons ip
        ON ip.candidate_id = cn.candidate_id
    WHERE 
        cn.text IS NOT NULL
        AND TRIM(cn.text) <> ''
    
    UNION ALL
    
    -- Combine notes and link to people through contact_id
    SELECT
        'ctn' || ctn.id AS note_id,
        ctn.text,
        ctn.created_at,
        ctn.updated_at,
        ctn.created_by_id,
        ctn.updated_by_id,
        ctn.type,
        ip.person_id,
        ip.atlas_person_id
    FROM 
        contact_notes ctn
    JOIN 
        internal_persons ip
        ON ip.contact_id = ctn.contact_id
    WHERE 
        ctn.text IS NOT NULL
        AND TRIM(ctn.text) <> ''
)

SELECT 
    note_id AS id,
    {{ atlas_uuid('note_id') }} AS atlas_id,
    text,
    TO_CHAR(DATE_TRUNC('day', created_at::timestamp), 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(DATE_TRUNC('day', updated_at::timestamp), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    created_by_id,
    updated_by_id,
    COALESCE(u.atlas_id, {{ atlas_uuid("'" ~ var('master_id') ~ "'") }}) AS created_by_atlas_id,
    COALESCE(u2.atlas_id, {{ atlas_uuid("'" ~ var('master_id') ~ "'") }}) AS updated_by_atlas_id,
    type,
    person_id,
    atlas_person_id
FROM 
    all_notes an
LEFT JOIN 
    {{ ref('user_mapping') }} AS u
    ON u.id = an.created_by_id
LEFT JOIN 
    {{ ref('user_mapping') }} AS u2 
    ON u2.id = an.updated_by_id
WHERE 
    atlas_person_id IS NOT NULL 