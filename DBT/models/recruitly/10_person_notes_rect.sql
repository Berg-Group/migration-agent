{{ config(
    materialized='table',
    alias='person_notes_rect',
    tags=['recruitly']
) }}

WITH source_candidates AS (
    SELECT
        c.candidate_id AS person_id,
        p.atlas_id AS atlas_person_id,
        c.owner_id AS created_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        {{ string_to_timestamp('c.createdon') }} AS created_at,
        {{ string_to_timestamp('c.modifiedon') }} AS updated_at,
        NULLIF(TRIM({{ clean_html('c.preferences') }}), '') AS pref_text,
        NULLIF(TRIM({{ clean_html('c.reasons_for_leaving') }}), '') AS reasons_text,
        NULLIF(TRIM({{ clean_html('c.internal_notes') }}), '') AS internal_text
    FROM {{ var('source_database') }}.candidates c
    INNER JOIN {{ ref('2_people_rect') }} p ON p.id = c.candidate_id
    LEFT JOIN {{ ref('1_users_rect') }} u ON u.id = c.owner_id
),
notes AS (
    SELECT
        ('rect_pref_' || person_id::text) AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || person_id::text || '::preferences'") }} AS atlas_id,
        person_id,
        atlas_person_id,
        created_by_id,
        created_by_atlas_id,
        'Preferences:' || chr(13) || chr(10) || pref_text AS text,
        created_at,
        updated_at,
        'manual' AS type
    FROM source_candidates
    WHERE pref_text IS NOT NULL

    UNION ALL

    SELECT
        ('rect_reasons_' || person_id::text) AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || person_id::text || '::reasons_for_leaving'") }} AS atlas_id,
        person_id,
        atlas_person_id,
        created_by_id,
        created_by_atlas_id,
        'Reasons for leaving:' || chr(13) || chr(10) || reasons_text AS text,
        created_at,
        updated_at,
        'manual' AS type
    FROM source_candidates
    WHERE reasons_text IS NOT NULL

    UNION ALL

    SELECT
        ('rect_internal_' || person_id::text) AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || person_id::text || '::internal_notes'") }} AS atlas_id,
        person_id,
        atlas_person_id,
        created_by_id,
        created_by_atlas_id,
        'Internal notes:' || chr(13) || chr(10) || internal_text AS text,
        created_at,
        updated_at,
        'manual' AS type
    FROM source_candidates
    WHERE internal_text IS NOT NULL
)
SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    created_by_id,
    created_by_atlas_id,
    text,
    created_at,
    updated_at,
    type
FROM notes