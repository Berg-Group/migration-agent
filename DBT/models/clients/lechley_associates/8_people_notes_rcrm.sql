{{ config(
    materialized = 'table',
    alias = 'people_notes_rcrm',
    tags = ["recruit_crm"]
) }}

{% set timestamp_format = 'YYYY-MM-DD"T00:00:00"' %}

WITH internal_persons AS (
    SELECT DISTINCT
        COALESCE(contact_slug, candidate_slug) AS person_id,
        atlas_id AS atlas_person_id
    FROM {{ ref('3_people_rcrm') }}
    WHERE atlas_id IS NOT NULL
),

-- Call logs source
call_logs AS (
    SELECT DISTINCT
        ('call_log_' || COALESCE(related_candidate, related_contact) || '_' || md5(call_notes))::text AS id,
        COALESCE(related_candidate, related_contact)::text AS person_id,
        call_notes::text AS note,
        created_by::text AS created_by_id,
        updated_by::text AS updated_by_id,
        to_char(
            DATE_TRUNC('day', TIMESTAMP 'epoch' + (created_on::bigint) * INTERVAL '1 second'),
            '{{ timestamp_format }}'
        )::text AS created_at,
        to_char(
            DATE_TRUNC('day', TIMESTAMP 'epoch' + (updated_on::bigint) * INTERVAL '1 second'),
            '{{ timestamp_format }}'
        )::text AS updated_at,
        'phone_call'::text AS type
    FROM {{ var('source_database') }}.call_log_data
    WHERE call_notes IS NOT NULL
        AND (related_candidate IS NOT NULL OR related_contact IS NOT NULL)
),

-- Regular notes source
regular_notes AS (
    SELECT DISTINCT
        note_id::text AS id,
        related_to::text AS person_id,
        note::text AS note,
        created_by::text AS created_by_id,
        updated_by::text AS updated_by_id,
        to_char(
            date_trunc('day', timestamp 'epoch' + (created_on::bigint)*interval '1 second'),
            '{{ timestamp_format }}'
        )::text AS created_at,
        to_char(
            date_trunc('day', timestamp 'epoch' + (updated_on::bigint)*interval '1 second'),
            '{{ timestamp_format }}'
        )::text AS updated_at,
        CASE 
            WHEN note_type_id IN (58736, 60192) THEN 'phone_call'
            ELSE 'manual'
        END::text AS type
    FROM {{ var('source_database') }}.note_data
    WHERE note IS NOT NULL
        AND related_to IS NOT NULL
),

-- Combined notes with joins
combined_notes AS (
    SELECT
        n.id,
        {{ atlas_uuid('n.id') }} AS atlas_id,
        {{ clean_html('n.note') }} AS text,
        n.created_at,
        n.updated_at,
        n.created_by_id,
        n.updated_by_id,
        COALESCE(u1.atlas_id, '{{ var("master_id") }}')::text AS created_by_atlas_id,
        COALESCE(u2.atlas_id, '{{ var("master_id") }}')::text AS updated_by_atlas_id,
        '{{ var("agency_id") }}'::text AS agency_id,
        n.type,
        ip.person_id,
        ip.atlas_person_id
    FROM (
        SELECT * FROM call_logs
        UNION ALL
        SELECT * FROM regular_notes
    ) n
    INNER JOIN internal_persons ip 
        ON ip.person_id = n.person_id
    LEFT JOIN {{ ref('user_mapping') }} AS u1
        ON u1.id = n.created_by_id
    LEFT JOIN {{ ref('user_mapping') }} AS u2
        ON u2.id = n.updated_by_id
)

-- Final aggregation to ensure uniqueness
SELECT 
    id,
    MAX(atlas_id)::text as atlas_id,
    MAX(text)::text as text,
    MAX(created_at)::text as created_at,
    MAX(updated_at)::text as updated_at,
    MAX(created_by_id)::text as created_by_id,
    MAX(updated_by_id)::text as updated_by_id,
    MAX(created_by_atlas_id)::text as created_by_atlas_id,
    MAX(updated_by_atlas_id)::text as updated_by_atlas_id,
    MAX(agency_id)::text as agency_id,
    MAX(type)::text as type,
    MAX(person_id)::text as person_id,
    MAX(atlas_person_id)::text as atlas_person_id
FROM combined_notes
GROUP BY id 