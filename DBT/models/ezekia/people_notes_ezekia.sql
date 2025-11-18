{{ config(
    materialized='table',
    alias='person_notes_ezekia'
) }}

WITH people_map AS (
    SELECT p.id, p.atlas_id
    FROM {{ ref('people_ezekia') }} p
),

user_map AS (
    SELECT u.id, u.atlas_id AS created_by_atlas_id
    FROM {{ ref('users_ezekia') }} u
),

source_notes AS (
    SELECT
        n.id,
        n.created_at,
        n.updated_at,
        n.is_automated,
        n.notable_type,
        n.notable_id,
        n.text,
        n.user_id
    FROM {{ var("source_database") }}.notes n
    WHERE n.is_automated = 0
      AND n.notable_type = 'person'
      AND LOWER(n.text) != 'added to sourcewhale'
),

processed AS (
    SELECT
        sn.id,
        {{ atlas_uuid('sn.id::text') }} AS atlas_id,
        TO_CHAR(sn.created_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(sn.updated_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        {{ clean_html('sn.text') }} AS text,
        sn.notable_id AS person_id,
        pm.atlas_id   AS atlas_person_id,
        sn.user_id    AS created_by_id,
        COALESCE(um.created_by_atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        'manual' AS type
    FROM source_notes sn
    INNER JOIN people_map pm ON sn.notable_id = pm.id
    LEFT JOIN user_map um ON sn.user_id = um.id
),

outlook_emails AS (
    SELECT
        oe.id,
        oe.user_id,
        oe."from"    AS from_addr,
        oe."to"      AS to_addr,
        oe.subject   AS subject,
        oe.text      AS body_text,
        oe.raw_html  AS body_html,
        oe.created_at,
        oe.updated_at
    FROM {{ var("source_database") }}.outlook_emails oe
),

outlook_emailables AS (
    SELECT
        oea.person_id,
        oea.outlook_email_id
    FROM {{ var("source_database") }}.outlook_emailables oea
),

emails_processed AS (
    SELECT
        oe.id,
        {{ atlas_uuid('oe.id::text || oea.person_id::text') }} AS atlas_id,
        TO_CHAR(oe.created_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(COALESCE(oe.updated_at, oe.created_at)::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        (
            'From: ' || COALESCE(oe.from_addr,'') ||
            ' | To: ' || COALESCE(oe.to_addr,'') ||
            CASE WHEN oe.subject IS NOT NULL AND oe.subject <> '' THEN ' — ' || oe.subject ELSE '' END || ' ' ||
            {{ clean_html("COALESCE(oe.body_text, oe.body_html, '')") }}
        ) AS text,
        oea.person_id AS person_id,
        pm.atlas_id   AS atlas_person_id,
        oe.user_id    AS created_by_id,
        COALESCE(um.created_by_atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        'manual' AS type
    FROM outlook_emails oe
    JOIN outlook_emailables oea ON oea.outlook_email_id = oe.id
    JOIN people_map pm ON pm.id = oea.person_id
    LEFT JOIN user_map um ON um.id = oe.user_id
)

SELECT
    id,
    atlas_id,
    created_at,
    updated_at,
    text,
    person_id,
    atlas_person_id,
    created_by_id,
    created_by_atlas_id,
    type
FROM processed

UNION ALL

SELECT
    id,
    atlas_id,
    created_at,
    updated_at,
    text,
    person_id,
    atlas_person_id,
    created_by_id,
    created_by_atlas_id,
    type
FROM emails_processed
WHERE NULLIF(TRIM(text), '') IS NOT NULL
