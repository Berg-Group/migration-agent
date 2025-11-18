{{ config(
    materialized='table',
    alias='meetings_ezekia',
    tags = ['ezekia']
) }}

WITH users_map AS (
    SELECT id::text AS user_id, atlas_id AS atlas_user_id
    FROM {{ ref('users_ezekia') }}
),
people_map AS (
    SELECT id::text AS person_id, atlas_id AS atlas_person_id
    FROM {{ ref('people_ezekia') }}
),
base AS (
    SELECT
        m.id,
        m.user_id::text      AS created_by_id,
        LOWER(COALESCE(m.channel_type, '')) AS channel_type,
        m.channel_id::text   AS channel_id,
        m.title,
        m.details,
        m.agenda,
        m.note,
        m.start_date,
        m.start_date_utc,
        m.end_date,
        m.end_date_utc,
        m.created_at,
        m.updated_at
    FROM {{ var('source_database') }}.meetings m
),
direct_person AS (
    SELECT
        b.id,
        b.created_by_id,
        b.title,
        b.details,
        b.agenda,
        b.note,
        b.start_date,
        b.start_date_utc,
        b.end_date,
        b.end_date_utc,
        b.created_at,
        b.updated_at,
        b.channel_id AS person_id
    FROM base b
    WHERE b.channel_type IN ('person','people','candidate','contact')
      AND b.channel_id IS NOT NULL
),
assign_meetings AS (
    SELECT
        b.*,
        COALESCE(b.start_date, b.start_date_utc, b.created_at) AS mt
    FROM base b
    WHERE b.channel_type IN ('assignment','brief','project')
      AND b.channel_id IS NOT NULL
),
assign_candidate_ranked AS (
    SELECT
        am.id                          AS meeting_id,
        am.created_by_id,
        am.title,
        am.details,
        am.agenda,
        am.note,
        am.start_date,
        am.start_date_utc,
        am.end_date,
        am.end_date_utc,
        am.created_at,
        am.updated_at,
        bc.person_id::text             AS person_id,
        ROW_NUMBER() OVER (
            PARTITION BY am.id
            ORDER BY ABS(DATEDIFF(second, am.mt, bc.created_at)) ASC,
                     bc.id DESC
        ) AS rn
    FROM assign_meetings am
    JOIN {{ var('source_database') }}.briefs b
      ON b.id::text = am.channel_id
    JOIN {{ var('source_database') }}.briefs_candidates bc
      ON bc.brief_id = b.id
),
assign_resolved AS (
    SELECT
        meeting_id AS id,
        created_by_id,
        title,
        details,
        agenda,
        note,
        start_date,
        start_date_utc,
        end_date,
        end_date_utc,
        created_at,
        updated_at,
        person_id
    FROM assign_candidate_ranked
    WHERE rn = 1
),
resolved AS (
    SELECT * FROM direct_person
    UNION ALL
    SELECT * FROM assign_resolved
),
meetings_enriched AS (
    SELECT
        r.id,
        {{ atlas_uuid("r.id::varchar") }} AS atlas_id,
        r.person_id,
        r.created_by_id,
        COALESCE(um.atlas_user_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        'meeting' AS action,
        'migrated meeting' AS name,
        REGEXP_REPLACE(COALESCE(r.details, r.agenda, r.note, ''), '<[^>]+>', ' ') AS notes,
        TO_CHAR(COALESCE(r.created_at, r.start_date)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(COALESCE(r.updated_at, r.end_date)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        CASE
            WHEN COALESCE(r.end_date_utc, r.end_date) IS NOT NULL
             AND COALESCE(r.end_date_utc, r.end_date) < GETDATE() THEN 'completed'
            ELSE 'planned'
        END AS status
    FROM resolved r
    LEFT JOIN users_map um ON um.user_id = r.created_by_id
)
SELECT
    me.id,
    me.atlas_id,
    me.person_id,
    pm.atlas_person_id,
    me.created_by_id,
    me.created_by_atlas_id,
    me.action,
    me.notes,
    me.name,
    me.created_at,
    me.updated_at,
    me.status
FROM meetings_enriched me
JOIN people_map pm ON pm.person_id = me.person_id
