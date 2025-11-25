{{ config(
    materialized = 'table',
    alias        = 'person_notes_ft'
) }}

WITH base AS (
    SELECT
        uc.usercommentid AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || uc.usercommentid::varchar") }} AS atlas_id,
        uc.commentinguserid AS created_by_id,
        uc.userid AS person_id,
        regexp_replace(
            uc.comments,
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS text,
        uc.action AS action,
        'manual' AS type,
        TO_CHAR(uc.dateadded::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
        TO_CHAR(uc.dateadded::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS updated_at
    FROM {{ var('source_database') }}."bh_usercomment" uc
    WHERE uc.comments IS NOT NULL AND uc.comments != ''
        AND LOWER(TRIM(uc.action)) NOT IN ('mail merge', 'linkedin inmail', 'email/linkedin message', 'email updated', 'out-bound email')
        AND LOWER(TRIM(uc.action)) NOT IN {{ get_agency_filter('meetings') }}
),
regular_notes AS (
    SELECT
        b.id,
        b.atlas_id,
        b.person_id,
        p.atlas_id AS atlas_person_id,
        b.created_by_id,
        COALESCE(um.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        b.action,
        b.text,
        b.created_at,
        b.updated_at,
        b.type
    FROM base b
    INNER JOIN {{ ref('1_people_ft') }} p  ON b.person_id = p.id
    LEFT JOIN {{ ref('users_ft') }} um ON b.created_by_id = um.id
),
dupe_people_notes AS (
    SELECT
        b.id,
        b.atlas_id,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        b.created_by_id,
        COALESCE(um.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        b.action,
        b.text,
        b.created_at,
        b.updated_at,
        b.type
    FROM base b
    INNER JOIN {{ ref('people_dupes_bh') }} pd ON pd.contact_id = b.person_id
    INNER JOIN {{ ref('1_people_ft') }} p ON p.id = pd.candidate_id
    LEFT JOIN {{ ref('users_ft') }} um ON um.id = b.created_by_id
)
SELECT 
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    created_by_id,
    created_by_atlas_id,
    action,
    text,
    created_at,
    updated_at,
    type
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn
    FROM (
        SELECT * FROM regular_notes
        UNION ALL
        SELECT * FROM dupe_people_notes
    ) combined
) final
WHERE rn = 1