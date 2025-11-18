{{ config(
    materialized='table',
    alias='person_notes_720',
    tags=["seven20"]
) }}

WITH base AS (
    SELECT 
        t.id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || t.id") }} AS atlas_id,
        t.whoid AS person_id,
        t.createdbyid AS created_by_id,
        regexp_replace(
            t.description,
            '<[^>]+>',
            ' ',
            1,
            'i'
        ) AS text,
        'manual' AS type,
        TO_CHAR(t.createddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(t.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
    FROM 
        {{ var('source_database') }}."task" t
    WHERE t.subject NOT ILIKE '%email%'
        AND t.description IS NOT NULL AND t.description NOT ILIKE '%to:%cc:%' and LENGTH(t.description) > 30
        AND t.subject NOT IN {{ get_agency_filter('meetings') }}
),
regular_notes AS (
    SELECT
        b.id,
        b.atlas_id,
        b.person_id,
        p.atlas_id AS atlas_person_id,
        b.created_by_id,
        COALESCE(um.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        b.text,
        b.type,
        b.created_at,
        b.updated_at
    FROM base b
    INNER JOIN {{ ref('2_people_720') }} p  ON b.person_id = p.id
    LEFT JOIN {{ ref('1_users_720') }} um ON b.created_by_id = um.id
),
dupe_people_notes AS (
    SELECT
        b.id,
        b.atlas_id,
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        b.created_by_id,
        COALESCE(um.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        b.text,
        b.type,
        b.created_at,
        b.updated_at
    FROM base b
    INNER JOIN {{ ref('people_dupes_720') }} pd ON pd.contact_id = b.person_id
    INNER JOIN {{ ref('2_people_720') }} p ON p.id = pd.candidate_id
    LEFT JOIN {{ ref('1_users_720') }} um ON um.id = b.created_by_id
)
SELECT 
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    created_by_id,
    created_by_atlas_id,
    text,
    type,
    created_at,
    updated_at
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM (
        SELECT * FROM regular_notes
        UNION ALL
        SELECT * FROM dupe_people_notes
    ) combined
) final
WHERE rn = 1