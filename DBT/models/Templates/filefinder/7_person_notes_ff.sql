{{ config(
    materialized='table',
    alias='person_notes_ff',
    tags=["filefinder"]
) }}

WITH progress_codes AS (
    SELECT
        cp.idcandidateprogress AS id,
        cp.value
    FROM {{ var('source_database') }}.candidateprogress cp
    UNION ALL
    SELECT
        ip.idintroductionprogress AS id,
        ip.value
    FROM {{ var('source_database') }}.introductionprogress ip
    UNION ALL
    SELECT
        ap.idassignmentsourceprogress AS id,
        ap.value
    FROM {{ var('source_database') }}.assignmentsourceprogress ap
    UNION ALL
    SELECT
        pp.idpersonreferenceprogress AS id,
        pp.value
    FROM {{ var('source_database') }}.personreferenceprogress pp
),
activity_logs AS (
    SELECT DISTINCT
        e.idactivitylogentity AS id,
        e.idperson AS person_id,
        TRIM(
            CASE 
                WHEN pc.value IS NOT NULL AND TRIM(pc.value) != '' 
                THEN 'Progress: ' || TRIM(pc.value)
                ELSE ''
            END ||
            CASE 
                WHEN (pc.value IS NOT NULL AND TRIM(pc.value) != '')
                     AND (
                         (a.subject IS NOT NULL AND TRIM(a.subject) != '') OR
                         (a.description IS NOT NULL AND TRIM(a.description) != '')
                     )
                THEN CHR(13) || CHR(10)
                ELSE ''
            END ||
            CASE 
                WHEN a.subject IS NOT NULL AND TRIM(a.subject) != '' 
                THEN 'Subject: ' || TRIM(a.subject)
                ELSE ''
            END ||
            CASE 
                WHEN a.subject IS NOT NULL AND TRIM(a.subject) != '' 
                     AND a.description IS NOT NULL AND TRIM(a.description) != ''
                THEN CHR(13) || CHR(10)
                ELSE ''
            END ||
            CASE 
                WHEN a.description IS NOT NULL AND TRIM(a.description) != '' 
                THEN 'Description: ' || TRIM(a.description)
                ELSE ''
            END
        ) AS text,
        TO_CHAR(COALESCE(e.createdon, current_timestamp)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(COALESCE(e.modifiedon, e.createdon, current_timestamp)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        a.createdby
    FROM {{ var('source_database') }}.activitylog a
    INNER JOIN {{ var('source_database') }}.activitylogentity e ON e.idactivitylog = a.idactivitylog
    LEFT JOIN progress_codes pc ON pc.id = a.progressid
    WHERE a.subject NOT IN (
            'Loaded via automatch into target list.', 'List loaded into target list.',
            'Sending letter', 'Addition to target list.', 
            'Status changed in target list.', 'Interview confirmation sent'
        ) 
      AND a.activitytype IN ('FreeEntry','Email', 'Letter','Task','Other','SMS','Fax') 
      AND (
            (a.description IS NOT NULL AND a.description != '' AND LENGTH(a.description) > 30)
            OR (a.subject IS NOT NULL AND a.subject != '' AND LENGTH(a.subject) > 10)
          ) 
      AND e.contextentitytype IN ('Assignment', 'Person') 
),
person_notes AS (
    SELECT 
        p.idperson || 'person_note' AS id,
        p.idperson AS person_id,
        p.note AS text,
        TO_CHAR(p.createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(p.modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        u.name AS createdby
    FROM {{ var('source_database') }}.person p
    LEFT JOIN {{ this.schema }}.users_ff u ON u.id = p.iduser
    WHERE p.note IS NOT NULL AND TRIM(p.note) != ''
),
final_notes AS (
    SELECT * FROM person_notes
    UNION ALL
    SELECT * FROM activity_logs
)
SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    text,
    type,
    created_at,
    updated_at,
    created_by_id,
    created_by_atlas_id
FROM (
    SELECT
        a.id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || a.id::varchar") }} AS atlas_id,
        a.person_id,
        p.atlas_id AS atlas_person_id,
        regexp_replace(
                a.text,
                '<[^>]+>',
                ' ',
                1,
                'i'
        ) AS text,
        'manual' AS type,
        a.created_at,
        a.updated_at,
        uf.id AS created_by_id,
        COALESCE(uf.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.id) AS rn
    FROM final_notes a
    INNER JOIN {{ ref('2_people_ff') }} p ON p.id = a.person_id
    LEFT JOIN {{ this.schema }}.users_ff uf ON LOWER(uf.name) = LOWER(a.createdby)
) final
WHERE rn = 1