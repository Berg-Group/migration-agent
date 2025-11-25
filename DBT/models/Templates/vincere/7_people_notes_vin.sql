{{ config(materialized = 'table', alias = 'people_notes_vincere') }}

{% set src = var('source_database') %}

WITH staged AS (
    SELECT
        pa.id::varchar                                 AS act_id,
        regexp_replace(pa.content,'<[^>]*>',' ')        AS content_plain,
        pa.insert_timestamp,
        pa.candidate_id::varchar                       AS candidate_id,
        pa.contact_id::varchar                         AS contact_id,
        pa.user_account_id::varchar                    AS created_by_id
    FROM {{ src }}."public_activity" pa
    WHERE pa.category = 'comment'
      AND pa.content IS NOT NULL
      AND LENGTH(TRIM(regexp_replace(pa.content,'<[^>]*>',' '))) > 10
      AND (pa.candidate_id IS NOT NULL OR pa.contact_id IS NOT NULL)
),

candidate_notes AS (
    SELECT act_id, content_plain, insert_timestamp, created_by_id,
           candidate_id, NULL::varchar AS contact_key
    FROM staged
    WHERE candidate_id IS NOT NULL
),

contact_notes AS (
    SELECT act_id, content_plain, insert_timestamp, created_by_id,
           NULL::varchar AS candidate_id, contact_id AS contact_key
    FROM staged
    WHERE contact_id IS NOT NULL
),

people AS (
    SELECT id::varchar AS person_id,
           COALESCE(company_contact_id::varchar,'') AS contact_key,
           atlas_id
    FROM {{ ref('1_people_vincere') }}
),
 
joined AS (
    SELECT n.*, p.person_id, p.atlas_id AS atlas_person_id
    FROM candidate_notes n
    JOIN people p ON n.candidate_id = p.person_id
    UNION ALL
    SELECT n.*, p.person_id, p.atlas_id AS atlas_person_id
    FROM contact_notes n
    JOIN people p USING (contact_key)
),

enriched AS (
    SELECT j.*,
           COALESCE(um.atlas_id, '{{ var("created_by_id") }}') AS created_by_atlas_id
    FROM joined j
    LEFT JOIN {{ ref('users_vin') }} um ON um.id = j.created_by_id
)

SELECT
    {{ atlas_uuid("act_id") }} AS id,
    {{ atlas_uuid("act_id") }} AS atlas_id,
    content_plain                                    AS text,
    created_by_id,
    created_by_atlas_id,
    person_id,
    'manual'                                         AS type,
    TO_CHAR(insert_timestamp::date,'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(insert_timestamp::date,'YYYY-MM-DD"T00:00:00"') AS updated_at,
    atlas_person_id,
    '{{ var("agency_id") }}'                         AS agency_id,
    'migration'                                      AS source
FROM enriched