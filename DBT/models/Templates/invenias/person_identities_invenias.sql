{{ config(
    materialized = 'table',
    alias = 'person_identities_invenias',
    tags = ["invenias"]
) }}

WITH filtered_people AS (
  SELECT
    p."itemid"      AS person_id,
    p."firstname"   AS first_name,
    p."familyname"  AS last_name,
    p."mobilephone" AS phone,
    p."email1address" AS email,
    TRIM(BOTH '/' FROM REPLACE(REPLACE(REPLACE(p."LinkedIn",'https://',''),'http://',''),'www.','')) AS linkedin_url,
    u.id                             AS created_by_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}')  AS created_by_atlas_id,
    TO_CHAR(p."datecreated"::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
    TO_CHAR(p."datemodified"::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
    u2.id                            AS updated_by_id,
    COALESCE(u2.atlas_id, '{{ var("master_id") }}') AS updated_by_atlas_id,
    typeclientchecked
  FROM {{ var('source_database') }}."people" p
  LEFT JOIN {{ ref('users_invenias') }}  u  ON u.id  = p."creatorid"
  LEFT JOIN {{ ref('users_invenias') }}  u2 ON u2.id = p."modifierid"
),

internal_ids AS (
  SELECT DISTINCT
    id       AS person_id,
    atlas_id AS atlas_person_id
  FROM {{ ref('people_invenias') }}
),

email_identities AS (
  SELECT
    {{ atlas_uuid('fp.email') }} AS atlas_id,
    fp.email                     AS value,
    TRUE                         AS favourite,
    TRUE                         AS active,
    'email'                      AS type,
    CASE WHEN fp.typeclientchecked = TRUE THEN 'corporate' ELSE 'personal' END AS identity_type_type,
    'EmailPersonIdentity'        AS class_type,
    fp.person_id,
    'migration'                  AS source,
    fp.created_by_id,
    fp.created_by_atlas_id,
    FALSE                        AS hidden,
    TRUE                         AS verified,
    fp.created_at,
    fp.updated_at
  FROM filtered_people fp
  WHERE fp.email IS NOT NULL AND fp.email <> '' AND fp.email <> ' '
),

phone_identities AS (
  SELECT
    {{ atlas_uuid('fp.phone') }} AS atlas_id,
    fp.phone                     AS value,
    TRUE                         AS favourite,
    TRUE                         AS active,
    'phone'                      AS type,
    CASE WHEN fp.typeclientchecked = TRUE THEN 'corporate' ELSE 'personal' END AS identity_type_type,
    'PhonePersonIdentity'        AS class_type,
    fp.person_id,
    'migration'                  AS source,
    fp.created_by_id,
    fp.created_by_atlas_id,
    FALSE                        AS hidden,
    TRUE                         AS verified,
    fp.created_at,
    fp.updated_at
  FROM filtered_people fp
  WHERE fp.phone IS NOT NULL
    AND fp.phone <> ''
    AND POSITION('@' IN fp.phone) = 0
),

linkedin_identities AS (
  SELECT
    {{ atlas_uuid('fp.linkedin_url') }} AS atlas_id,
    {{linkedin_norm('fp.linkedin_url')}}  AS value,
    FALSE                         AS favourite,
    TRUE                          AS active,
    'linkedin'                    AS type,
    'personal'                    AS identity_type_type,
    'LinkedinPersonIdentity'      AS class_type,
    fp.person_id,
    'migration'                   AS source,
    fp.created_by_id,
    fp.created_by_atlas_id,
    FALSE                         AS hidden,
    TRUE                          AS verified,
    fp.created_at,
    fp.updated_at
  FROM filtered_people fp
  WHERE fp.linkedin_url IS NOT NULL
    AND POSITION('linkedin.com/in/' IN fp.linkedin_url) > 0
),

merged AS (
  SELECT atlas_id, value, favourite, active, type, identity_type_type, class_type,
         person_id, source, created_by_id, created_by_atlas_id, hidden, verified,
         created_at, updated_at
  FROM email_identities
  UNION ALL
  SELECT atlas_id, value, favourite, active, type, identity_type_type, class_type,
         person_id, source, created_by_id, created_by_atlas_id, hidden, verified,
         created_at, updated_at
  FROM phone_identities
  UNION ALL
  SELECT atlas_id, value, favourite, active, type, identity_type_type, class_type,
         person_id, source, created_by_id, created_by_atlas_id, hidden, verified,
         created_at, updated_at
  FROM linkedin_identities
),

owner_pick AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (
      PARTITION BY LOWER(TRIM(m.value)), m.type
      ORDER BY m.created_at ASC NULLS LAST, m.person_id ASC
    ) AS rn
  FROM merged m
)

SELECT
  op.atlas_id,
  op.value,
  op.favourite,
  op.active,
  op.type,
  op.identity_type_type,
  op.class_type,
  op.person_id,
  op.source,
  op.created_by_id,
  op.created_by_atlas_id,
  op.hidden,
  op.verified,
  op.created_at,
  op.updated_at,
  ip.atlas_person_id
FROM owner_pick op
JOIN internal_ids ip USING (person_id)
WHERE op.rn = 1
ORDER BY atlas_person_id