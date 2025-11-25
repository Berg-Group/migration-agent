{{ config(
    materialized = 'table',
    alias = 'person_identities_bba',
    tags = ['bba']
) }}

WITH internal_people AS (
  SELECT
      id,
      atlas_id
  FROM {{ ref('people_bba') }}
),

h AS (
  SELECT
      entity_id,
      createddate,
      updateddate
  FROM {{ var('source_database') }}."entity_table"
),

emails AS (
  SELECT
      (pe.bisuniqueid::text || '_email')                     AS id,
      {{ atlas_uuid('pe.bisuniqueid::text || pe.email_add') }} AS atlas_id,
      TO_CHAR(h.createddate::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
      TO_CHAR(h.updateddate::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
      pe.reference                                           AS person_id,
      ip.atlas_id                                            AS atlas_person_id,
      'email'                                                AS type,
      {{ email_norm('pe.email_add') }}                       AS value,
      'personal'                                             AS identity_type_type,
      'EmailPersonIdentity'                                  AS class_type,
      'migration'                                            AS source,
      '1'                                                    AS created_by_id,
      '{{ var("master_id") }}'                               AS created_by_atlas_id,
      '{{ var("master_id") }}'                               AS updated_by_atlas_id,
      FALSE                                                  AS bounced,
      FALSE                                                  AS hidden,
      FALSE                                                  AS favourite,
      TRUE                                                   AS active,
      ROW_NUMBER() OVER (
          PARTITION BY {{ email_norm('pe.email_add') }}
          ORDER BY h.createddate NULLS LAST, pe.bisuniqueid
      )                                                      AS rn
  FROM {{ var('source_database') }}."prop_email" pe
  JOIN internal_people ip
    ON ip.id = pe.reference
  LEFT JOIN h
    ON h.entity_id = pe.reference
  WHERE pe.email_add IS NOT NULL
    AND BTRIM(pe.email_add) <> ''
),

phones_raw AS (
  SELECT
      (pt.bisuniqueid::text || '_phone')                     AS id,
      {{ atlas_uuid('pt.reference::text || pt.tel_number') }} AS atlas_id,
      TO_CHAR(h.createddate::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
      TO_CHAR(h.updateddate::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
      pt.reference                                           AS person_id,
      ip.atlas_id                                            AS atlas_person_id,
      pt.tel_number                                          AS norm_phone,
      h.createddate                                          AS created_dt_order
  FROM {{ var('source_database') }}."prop_telephone" pt
  JOIN internal_people ip
    ON ip.id = pt.reference
  LEFT JOIN h
    ON h.entity_id = pt.reference
  WHERE pt.tel_number IS NOT NULL
    AND BTRIM(pt.tel_number) <> ''
    AND pt.tel_number NOT ILIKE '%@%'
    AND pt.tel_number NOT ILIKE 'http%'
    AND pt.tel_number NOT ILIKE 'mailto:%'
),

phones AS (
  SELECT
      id,
      atlas_id,
      created_at,
      updated_at,
      person_id,
      atlas_person_id,
      'phone'                      AS type,
      norm_phone                   AS value,
      'personal'                   AS identity_type_type,
      'PhonePersonIdentity'        AS class_type,
      'migration'                  AS source,
      '1'                          AS created_by_id,
      '{{ var("master_id") }}'     AS created_by_atlas_id,
      '{{ var("master_id") }}'     AS updated_by_atlas_id,
      FALSE                        AS bounced,
      FALSE                        AS hidden,
      FALSE                        AS favourite,
      TRUE                         AS active,
      ROW_NUMBER() OVER (
          PARTITION BY norm_phone
          ORDER BY created_dt_order NULLS LAST, id
      )                            AS rn
  FROM phones_raw
  WHERE norm_phone IS NOT NULL
    AND BTRIM(norm_phone) <> ''
    AND LENGTH(norm_phone) >= 6
),

links AS (
  SELECT
      (p.reference::text || '_linkedin')                     AS id,
      {{ atlas_uuid('p.reference::text || p.linkedin') }}    AS atlas_id,
      TO_CHAR(h.createddate::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
      TO_CHAR(h.updateddate::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
      p.reference                                           AS person_id,
      ip.atlas_id                                           AS atlas_person_id,
      'linkedin'                                            AS type,
      {{ linkedin_norm('p.linkedin') }}                     AS value,
      'personal'                                            AS identity_type_type,
      'LinkedinPersonIdentity'                              AS class_type,
      'migration'                                           AS source,
      '{{ var("master_id") }}'                              AS created_by_id,
      '{{ var("master_id") }}'                              AS created_by_atlas_id,
      '{{ var("master_id") }}'                              AS updated_by_atlas_id,
      FALSE                                                 AS bounced,
      FALSE                                                 AS hidden,
      FALSE                                                 AS favourite,
      TRUE                                                  AS active,
      ROW_NUMBER() OVER (
          PARTITION BY {{ linkedin_norm('p.linkedin') }}
          ORDER BY h.createddate NULLS LAST, p.reference
      )                                                     AS rn
  FROM {{ var('source_database') }}."prop_person_gen" p
  JOIN internal_people ip
    ON ip.id = p.reference
  LEFT JOIN h
    ON h.entity_id = p.reference
  WHERE p.linkedin IS NOT NULL
    AND BTRIM(p.linkedin) <> ''
),

merged AS (
SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    created_at,
    updated_at,
    type,
    value,
    identity_type_type,
    class_type,
    source,
    created_by_id,
    created_by_atlas_id,
    updated_by_atlas_id,
    bounced,
    hidden,
    favourite,
    active
FROM emails
WHERE rn = 1

UNION ALL

SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    created_at,
    updated_at,
    type,
    value,
    identity_type_type,
    class_type,
    source,
    created_by_id,
    created_by_atlas_id,
    updated_by_atlas_id,
    bounced,
    hidden,
    favourite,
    active
FROM phones
WHERE rn = 1

UNION ALL

SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    created_at,
    updated_at,
    type,
    value,
    identity_type_type,
    class_type,
    source,
    created_by_id,
    created_by_atlas_id,
    updated_by_atlas_id,
    bounced,
    hidden,
    favourite,
    active
FROM links
WHERE rn = 1)

SELECT * FROM merged 
ORDER BY atlas_person_id 
