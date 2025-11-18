{{ config(
  materialized = 'table',
  alias = 'person_identities_loxo',
  tags = ["loxo"]
) }}

WITH emails AS (
  SELECT
    p.id AS person_id,
    p.atlas_id AS atlas_person_id,
    {{ email_norm("pe.email") }} AS email,
    LOWER(TRIM(pe."type")) AS email_src,
    p.created_at,
    p.updated_at,
    '{{ var("master_id") }}' AS created_by_id,
    '{{ var("agency_id") }}' AS agency_id
  FROM {{ var('source_database') }}.people_emails pe
  INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = pe.root_id
),
phones AS (
  SELECT
    p.id AS person_id,
    p.atlas_id AS atlas_person_id,
    {{ phone_norm("pp.phone") }} AS phone,
    LOWER(TRIM(pp."type")) AS phone_src,
    p.created_at,
    p.updated_at,
    '{{ var("master_id") }}' AS created_by_id,
    '{{ var("agency_id") }}' AS agency_id
  FROM {{ var('source_database') }}.people_phones pp
  INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = pp.root_id
),
social AS (
  SELECT 
    p.id AS person_id,
    p.atlas_id AS atlas_person_id,
    {{ linkedin_norm("ps.value") }} AS linkedin_url,
    {{ website_norm("ps.value") }} AS website_url,
    p.created_at,
    p.updated_at,
    '{{ var("master_id") }}' AS created_by_id,
    '{{ var("agency_id") }}' AS agency_id
  FROM {{ var('source_database') }}.people_social ps 
  INNER JOIN {{ ref('1_people_loxo') }} p ON p.id = ps.root_id
),
raw_identities AS (
  SELECT
    {{ atlas_uuid("(atlas_person_id || 'email' || email)") }} AS atlas_id,
    atlas_person_id,
    person_id,
    'email' AS type,
    email AS value,
    CASE
      WHEN {{ is_personal_email('email') }} THEN 'personal'
      ELSE 'corporate'
    END AS identity_type_type,
    'EmailPersonIdentity' AS class_type,
    agency_id,
    created_by_id,
    'migration' AS source,
    FALSE AS hidden,
    FALSE AS bounced,
    TRUE AS active,
    CASE
      WHEN {{ is_personal_email('email') }} THEN TRUE
      ELSE FALSE
    END AS favourite,
    TRUE AS verified,
    created_at,
    updated_at
  FROM emails
  WHERE email IS NOT NULL
    AND email ILIKE '%@%.%'

  UNION ALL

  SELECT
    {{ atlas_uuid("(atlas_person_id || 'linkedin' || linkedin_url)") }} AS atlas_id,
    atlas_person_id,
    person_id,
    'linkedin' AS type,
    linkedin_url AS value,
    NULL AS identity_type_type,
    'LinkedinPersonIdentity' AS class_type,
    agency_id,
    created_by_id,
    'migration' AS source,
    FALSE AS hidden,
    FALSE AS bounced,
    TRUE AS active,
    FALSE AS favourite,
    TRUE AS verified,
    created_at,
    updated_at
  FROM social
  WHERE linkedin_url IS NOT NULL
    AND POSITION('linkedin.com' IN linkedin_url) > 0

  UNION ALL

  SELECT
    {{ atlas_uuid("(atlas_person_id || 'website' || website_url)") }} AS atlas_id,
    atlas_person_id,
    person_id,
    'website' AS type,
    website_url AS value,
    NULL AS identity_type_type,
    'SocialPersonIdentity' AS class_type,
    agency_id,
    created_by_id,
    'migration' AS source,
    FALSE AS hidden,
    FALSE AS bounced,
    TRUE AS active,
    FALSE AS favourite,
    TRUE AS verified,
    created_at,
    updated_at
  FROM social
  WHERE website_url IS NOT NULL
    AND POSITION('linkedin.com' IN website_url) = 0
    AND POSITION('.' IN website_url) > 0

  UNION ALL

  SELECT
    {{ atlas_uuid("(atlas_person_id || 'phone' || phone)") }} AS atlas_id,
    atlas_person_id,
    person_id,
    'phone' AS type,
    phone AS value,
    CASE 
      WHEN phone_src IN ('mobile', 'phone', 'home', 'cell', 'main', 'personal') THEN 'personal' 
      ELSE 'corporate' 
    END AS identity_type_type,
    'PhonePersonIdentity' AS class_type,
    agency_id,
    created_by_id,
    'migration' AS source,
    FALSE AS hidden,
    FALSE AS bounced,
    TRUE AS active,
    CASE 
      WHEN phone_src IN ('mobile', 'phone', 'home', 'cell', 'main', 'personal') THEN TRUE 
      ELSE FALSE 
    END AS favourite,
    TRUE AS verified,
    created_at,
    updated_at
  FROM phones
  WHERE phone IS NOT NULL
    AND LENGTH(TRIM(phone)) > 3 
)
SELECT
  atlas_id,
  atlas_person_id,
  person_id,
  type,
  value,
  CASE
    WHEN type IN ('phone','email') THEN COALESCE(NULLIF(TRIM(identity_type_type), ''), 'personal')
    ELSE identity_type_type
  END AS identity_type_type,
  class_type,
  agency_id,
  created_by_id,
  source,
  hidden,
  bounced,
  active,
  favourite,
  verified,
  TO_CHAR(TRY_CAST(created_at AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
  TO_CHAR(TRY_CAST(updated_at AS TIMESTAMP)::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY type, value
      ORDER BY created_at
    ) AS rn
  FROM raw_identities
) dupe
WHERE rn = 1
ORDER BY atlas_person_id