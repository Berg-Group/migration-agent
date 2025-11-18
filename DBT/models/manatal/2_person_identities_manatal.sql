{{ config(materialized='table', alias='person_identities_manatal') }}

{# ───── grab YYYY-MM-DD from dbt_project.yml ───── #}
{% set iso_midnight = var('date') ~ 'T00:00:00' %}

{% set db = var('source_database') %}

WITH candidate_emails AS (
    -- Candidate emails (personal)
    SELECT 
        id AS person_id,
        email AS value,
        'email' AS type,
        'EmailPersonIdentity' AS class_type,
        1 AS priority,
        'personal' AS identity_type_type
    FROM {{ db }}.candidate
    WHERE email IS NOT NULL AND TRIM(email) <> ''
),

candidate_phones AS (
    -- Candidate phones (personal)
    SELECT 
        id AS person_id,
        phone_number AS value,
        'phone' AS type,
        'PhonePersonIdentity' AS class_type,
        1 AS priority,
        'personal' AS identity_type_type
    FROM {{ db }}.candidate
    WHERE phone_number IS NOT NULL AND TRIM(phone_number) <> ''
),

candidate_linkedin AS (
    -- Candidate LinkedIn profiles
    SELECT 
        c.id AS person_id,
        cs.social_media_url AS value,
        'linkedin' AS type,
        'LinkedinPersonIdentity' AS class_type,
        1 AS priority,
        'personal' AS identity_type_type
    FROM {{ db }}.candidate c
    JOIN {{ db }}.candidate_social cs ON c.id = cs.candidate_id
    WHERE cs.social_media_url IS NOT NULL 
      AND TRIM(cs.social_media_url) <> ''
),

-- Only include contacts that aren't matched with candidates
contact_ids AS (
    SELECT 
        ct.id AS contact_id,
        'cc' || ct.id AS person_id
    FROM {{ db }}.contact ct
    LEFT JOIN {{ db }}.candidate_social cs
        ON rtrim(ct.linkedin_url, '/') = rtrim(cs.social_media_url, '/')
    WHERE cs.candidate_id IS NULL
),

contact_emails AS (
    -- Contact emails (always corporate)
    SELECT
        ci.person_id,
        ct.email AS value,
        'email' AS type,
        'EmailPersonIdentity' AS class_type,
        2 AS priority,
        'corporate' AS identity_type_type
    FROM {{ db }}.contact ct
    JOIN contact_ids ci ON ct.id = ci.contact_id
    WHERE ct.email IS NOT NULL AND TRIM(ct.email) <> ''
),

contact_phones AS (
    -- Contact phones (always corporate)
    SELECT
        ci.person_id,
        ct.phone_number AS value,
        'phone' AS type,
        'PhonePersonIdentity' AS class_type,
        2 AS priority,
        'corporate' AS identity_type_type
    FROM {{ db }}.contact ct
    JOIN contact_ids ci ON ct.id = ci.contact_id
    WHERE ct.phone_number IS NOT NULL AND TRIM(ct.phone_number) <> ''
),

contact_linkedin AS (
    -- Contact LinkedIn profiles
    SELECT
        ci.person_id,
        ct.linkedin_url AS value,
        'linkedin' AS type,
        'LinkedinPersonIdentity' AS class_type,
        2 AS priority,
        'personal' AS identity_type_type
    FROM {{ db }}.contact ct
    JOIN contact_ids ci ON ct.id = ci.contact_id
    WHERE ct.linkedin_url IS NOT NULL AND TRIM(ct.linkedin_url) <> ''
),

-- Also handle identities for candidates that are linked to contacts
linked_contact_emails AS (
    -- Contact emails for linked candidates (corporate)
    SELECT
        c.id AS person_id,
        ct.email AS value,
        'email' AS type,
        'EmailPersonIdentity' AS class_type,
        2 AS priority,
        'corporate' AS identity_type_type
    FROM {{ db }}.candidate c
    JOIN {{ db }}.candidate_social cs ON c.id = cs.candidate_id
    JOIN {{ db }}.contact ct ON rtrim(ct.linkedin_url, '/') = rtrim(cs.social_media_url, '/')
    WHERE ct.email IS NOT NULL AND TRIM(ct.email) <> ''
),

linked_contact_phones AS (
    -- Contact phones for linked candidates (corporate)
    SELECT
        c.id AS person_id,
        ct.phone_number AS value,
        'phone' AS type,
        'PhonePersonIdentity' AS class_type,
        2 AS priority,
        'corporate' AS identity_type_type
    FROM {{ db }}.candidate c
    JOIN {{ db }}.candidate_social cs ON c.id = cs.candidate_id
    JOIN {{ db }}.contact ct ON rtrim(ct.linkedin_url, '/') = rtrim(cs.social_media_url, '/')
    WHERE ct.phone_number IS NOT NULL AND TRIM(ct.phone_number) <> ''
),

all_identities_raw AS (
    -- Combine all identities
    SELECT * FROM candidate_emails 
    UNION ALL
    SELECT * FROM candidate_phones 
    UNION ALL
    SELECT * FROM candidate_linkedin 
    UNION ALL
    SELECT * FROM contact_emails 
    UNION ALL
    SELECT * FROM contact_phones 
    UNION ALL
    SELECT * FROM contact_linkedin
    UNION ALL
    SELECT * FROM linked_contact_emails
    UNION ALL
    SELECT * FROM linked_contact_phones
),

dedup AS (
    -- Remove duplicate identities keeping the highest priority
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(value)) ORDER BY priority) AS rn
        FROM all_identities_raw
    ) r
    WHERE rn = 1
),

joined AS (
    -- Join with people table to get atlas_person_id
    SELECT 
        d.person_id,
        p.atlas_id AS atlas_person_id,
        d.value,
        d.type,
        d.class_type,
        d.identity_type_type
    FROM dedup d
    LEFT JOIN {{ ref('1_people_manatal') }} p
           ON p.id = d.person_id
)

-- Final output
SELECT
    {{ atlas_uuid('value') }} AS atlas_id,
    value,
    TRUE AS favourite,
    TRUE AS active,
    type,
    identity_type_type,
    class_type,
    person_id,
    atlas_person_id,
    '{{ iso_midnight }}' AS created_at,
    '{{ iso_midnight }}' AS updated_at,
    'migration' AS source
FROM joined
ORDER BY person_id, type 