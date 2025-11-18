{{ config(
    materialized = 'table',
    alias        = 'person_identities_fr',
    tags         = ['seven20']
) }}

WITH filtered_contacts AS (
    SELECT 
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        p.first_name,
        p.last_name,
        replace(replace(replace(replace(replace(c.mobilephone, '+',''), ' ', ''), '(', ''), ')', ''), '-', '') AS mobilephone,
        c.email,
        trim(both '/;' from replace(replace(replace(c.seven20__linkedin__c, 'https://', ''), 'http://', ''), 'www.', '')) AS linkedin_url,
        p.created_by_atlas_id,
        p.created_at,
        p.updated_at,
        coalesce(r.name, c.seven20__record_type_name__c) as record_type
    FROM {{ ref('people_fr') }} p
    LEFT JOIN {{ var('source_database') }}."contact" c USING (id)
    LEFT JOIN {{ var('source_database') }}."recordtype" r ON r.id = c.seven20__record_type_name__c
),

email_identities AS (
    SELECT
        {{ atlas_uuid('fc.email') }} AS atlas_id,
        fc.email AS value,
        true AS favourite,
        true AS active,
        'email' AS type,
        CASE WHEN record_type = 'Candidate' THEN 'personal' ELSE 'corporate' END AS identity_type_type,
        'EmailPersonIdentity' AS class_type,
        fc.person_id,
        fc.atlas_person_id,
        'migration' AS source,
        fc.created_by_atlas_id,
        false AS hidden,
        true AS verified,
        created_at,
        updated_at
    FROM filtered_contacts fc
    WHERE fc.email IS NOT NULL AND trim(fc.email) <> ''
),

phone_identities AS (
    SELECT
        {{ atlas_uuid('fc.mobilephone') }} AS atlas_id,
        fc.mobilephone AS value,
        true AS favourite,
        true AS active,
        'phone' AS type,
        CASE WHEN record_type = 'Candidate' THEN 'personal' ELSE 'corporate' END AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        fc.person_id,
        fc.atlas_person_id,
        'migration' AS source,
        fc.created_by_atlas_id,
        false AS hidden,
        true AS verified,
        created_at,
        updated_at
    FROM filtered_contacts fc
    WHERE fc.mobilephone IS NOT NULL AND trim(fc.mobilephone) <> ''
),

linkedin_identities AS (
    SELECT
        {{ atlas_uuid('linkedin_url') }} AS atlas_id,
        linkedin_url AS value,
        false AS favourite,
        true AS active,
        'linkedin' AS type,
        'personal' AS identity_type_type,
        'LinkedinPersonIdentity' AS class_type,
        fc.person_id,
        fc.atlas_person_id,
        'migration' AS source,
        fc.created_by_atlas_id,
        false AS hidden,
        true AS verified,
        created_at,
        updated_at
    FROM filtered_contacts fc
    WHERE fc.linkedin_url IS NOT NULL
      AND position('linkedin.com/in/' in fc.linkedin_url) > 0
),

merged AS (
    SELECT * FROM email_identities
    UNION ALL
    SELECT * FROM phone_identities
    UNION ALL
    SELECT * FROM linkedin_identities
),

ranked AS (
    SELECT
        m.*,
        row_number() over (
            partition by type, value
            order by favourite desc, verified desc, created_at
        ) as rn
    FROM merged m
),

base AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
)

SELECT
    atlas_id,
    value,
    favourite,
    active,
    type,
    identity_type_type,
    class_type,
    person_id,
    atlas_person_id,
    source,
    created_by_atlas_id,
    hidden,
    verified,
    created_at,
    updated_at
FROM base
ORDER BY atlas_person_id
