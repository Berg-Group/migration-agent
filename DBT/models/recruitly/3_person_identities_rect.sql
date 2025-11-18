{{ config(
    materialized='table',
    alias='person_identities_rect',
    tags=['recruitly']
) }}

WITH base_identities AS (
    SELECT 
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        {{ email_norm('c.email') }} AS email,
        {{ email_norm('c.alternate_email') }} AS alternate_email,
        {{ phone_norm('c.home_phone') }} AS home_phone,
        {{ phone_norm('c.work_phone') }} AS work_phone,
        {{ phone_norm('c.mobile') }} AS mobile,
        {{ linkedin_norm('c.linkedin') }} AS linkedin,
        p.created_at,
        p.updated_at,
        '{{ var("master_id") }}' AS created_by_id,
        '{{ var("agency_id") }}' AS agency_id
    FROM {{ var('source_database') }}.candidates c
    INNER JOIN {{ ref('2_people_rect') }} p ON p.id = c.candidate_id

    UNION ALL

    SELECT 
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        {{ email_norm('ct.email') }} AS email,
        NULL AS alternate_email,
        NULL AS home_phone,
        {{ phone_norm('ct.work_phone') }} AS work_phone,
        {{ phone_norm('ct.mobile') }} AS mobile,
        NULL AS linkedin,
        p.created_at,
        p.updated_at,
        '{{ var("master_id") }}' AS created_by_id,
        '{{ var("agency_id") }}' AS agency_id
    FROM {{ var('source_database') }}.contacts ct
    INNER JOIN {{ ref('2_people_rect') }} p ON p.id = ct.contact_id
),
emails AS (
    SELECT
        person_id,
        atlas_person_id,
        email AS email_value,
        created_at,
        updated_at,
        created_by_id,
        agency_id
    FROM base_identities
    WHERE email IS NOT NULL AND POSITION('@' IN email) > 0 AND email ILIKE '%@%.%'

    UNION ALL

    SELECT
        person_id,
        atlas_person_id,
        alternate_email AS email_value,
        created_at,
        updated_at,
        created_by_id,
        agency_id
    FROM base_identities
    WHERE alternate_email IS NOT NULL AND POSITION('@' IN alternate_email) > 0 AND alternate_email ILIKE '%@%.%'
),
phones AS (
    SELECT 
        person_id, 
        atlas_person_id, 
        mobile AS phone, 
        'mobile' AS phone_src, 
        created_at, 
        updated_at, 
        created_by_id, 
        agency_id
    FROM base_identities
    WHERE mobile IS NOT NULL AND LENGTH(TRIM(mobile)) > 3

    UNION ALL

    SELECT 
        person_id, 
        atlas_person_id, 
        work_phone AS phone, 
        'work' AS phone_src, 
        created_at, 
        updated_at, 
        created_by_id, 
        agency_id
    FROM base_identities
    WHERE work_phone IS NOT NULL AND LENGTH(TRIM(work_phone)) > 3

    UNION ALL

    SELECT 
        person_id, 
        atlas_person_id, 
        home_phone AS phone, 
        'home' AS phone_src, 
        created_at, 
        updated_at, 
        created_by_id, 
        agency_id
    FROM base_identities
    WHERE home_phone IS NOT NULL AND LENGTH(TRIM(home_phone)) > 3
),
linkedin AS (
    SELECT
        person_id,
        atlas_person_id,
        linkedin AS linkedin_url,
        created_at,
        updated_at,
        created_by_id,
        agency_id
    FROM base_identities
    WHERE linkedin IS NOT NULL AND POSITION('linkedin.com' IN linkedin) > 0
),
raw_identities AS (
    SELECT
        atlas_person_id,
        person_id,
        'email' AS type,
        email_value AS value,
        CASE WHEN {{ is_personal_email('email_value') }} THEN 'personal' ELSE 'corporate' END AS identity_type_type,
        'EmailPersonIdentity' AS class_type,
        agency_id,
        created_by_id,
        'migration' AS source,
        FALSE AS hidden,
        FALSE AS bounced,
        TRUE AS active,
        CASE WHEN {{ is_personal_email('email_value') }} THEN TRUE ELSE FALSE END AS favourite,
        TRUE AS verified,
        created_at,
        updated_at
    FROM emails

    UNION ALL

    SELECT
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
    FROM linkedin

    UNION ALL

    SELECT
        atlas_person_id,
        person_id,
        'phone' AS type,
        phone AS value,
        CASE WHEN phone_src IN ('mobile','phone','home','cell','main','personal') THEN 'personal' ELSE 'corporate' END AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        agency_id,
        created_by_id,
        'migration' AS source,
        FALSE AS hidden,
        FALSE AS bounced,
        TRUE AS active,
        CASE WHEN phone_src IN ('mobile','phone','home','cell','main','personal') THEN TRUE ELSE FALSE END AS favourite,
        TRUE AS verified,
        created_at,
        updated_at
    FROM phones
)
SELECT
    {{ atlas_uuid("(atlas_person_id || type || value)") }} AS atlas_id,
    atlas_person_id,
    person_id,
    type,
    value,
    CASE WHEN type IN ('phone','email') THEN COALESCE(NULLIF(TRIM(identity_type_type), ''), 'personal') ELSE identity_type_type END AS identity_type_type,
    class_type,
    agency_id,
    created_by_id,
    source,
    hidden,
    bounced,
    active,
    favourite,
    verified,
    created_at,
    updated_at
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
