{{ config(
    materialized = 'table',
    alias        = 'person_identities_bh',
    tags         = ['bullhorn']
) }}

WITH source_people AS (
    SELECT
        u.userid AS person_id,
        {{ email_norm("u.email") }} AS email,
        {{ email_norm("u.email2") }} AS email2,
        u.customint1 AS email_status,
        {{ phone_norm("u.phone") }} AS phone,
        {{ phone_norm("u.phone2") }} AS phone2,
        {{ phone_norm("u.mobile") }} AS mobile,
        {{ phone_norm("u.workphone") }} AS workphone,
        (u.clientcorporationid IS NOT NULL) AS has_client_corp,
        p.atlas_id AS atlas_person_id,
        '{{ var("agency_id") }}' AS agency_id,
        '{{ var("master_id") }}' AS created_by_id,
        {{ linkedin_norm("u.customtext2") }} AS linkedin_clean,
        {{ linkedin_norm("u.customtext3") }} AS linkedin_clean_2
    FROM {{ var('source_database') }}."bh_usercontact" u
    INNER JOIN {{ ref('1_people_bh') }} p ON p.id = u.userid
),
source_dupe_people AS (
    SELECT
        p.id AS person_id,
        {{ email_norm("u.email") }} AS email,
        {{ email_norm("u.email2") }} AS email2,
        u.customint1 AS email_status,
        {{ phone_norm("u.phone") }} AS phone,
        {{ phone_norm("u.phone2") }} AS phone2,
        {{ phone_norm("u.mobile") }} AS mobile,
        {{ phone_norm("u.workphone") }} AS workphone,
        (u.clientcorporationid IS NOT NULL) AS has_client_corp,
        p.atlas_id AS atlas_person_id,
        '{{ var("agency_id") }}' AS agency_id,
        '{{ var("master_id") }}' AS created_by_id,
        {{ linkedin_norm("u.customtext2") }} AS linkedin_clean,
        {{ linkedin_norm("u.customtext3") }} AS linkedin_clean_2
    FROM {{ var('source_database') }}."bh_usercontact" u
    INNER JOIN {{ ref('people_dupes_bh') }} pd ON pd.contact_id = u.userid
    INNER JOIN {{ ref('1_people_bh') }} p ON p.id = pd.candidate_id
),
combined_source AS (
    SELECT * FROM source_people
    UNION ALL
    SELECT * FROM source_dupe_people
),
raw_identities AS (
    SELECT
        atlas_person_id,
        person_id,
        'email' AS type,
        LOWER(COALESCE(email, email2)) AS value,
        CASE
            WHEN has_client_corp THEN 'corporate'
            WHEN {{ is_personal_email('COALESCE(email, email2)') }} THEN 'personal'
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
            WHEN NOT has_client_corp
            AND {{ is_personal_email('COALESCE(email, email2)') }} THEN TRUE
            ELSE FALSE
        END AS favourite,
        TRUE AS verified,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        {{ atlas_uuid("(atlas_person_id || 'email' || COALESCE(email, email2, ''))") }} AS atlas_id,
        CASE 
            WHEN email_status = 2 THEN 'unsubscribed'
            ELSE NULL 
        END AS inactive_reason
    FROM combined_source
    WHERE COALESCE(email, email2) IS NOT NULL
      AND COALESCE(email, email2) ~* '@'
      AND LENGTH(TRIM(COALESCE(email, email2, ''))) > 3

    UNION ALL

    SELECT
        atlas_person_id,
        person_id,
        'linkedin' AS type,
        LOWER(COALESCE(linkedin_clean, linkedin_clean_2)) AS value,
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
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        {{ atlas_uuid("(atlas_person_id || 'linkedin' || COALESCE(linkedin_clean, linkedin_clean_2))") }} AS atlas_id,
        NULL AS inactive_reason
    FROM combined_source
    WHERE COALESCE(linkedin_clean, linkedin_clean_2) IS NOT NULL
        AND POSITION('linkedin.com' IN COALESCE(linkedin_clean, linkedin_clean_2)) > 0

    UNION ALL

    SELECT
        atlas_person_id,
        person_id,
        'phone' AS type,
        REPLACE(TRIM(phone_value), ' ', '') AS value,
        CASE WHEN phone_src = 'workphone' THEN 'corporate' ELSE 'personal' END AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        agency_id,
        created_by_id,
        'migration' AS source,
        FALSE AS hidden,
        FALSE AS bounced,
        TRUE AS active,
        CASE WHEN phone_src = 'mobile' THEN TRUE ELSE FALSE END AS favourite,
        TRUE AS verified,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        {{ atlas_uuid("(atlas_person_id || 'phone' || phone_value)") }} AS atlas_id,
        NULL AS inactive_reason
    FROM (
        SELECT 
            atlas_person_id, 
            person_id, 
            agency_id, 
            created_by_id, 
            'phone' AS phone_src, 
            phone AS phone_value 
        FROM combined_source
        WHERE phone IS NOT NULL AND LENGTH(phone) > 3 AND phone !~* '[a-z]'
        UNION ALL
        SELECT 
            atlas_person_id, 
            person_id, 
            agency_id, 
            created_by_id, 
            'phone2' AS phone_src, 
            phone2 AS phone_value 
        FROM combined_source
        WHERE phone2 IS NOT NULL AND LENGTH(phone2) > 3 AND phone2 !~* '[a-z]'
        UNION ALL
        SELECT 
            atlas_person_id, 
            person_id, 
            agency_id, 
            created_by_id, 
            'mobile' AS phone_src, 
            mobile AS phone_value 
        FROM combined_source
        WHERE mobile IS NOT NULL AND LENGTH(mobile) > 3 AND mobile !~* '[a-z]'
        UNION ALL
        SELECT 
            atlas_person_id, 
            person_id, 
            agency_id, 
            created_by_id, 
            'workphone' AS phone_src, 
            workphone AS phone_value 
        FROM combined_source
        WHERE workphone IS NOT NULL AND LENGTH(workphone) > 3 AND workphone !~* '[a-z]'
    ) p
)
SELECT
    atlas_person_id,
    person_id,
    type,
    value,
    CASE
        WHEN type IN ('phone','email')
        THEN COALESCE(NULLIF(TRIM(identity_type_type), ''), 'personal')
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
    created_at,
    updated_at,
    atlas_id,
    inactive_reason
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY type, value
            ORDER BY
                (identity_type_type IS NULL OR identity_type_type = '') ASC,
                created_at
        ) AS rn
    FROM raw_identities
) sub
WHERE rn = 1
ORDER BY atlas_person_id