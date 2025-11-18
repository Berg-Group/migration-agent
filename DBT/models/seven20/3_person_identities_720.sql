{{ config(
    materialized='table',
    alias='person_identities_720',
    tags=["seven20"]
) }}

WITH source_people AS (
    SELECT 
        c.id AS person_id,
        p.atlas_id AS atlas_person_id,
        {{ phone_norm("c.phone") }} AS phone,
        {{ phone_norm("c.mobilephone") }} AS mobile,
        {{ phone_norm("c.otherphone") }} AS otherphone,
        {{ email_norm("c.email") }} AS email,
        {{ linkedin_norm("c.seven20__linkedin__c") }} AS linkedin_url,
        {{ linkedin_norm("c.plaunch__linkedin__c") }} AS linkedin_url2,
        TO_CHAR(c.createddate::timestamp(0), 'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(c.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
        c.createdbyid AS created_by_id,
        '{{ var("agency_id") }}' AS agency_id,
        coalesce(r.name, c.seven20__record_type_name__c) as record_type
    FROM {{ var('source_database') }}."contact" c
    INNER JOIN {{ ref('2_people_720') }} p ON p.id = c.id
    LEFT JOIN  {{ var('source_database') }}."recordtype" r ON r.id = c.seven20__record_type_name__c
),
source_people_dupes AS (
    SELECT 
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        {{ phone_norm("c.phone") }} AS phone,
        {{ phone_norm("c.mobilephone") }} AS mobile,
        {{ phone_norm("c.otherphone") }} AS otherphone,
        {{ email_norm("c.email") }} AS email,
        {{ linkedin_norm("c.seven20__linkedin__c") }} AS linkedin_url,
        {{ linkedin_norm("c.plaunch__linkedin__c") }} AS linkedin_url2,
        TO_CHAR(c.createddate::timestamp(0), 'YYYY-MM-DD"T00:00:00"') AS created_at,
        TO_CHAR(c.lastmodifieddate::timestamp(0), 'YYYY-MM-DD"T00:00:00"') AS updated_at,
        c.createdbyid AS created_by_id,
        '{{ var("agency_id") }}' AS agency_id,
        coalesce(r.name, c.seven20__record_type_name__c) as record_type
    FROM {{ var('source_database') }}."contact" c
    INNER JOIN {{ ref('people_dupes_720') }} pd ON pd.contact_id = c.id
    INNER JOIN {{ ref('2_people_720') }} p ON p.id = pd.candidate_id
    LEFT JOIN  {{ var('source_database') }}."recordtype" r ON r.id = c.seven20__record_type_name__c
),
combined_source AS (
    SELECT * FROM source_people
    UNION ALL
    SELECT * FROM source_people_dupes
),
raw_identities AS (
    SELECT
        {{ atlas_uuid("(atlas_person_id || 'email' || email)") }} AS atlas_id,
        atlas_person_id,
        person_id,
        'email' AS type,
        email AS value,
        CASE
            WHEN record_type = 'Candidate' THEN 'personal'
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
            WHEN record_type = 'Candidate' AND {{ is_personal_email('email') }} THEN TRUE
            ELSE FALSE
        END AS favourite,
        TRUE AS verified,
        created_at,
        updated_at
    FROM combined_source
    WHERE email IS NOT NULL
      AND email ~* '@'
      AND LENGTH(TRIM(email)) > 3

    UNION ALL

    SELECT
        {{ atlas_uuid("(atlas_person_id || 'linkedin' || COALESCE(linkedin_url, linkedin_url2))") }} AS atlas_id,
        atlas_person_id,
        person_id,
        'linkedin' AS type,
        COALESCE(linkedin_url, linkedin_url2) AS value,
        'personal' AS identity_type_type,
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
    FROM combined_source
    WHERE COALESCE(linkedin_url, linkedin_url2) IS NOT NULL
        AND POSITION('linkedin.com' IN COALESCE(linkedin_url, linkedin_url2)) > 0

    UNION ALL

    SELECT
        {{ atlas_uuid("(atlas_person_id || 'phone' || phone_value)") }} AS atlas_id,
        atlas_person_id,
        person_id,
        'phone' AS type,
        phone_value AS value,
        CASE 
            WHEN phone_src IN ('mobile', 'phone') THEN 'personal' 
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
            WHEN phone_src IN ('mobile', 'phone') THEN TRUE 
            ELSE FALSE 
        END AS favourite,
        TRUE AS verified,
        created_at,
        updated_at
    FROM (
        SELECT 
            atlas_person_id, 
            person_id, 
            agency_id, 
            created_by_id,
            created_at,
            updated_at,
            'phone' AS phone_src, 
            phone AS phone_value 
        FROM combined_source
        WHERE LENGTH(phone) > 5
        UNION ALL
        SELECT 
            atlas_person_id, 
            person_id, 
            agency_id, 
            created_by_id,
            created_at,
            updated_at,
            'mobile' AS phone_src, 
            mobile AS phone_value 
        FROM combined_source
        WHERE LENGTH(mobile) > 5
        UNION ALL
        SELECT 
            atlas_person_id, 
            person_id, 
            agency_id, 
            created_by_id,
            created_at,
            updated_at,
            'otherphone' AS phone_src, 
            otherphone AS phone_value 
        FROM combined_source
        WHERE LENGTH(otherphone) > 5
    ) p
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
    created_at,
    updated_at
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
