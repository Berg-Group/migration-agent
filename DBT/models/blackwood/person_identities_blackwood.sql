{{ config(
    materialized='table',
    alias='person_identities_blackwood',
    tags=['blackwood']
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id 
    FROM {{ ref('people_blackwood')}}
),
emails_norm AS (
    SELECT 
        ce.cref AS person_id,
        {{ email_norm('ce.emailaddress') }} AS value,
        CASE WHEN et.emailtype = 'Work' THEN 'corporate' ELSE 'personal' END AS identity_type_type,
        'EmailPersonIdentity' AS class_type,
        'email' AS type,
        cr.createdbywho AS created_by,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        TO_CHAR(cr.createddate, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(COALESCE(cr.updateddate, cr.createddate), 'YYYY-MM-DD"T"00:00:00') AS updated_at
    FROM {{ var('source_database') }}."candidate_email" ce
    LEFT JOIN {{ var('source_database') }}."library_emailtypes" et ON et.id = ce.emailtype
    LEFT JOIN {{ var('source_database') }}."candidate_recordinformation" cr USING (cref)
    LEFT JOIN {{ ref('users_blackwood')}} u ON u.id = cr.createdbywho
    WHERE ce.emailaddress IS NOT NULL AND ce.emailaddress <> '' AND ce.emailaddress <> ' '
),
emails_with_rank AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY value ORDER BY person_id) AS value_rank,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY value) AS fav_rank
    FROM emails_norm
),
unique_emails AS (
    SELECT 
        person_id, value, identity_type_type, class_type, type,
        CASE WHEN fav_rank = 1 THEN TRUE ELSE FALSE END AS favourite,
        created_by, created_by_atlas_id, created_at, updated_at
    FROM emails_with_rank
    WHERE value_rank = 1 AND value <> ''
),
phones_norm AS (
    SELECT
        p.cref AS person_id,
        {{ phone_norm('p.phone') }} AS value,
        CASE WHEN pt.phonetype = 'Company Main' THEN 'corporate' ELSE 'personal' END AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        'phone' AS type,
        cr.createdbywho AS created_by,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        TO_CHAR(cr.createddate, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(COALESCE(cr.updateddate, cr.createddate), 'YYYY-MM-DD"T"00:00:00') AS updated_at
    FROM {{ var('source_database') }}."candidate_phone" p
    LEFT JOIN {{ var('source_database') }}."library_phonetypes" pt ON pt.id = p.phonetype
    LEFT JOIN {{ var('source_database') }}."candidate_recordinformation" cr USING (cref)
    LEFT JOIN {{ ref('users_blackwood') }} u ON u.id = cr.createdbywho
    WHERE p.phone IS NOT NULL AND p.phone <> '' AND p.phone <> ' '
),
phones_with_rank AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY value ORDER BY person_id) AS value_rank
    FROM phones_norm
),
unique_phones AS (
    SELECT 
        person_id, value, identity_type_type, class_type, type,
        FALSE AS favourite,
        created_by, created_by_atlas_id, created_at, updated_at
    FROM phones_with_rank
    WHERE value_rank = 1 AND value <> ''
),
linkedin_norm_cte AS (
    SELECT
        p.cref AS person_id,
        TRIM(
            BOTH '/; ' FROM
            REPLACE(
                REPLACE(
                    REPLACE(
                        REGEXP_REPLACE({{ linkedin_norm('p.linkedin') }}, '^([a-z0-9-]+[.])*linkedin[.]com', 'linkedin.com'),
                        '<',''
                    ),
                    '>',''
                ),
                ' ',''
            )
        ) AS value,
        'personal' AS identity_type_type,
        'LinkedinPersonIdentity' AS class_type,
        'linkedin' AS type,
        cr.createdbywho AS created_by,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        TO_CHAR(cr.createddate, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(COALESCE(cr.updateddate, cr.createddate), 'YYYY-MM-DD"T"00:00:00') AS updated_at
    FROM {{ var('source_database') }}."candidate_socialmedia" p
    LEFT JOIN {{ var('source_database') }}."candidate_recordinformation" cr USING (cref)
    LEFT JOIN {{ ref('users_blackwood') }} u ON u.id = cr.createdbywho
    WHERE p.linkedin IS NOT NULL AND p.linkedin <> '' AND p.linkedin <> ' '
),
linkedin_with_rank AS (
    SELECT
        person_id,
        value,
        identity_type_type,
        class_type,
        type,
        ROW_NUMBER() OVER (PARTITION BY value ORDER BY person_id) AS value_rank,
        created_by,
        created_by_atlas_id,
        created_at,
        updated_at
    FROM linkedin_norm_cte
),
unique_linkedin AS (
    SELECT *
    FROM linkedin_with_rank
    WHERE value_rank = 1 AND value <> ''
),
merged AS (
    SELECT 
        {{ atlas_uuid("value::text || 'email'") }} AS atlas_id,
        person_id,
        value,
        identity_type_type,
        class_type,
        type,
        'migration' AS source,
        favourite,
        TRUE AS active,
        created_by,
        created_by_atlas_id,
        FALSE AS hidden,
        TRUE AS verified,
        created_at,
        updated_at
    FROM unique_emails
    
    UNION ALL 
    
    SELECT 
        {{ atlas_uuid("value::text || 'phone'") }} AS atlas_id,
        person_id,
        value,
        identity_type_type,
        class_type,
        type,
        'migration' AS source,
        FALSE AS favourite,
        TRUE AS active,
        created_by,
        created_by_atlas_id,
        FALSE AS hidden,
        TRUE AS verified,
        created_at,
        updated_at
    FROM unique_phones
    
    UNION ALL 
    
    SELECT 
        {{ atlas_uuid("value::text || 'linkedin'") }} AS atlas_id,
        person_id,
        value,
        identity_type_type,
        class_type,
        type,
        'migration' AS source,
        FALSE AS favourite,
        TRUE AS active,
        created_by,
        created_by_atlas_id,
        FALSE AS hidden,
        TRUE AS verified,
        created_at,
        updated_at
    FROM unique_linkedin
)
SELECT 
    m.*,
    ip.atlas_person_id
FROM merged m
LEFT JOIN internal_persons ip USING (person_id)
ORDER BY atlas_person_id
