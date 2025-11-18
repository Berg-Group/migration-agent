{{ config(
    materialized='table',
    alias='person_identities_loxo_csv',
    tags=["seven20"]
) }}

WITH filtered_contacts AS (
    SELECT 
        c.id AS person_id,
        c.phone,
        c.email,
        TRIM(BOTH '/;' FROM REPLACE(REPLACE(REPLACE(c.linkedin, 'https://', ''), 'http://', ''), 'www.', '')) AS linkedin_url,
        '{{ var('created_by_id') }}' AS created_by_id,
        to_char(c.created_date::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        to_char(c.recent_activity_date::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        '{{ var('created_by_id') }}'  AS updated_by_id,
        type
    FROM {{ var('source_database') }}."candidate" c

    UNION ALL 
    SELECT 
        cl.id AS person_id,
        cl.phone,
        cl.email,
        TRIM(BOTH '/;' FROM REPLACE(REPLACE(REPLACE(cl.linkedin, 'https://', ''), 'http://', ''), 'www.', '')) AS linkedin_url,
        '{{ var('created_by_id') }}' AS created_by_id,
        to_char(cl.created_date::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
        to_char(cl.recent_activity_date::timestamp(0),  'YYYY-MM-DD"T"HH24:MI:SS') AS updated_at,
        '{{ var('created_by_id') }}'  AS updated_by_id,
        type
    FROM {{ var('source_database') }}."client" cl
),

internal_ids AS (
    SELECT 
        DISTINCT id AS person_id,
        atlas_id AS atlas_person_id
    FROM "{{ this.schema }}"."people_loxo"
),

email_identities AS (
    SELECT
        lower(
            substring(md5(fc.email || getdate()::text), 1, 8) || '-' ||
            substring(md5(fc.email || getdate()::text), 9, 4) || '-' ||
            substring(md5(fc.email || getdate()::text), 13, 4) || '-' ||
            substring(md5(fc.email || getdate()::text), 17, 4) || '-' ||
            substring(md5(fc.email || getdate()::text), 21, 12)
        ) AS atlas_id,
        fc.email AS value,
        true AS favourite,
        true AS active,
        'email' AS type,
        CASE WHEN type = 'Candidate' THEN 'personal' 
            ELSE 'corporate' END AS identity_type_type,
        'EmailPersonIdentity' AS class_type,
        fc.person_id,
        'migration' AS source,
        fc.created_by_id,
        'false' AS hidden,
        'true' AS verified,
        created_at,
        updated_at

    FROM filtered_contacts fc
    WHERE fc.email IS NOT NULL AND fc.email <> '' AND fc.email <> ' '
),

phone_identities AS (
    SELECT
        lower(
            substring(md5(fc.phone || getdate()::text), 1, 8) || '-' ||
            substring(md5(fc.phone || getdate()::text), 9, 4) || '-' ||
            substring(md5(fc.phone || getdate()::text), 13, 4) || '-' ||
            substring(md5(fc.phone || getdate()::text), 17, 4) || '-' ||
            substring(md5(fc.phone || getdate()::text), 21, 12)
        ) AS atlas_id,
        fc.phone AS value,
        true AS favourite,
        true AS active,
        'phone' AS type,
        CASE WHEN type = 'Candidate' THEN 'personal' 
            ELSE 'corporate' END AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        fc.person_id,
        'migration' AS source,
        fc.created_by_id,
        'false' AS hidden,
        'true' AS verified,
        created_at,
        updated_at

    FROM filtered_contacts fc
    WHERE fc.phone IS NOT NULL AND fc.phone <> '' AND fc.phone <> ' '
),

linkedin_identities AS (
    SELECT
        lower(
            substring(md5(linkedin_url || getdate()::text), 1, 8) || '-' ||
            substring(md5(linkedin_url || getdate()::text), 9, 4) || '-' ||
            substring(md5(linkedin_url || getdate()::text), 13, 4) || '-' ||
            substring(md5(linkedin_url || getdate()::text), 17, 4) || '-' ||
            substring(md5(linkedin_url || getdate()::text), 21, 12)
        ) AS id,
        
       linkedin_url AS value,
        false AS favourite,
        true AS active,
        'linkedin' AS type,
        NULL AS identity_type_type,
        'LinkedinPersonIdentity' AS class_type,
        fc.person_id,
        'migration' AS source,
        fc.created_by_id,
        'false' AS hidden,
        'true' AS verified,
        created_at,
        updated_at

    FROM 
    filtered_contacts fc
    WHERE fc.linkedin_url IS NOT NULL 
      AND position('linkedin.com/in/' in fc.linkedin_url) > 0
),

merged AS (
    SELECT * FROM email_identities
    UNION ALL
    SELECT * FROM phone_identities
    UNION ALL
    SELECT * FROM linkedin_identities
)

SELECT 
    m.*, 
    ip.atlas_person_id
FROM merged m
LEFT JOIN internal_ids ip ON ip.person_id::text = m.person_id::text

