{{ config(
    materialized='table',
    alias='person_identities_720',
    tags=["seven20"]
) }}

WITH filtered_contacts AS (
    SELECT 
        c.id AS person_id,
        c.firstname AS first_name,
        c.lastname AS last_name,
        c.mobilephone,
        c.email,
        TRIM(BOTH '/;' FROM REPLACE(REPLACE(REPLACE(c.seven20__linkedin__c, 'https://', ''), 'http://', ''), 'www.', '')) AS linkedin_url,

        c.createdbyid AS created_by_id,
        to_char(c.createddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS created_at,
        to_char(c.lastmodifieddate::timestamp(0), 
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) AS updated_at,
        c.lastmodifiedbyid AS updated_by_id,
        coalesce(r.name, c.seven20__record_type_name__c) as record_type
    FROM {{ var('source_database') }}."contact" c
    LEFT JOIN  {{ var('source_database') }}."recordtype" r ON r.id = c.seven20__record_type_name__c
),

internal_ids AS (
    SELECT 
        DISTINCT id AS person_id,
        atlas_id AS atlas_person_id
    FROM "{{ this.schema }}"."people"
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
        CASE WHEN record_type = 'Candidate' THEN 'personal' 
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
    WHERE fc.email IS NOT NULL
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
        CASE WHEN record_type = 'Candidate' THEN 'personal' 
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
    WHERE fc.phone IS NOT NULL
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
    ip.atlas_person_id,
    u.atlas_id AS created_by_atlas_id
FROM merged m
LEFT JOIN internal_ids ip USING (person_id)
LEFT JOIN "{{ this.schema }}"."users" u ON u.id = m.created_by_id
