{{ config(
    materialized='table',
    alias='sjt_person_identities_fix',
    tags=["seven20"]
) }}

WITH filtered_contacts AS (
    SELECT 
        p.id AS person_id,
        p.atlas_id AS atlas_person_id,
        p.first_name,
        p.last_name,
        c.mobilephone,  
        c.email, 
        TRIM(BOTH '/;' FROM REPLACE(REPLACE(REPLACE(c.seven20__linkedin__c, 'https://', ''), 'http://', ''), 'www.', '')) AS linkedin_url, 
        p.created_by_atlas_id,
        p.created_at,
        p.updated_at,
        coalesce(r.name, c.seven20__record_type_name__c) as record_type
    FROM {{ this.schema }}.sjt_people_fix p
    LEFT JOIN {{ var('source_database') }}."contact" c ON c.id = p.id  
    LEFT JOIN {{ var('source_database') }}."recordtype" r ON r.id = c.seven20__record_type_name__c
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
        CASE WHEN record_type = 'Candidate' THEN 'personal' ELSE 'corporate' END AS identity_type_type,
        'EmailPersonIdentity' AS class_type,
        fc.person_id,
        fc.atlas_person_id,
        'migration' AS source,
        fc.created_by_atlas_id,
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
            substring(md5(fc.mobilephone || getdate()::text), 1, 8) || '-' ||
            substring(md5(fc.mobilephone || getdate()::text), 9, 4) || '-' ||
            substring(md5(fc.mobilephone || getdate()::text), 13, 4) || '-' ||
            substring(md5(fc.mobilephone || getdate()::text), 17, 4) || '-' ||
            substring(md5(fc.mobilephone || getdate()::text), 21, 12)
        ) AS atlas_id,
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
        'false' AS hidden,
        'true' AS verified,
        created_at,
        updated_at
    FROM filtered_contacts fc
    WHERE fc.mobilephone IS NOT NULL
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
        fc.atlas_person_id,
        'migration' AS source,
        fc.created_by_atlas_id,
        'false' AS hidden,
        'true' AS verified,
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
)

SELECT 
    m.*
FROM merged m
