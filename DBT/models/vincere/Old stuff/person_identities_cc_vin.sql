-- File: models/vincere/person_identities_vin_contact.sql

{{ config(
    materialized='table',
    alias='person_identities_cc_vincere'
) }}

-- Creating person_identities table based on contact information in public_contact

WITH filtered_contacts AS (
    SELECT *
    FROM {{ var('source_database') }}."contact"
),

internal_ids AS (
    SELECT 
        DISTINCT id AS person_id,
        atlas_id AS atlas_person_id
    FROM
        "{{ this.schema }}"."people_cc_vincere"
),

email_identities AS (
    -- Primary email entries, excluding emails with "No_email"
    SELECT
        lower(
            substring(md5(public_contact.email || getdate()::text), 1, 8) || '-' ||
            substring(md5(public_contact.email || getdate()::text), 9, 4) || '-' ||
            substring(md5(public_contact.email || getdate()::text), 13, 4) || '-' ||
            substring(md5(public_contact.email || getdate()::text), 17, 4) || '-' ||
            substring(md5(public_contact.email || getdate()::text), 21, 12)
        ) AS id,
        
        public_contact.email AS value,
        true AS favourite,
        true AS active,
        'email' AS type,
        'corporate' AS identity_type_type,
        'EmailPersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        'cc' || public_contact.id::text AS person_id,
        'migration' AS source,
        public_contact.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_contacts AS public_contact
    WHERE 
        position('@' in public_contact.email) > 0
        AND position('No_email' in public_contact.email) = 0
),

phone_identities AS (
    -- Phone entries for phone, mobile_phone, and switchboard_phone
    SELECT
        lower(
            substring(md5(regexp_replace(public_contact.phone, '^07', '+447') || getdate()::text), 1, 8) || '-' ||
            substring(md5(regexp_replace(public_contact.phone, '^07', '+447') || getdate()::text), 9, 4) || '-' ||
            substring(md5(regexp_replace(public_contact.phone, '^07', '+447') || getdate()::text), 13, 4) || '-' ||
            substring(md5(regexp_replace(public_contact.phone, '^07', '+447') || getdate()::text), 17, 4) || '-' ||
            substring(md5(regexp_replace(public_contact.phone, '^07', '+447') || getdate()::text), 21, 12)
        ) AS id,
        
        regexp_replace(public_contact.phone, '^07', '+447') AS value,
        true AS favourite,
        true AS active,
        'phone' AS type,
        'corporate' AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        'cc' || public_contact.id::text AS person_id,
        'migration' AS source,
        public_contact.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_contacts AS public_contact
    WHERE public_contact.phone IS NOT NULL

    UNION ALL

    SELECT
        lower(
            substring(md5(regexp_replace(public_contact.mobile_phone, '^07', '+447') || getdate()::text), 1, 8) || '-' ||
            substring(md5(regexp_replace(public_contact.mobile_phone, '^07', '+447') || getdate()::text), 9, 4) || '-' ||
            substring(md5(regexp_replace(public_contact.mobile_phone, '^07', '+447') || getdate()::text), 13, 4) || '-' ||
            substring(md5(regexp_replace(public_contact.mobile_phone, '^07', '+447') || getdate()::text), 17, 4) || '-' ||
            substring(md5(regexp_replace(public_contact.mobile_phone, '^07', '+447') || getdate()::text), 21, 12)
        ) AS id,
        
        regexp_replace(public_contact.mobile_phone, '^07', '+447') AS value,
        false AS favourite,
        true AS active,
        'phone' AS type,
        'corporate' AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        'cc' || public_contact.id::text AS person_id,
        'migration' AS source,
        public_contact.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_contacts AS public_contact
    WHERE public_contact.mobile_phone IS NOT NULL

    UNION ALL

    SELECT
        lower(
            substring(md5(regexp_replace(public_contact.switchboard_phone, '^07', '+447') || getdate()::text), 1, 8) || '-' ||
            substring(md5(regexp_replace(public_contact.switchboard_phone, '^07', '+447') || getdate()::text), 9, 4) || '-' ||
            substring(md5(regexp_replace(public_contact.switchboard_phone, '^07', '+447') || getdate()::text), 13, 4) || '-' ||
            substring(md5(regexp_replace(public_contact.switchboard_phone, '^07', '+447') || getdate()::text), 17, 4) || '-' ||
            substring(md5(regexp_replace(public_contact.switchboard_phone, '^07', '+447') || getdate()::text), 21, 12)
        ) AS id,
        
        regexp_replace(public_contact.switchboard_phone, '^07', '+447') AS value,
        false AS favourite,
        true AS active,
        'phone' AS type,
        'corporate' AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        'cc' || public_contact.id::text AS person_id,
        'migration' AS source,
        public_contact.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_contacts AS public_contact
    WHERE public_contact.switchboard_phone IS NOT NULL
),

linkedin_identities AS (
    -- LinkedIn entries with cleansing
    SELECT
        lower(
            substring(md5(public_contact.linkedin || getdate()::text), 1, 8) || '-' ||
            substring(md5(public_contact.linkedin || getdate()::text), 9, 4) || '-' ||
            substring(md5(public_contact.linkedin || getdate()::text), 13, 4) || '-' ||
            substring(md5(public_contact.linkedin || getdate()::text), 17, 4) || '-' ||
            substring(md5(public_contact.linkedin || getdate()::text), 21, 12)
        ) AS id,
        
        regexp_replace(regexp_replace(public_contact.linkedin, '/$', '', 'g'), '.*(linkedin\\.com)', 'linkedin.com') AS value,
        false AS favourite,
        true AS active,
        'linkedin' AS type,
        NULL AS identity_type_type,
        'LinkedinPersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        'cc' || public_contact.id::text AS person_id,
        'migration' AS source,
        public_contact.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_contacts AS public_contact
    WHERE position('linkedin.com/in/' in public_contact.linkedin) > 0
),

merged AS (SELECT *
FROM email_identities
UNION ALL
SELECT *
FROM phone_identities
UNION ALL
SELECT *
FROM linkedin_identities)

SELECT m.*, i.atlas_person_id 
FROM 
    merged m
LEFT JOIN internal_ids i USING (person_id)
