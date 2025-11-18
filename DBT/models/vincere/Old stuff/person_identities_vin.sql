-- File: models/vincere/person_identities_vin.sql

{{ config(
    materialized='table',
    alias='person_identities_vincere_old'
) }}

-- Migration-specific logic: Creating person_identities table based on contact information in public_candidate

WITH filtered_candidates AS (
    SELECT *
    FROM {{ var('source_database') }}."candidate"
    WHERE deleted_reason IS NULL
),

internal_ids AS (
    SELECT 
        DISTINCT id AS person_id,
        atlas_id AS atlas_person_id
    FROM
        "{{ this.schema }}"."people_vincere"
),

email_identities AS (
    -- Primary email entries
    SELECT
        lower(
            substring(md5(public_candidate.email || getdate()::text), 1, 8) || '-' ||
            substring(md5(public_candidate.email || getdate()::text), 9, 4) || '-' ||
            substring(md5(public_candidate.email || getdate()::text), 13, 4) || '-' ||
            substring(md5(public_candidate.email || getdate()::text), 17, 4) || '-' ||
            substring(md5(public_candidate.email || getdate()::text), 21, 12)
        ) AS id,
        
        public_candidate.email AS value,
        true AS favourite,
        true AS active,
        'email' AS type,
        'personal' AS identity_type_type,
        'EmailPersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        public_candidate.id AS person_id,
        'migration' AS source,
        public_candidate.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_candidates AS public_candidate
    WHERE 
        position('@' in public_candidate.email) > 0
        AND position('vincere.io' in public_candidate.email) = 0
        AND position('No_email' in public_candidate.email) = 0

    UNION ALL

    -- Additional entries for email2
    SELECT
        lower(
            substring(md5(public_candidate.email2 || getdate()::text), 1, 8) || '-' ||
            substring(md5(public_candidate.email2 || getdate()::text), 9, 4) || '-' ||
            substring(md5(public_candidate.email2 || getdate()::text), 13, 4) || '-' ||
            substring(md5(public_candidate.email2 || getdate()::text), 17, 4) || '-' ||
            substring(md5(public_candidate.email2 || getdate()::text), 21, 12)
        ) AS id,
        
        public_candidate.email2 AS value,
        false AS favourite,
        true AS active,
        'email' AS type,
        'personal' AS identity_type_type,
        'EmailPersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        public_candidate.id AS person_id,
        'migration' AS source,
        public_candidate.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_candidates AS public_candidate
    WHERE 
        public_candidate.email2 IS NOT NULL
        AND position('@' in public_candidate.email2) > 0
        AND position('vincere.io' in public_candidate.email2) = 0
        AND position('No_email' in public_candidate.email2) = 0

    UNION ALL

    -- LinkedIn entries
    SELECT
        lower(
            substring(md5(public_candidate.linked_in_profile || getdate()::text), 1, 8) || '-' ||
            substring(md5(public_candidate.linked_in_profile || getdate()::text), 9, 4) || '-' ||
            substring(md5(public_candidate.linked_in_profile || getdate()::text), 13, 4) || '-' ||
            substring(md5(public_candidate.linked_in_profile || getdate()::text), 17, 4) || '-' ||
            substring(md5(public_candidate.linked_in_profile || getdate()::text), 21, 12)
        ) AS id,
        
        regexp_replace(regexp_replace(public_contact.linkedin, '/$', '', 'g'), '.*(linkedin\\.com)', 'linkedin.com') AS value,
        false AS favourite,
        true AS active,
        'linkedin' AS type,
        NULL AS identity_type_type,
        'LinkedinPersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        public_candidate.id AS person_id,
        'migration' AS source,
        public_candidate.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_candidates AS public_candidate
    WHERE 
        position('linkedin.com/in/' in public_candidate.linked_in_profile) > 0

    UNION ALL

    -- Phone entries
    SELECT
        lower(
            substring(md5(regexp_replace(public_candidate.phone, '^07', '+447') || getdate()::text), 1, 8) || '-' ||
            substring(md5(regexp_replace(public_candidate.phone, '^07', '+447') || getdate()::text), 9, 4) || '-' ||
            substring(md5(regexp_replace(public_candidate.phone, '^07', '+447') || getdate()::text), 13, 4) || '-' ||
            substring(md5(regexp_replace(public_candidate.phone, '^07', '+447') || getdate()::text), 17, 4) || '-' ||
            substring(md5(regexp_replace(public_candidate.phone, '^07', '+447') || getdate()::text), 21, 12)
        ) AS id,
        
        regexp_replace(public_candidate.phone, '^07', '+447') AS value,
        true AS favourite,
        true AS active,
        'phone' AS type,
        NULL AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        public_candidate.id AS person_id,
        'migration' AS source,
        public_candidate.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_candidates AS public_candidate
    WHERE public_candidate.phone IS NOT NULL

    UNION ALL

    -- Work phone entries
    SELECT
        lower(
            substring(md5(regexp_replace(public_candidate.work_phone, '^07', '+447') || getdate()::text), 1, 8) || '-' ||
            substring(md5(regexp_replace(public_candidate.work_phone, '^07', '+447') || getdate()::text), 9, 4) || '-' ||
            substring(md5(regexp_replace(public_candidate.work_phone, '^07', '+447') || getdate()::text), 13, 4) || '-' ||
            substring(md5(regexp_replace(public_candidate.work_phone, '^07', '+447') || getdate()::text), 17, 4) || '-' ||
            substring(md5(regexp_replace(public_candidate.work_phone, '^07', '+447') || getdate()::text), 21, 12)
        ) AS id,
        
        regexp_replace(public_candidate.work_phone, '^07', '+447') AS value,
        false AS favourite,
        true AS active,
        'phone' AS type,
        NULL AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        public_candidate.id AS person_id,
        'migration' AS source,
        public_candidate.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_candidates AS public_candidate
    WHERE public_candidate.work_phone IS NOT NULL

    UNION ALL

    -- Secondary phone entries
    SELECT
        lower(
            substring(md5(regexp_replace(public_candidate.phone2, '^07', '+447') || getdate()::text), 1, 8) || '-' ||
            substring(md5(regexp_replace(public_candidate.phone2, '^07', '+447') || getdate()::text), 9, 4) || '-' ||
            substring(md5(regexp_replace(public_candidate.phone2, '^07', '+447') || '-' || getdate()::text), 13, 4) || '-' ||
            substring(md5(regexp_replace(public_candidate.phone2, '^07', '+447') || getdate()::text), 17, 4) || '-' ||
            substring(md5(regexp_replace(public_candidate.phone2, '^07', '+447') || getdate()::text), 21, 12)
        ) AS id,
        
        regexp_replace(public_candidate.phone2, '^07', '+447') AS value,
        false AS favourite,
        true AS active,
        'phone' AS type,
        NULL AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        '{{ var('agency_id') }}' AS agency_id,
        public_candidate.id AS person_id,
        'migration' AS source,
        public_candidate.user_account_id AS created_by_id,
        'false' AS hidden,
        'true' AS verified

    FROM filtered_candidates AS public_candidate
    WHERE public_candidate.phone2 IS NOT NULL
)

SELECT e.*, i.atlas_person_id
FROM email_identities e
LEFT JOIN internal_ids i USING (person_id)
