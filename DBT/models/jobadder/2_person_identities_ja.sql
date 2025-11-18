-- File: models/intercity/person_identities_ja.sql

{{ config(
    materialized='table',
    alias='person_identities_ja'
) }}

-- Step 1: Find values that appear EXACTLY ONCE in the entire table
WITH one_time_emails AS (
    -- Only get emails that appear exactly once in the entire database
    SELECT email AS value, MIN(contactid) AS person_id
    FROM {{ var('source_database') }}."contact"
    WHERE email IS NOT NULL AND TRIM(email) <> '' AND deleted = FALSE
    GROUP BY email
    HAVING COUNT(*) = 1
),

internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id 
    FROM 
        {{ref('1_people_ja')}}
),

-- Combine all phone numbers (both phone and mobile) into a single analysis
all_phone_numbers AS (
    -- Get all phone numbers from the phone column
    SELECT 
        contactid AS person_id,
        {{phone_norm('phone')}} AS phone_number
    FROM {{ var('source_database') }}."contact"
    WHERE phone IS NOT NULL AND TRIM(phone) <> '' AND deleted = FALSE
    
    UNION ALL
    
    -- Get all phone numbers from the mobile column
    SELECT 
        contactid AS person_id,
        {{phone_norm('mobile')}}AS phone_number
    FROM {{ var('source_database') }}."contact"
    WHERE mobile IS NOT NULL AND TRIM(mobile) <> '' AND deleted = FALSE
),

one_time_phones AS (
    -- Only get phone numbers that appear exactly once across both phone and mobile columns
    SELECT phone_number AS value, MIN(person_id) AS person_id
    FROM all_phone_numbers
    GROUP BY phone_number
    HAVING COUNT(*) = 1
),

one_time_linkedins AS (
    -- Only get LinkedIn URLs that appear exactly once in the entire database
    SELECT regexp_replace(linkedinurl, '^(https?://)?(www\.)?', '') AS value, MIN(contactid) AS person_id
    FROM {{ var('source_database') }}."contact"
    WHERE linkedinurl IS NOT NULL AND TRIM(linkedinurl) <> '' AND deleted = FALSE
    GROUP BY regexp_replace(linkedinurl, '^(https?://)?(www\.)?', '')
    HAVING COUNT(*) = 1
),

merged AS (
SELECT
    {{ atlas_uuid('value') }} AS atlas_id,
    value,
    true AS favourite,
    true AS active,
    'email' AS type,
    CASE WHEN {{ is_personal_email('value') }} THEN 'personal' ELSE 'corporate' END AS identity_type_type,
    'EmailPersonIdentity' AS class_type,
    '{{ var('agency_id') }}' AS agency_id,
    person_id,
    '{{ var('master_id') }}' AS created_by_id,
    false AS hidden,
    true AS verified,
    ip.atlas_person_id,
    'migration' AS source
FROM one_time_emails
INNER JOIN 
    internal_persons ip USING (person_id) 

UNION ALL

SELECT
    {{ atlas_uuid('value') }} AS atlas_id,
    value,
    true AS favourite,
    true AS active,
    'phone' AS type,
    'personal' AS identity_type_type,
    'PhonePersonIdentity' AS class_type,
    '{{ var('agency_id') }}' AS agency_id,
    person_id,
    '{{ var('master_id') }}' AS created_by_id,
    false AS hidden,
    true AS verified,
    ip.atlas_person_id,
    'migration' AS source
FROM one_time_phones
INNER JOIN 
    internal_persons ip USING (person_id) 

UNION ALL

SELECT
    {{ atlas_uuid('value') }} AS atlas_id,
    value,
    true AS favourite,
    true AS active,
    'linkedin' AS type,
    'personal' AS identity_type_type,
    'LinkedinPersonIdentity' AS class_type,
    '{{ var('agency_id') }}' AS agency_id,
    person_id,
    '{{ var('master_id') }}' AS created_by_id,
    false AS hidden,
    true AS verified,
    ip.atlas_person_id,
    'migration' AS source
FROM one_time_linkedins
INNER JOIN 
    internal_persons ip USING (person_id))

SELECT * FROM merged 
ORDER BY atlas_person_id 
