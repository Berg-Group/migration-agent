-- File: models/intercity/person_identities_ja.sql

{{ config(
    materialized='table',
    alias='person_identities_jobadder'
) }}

WITH base AS (
    SELECT
        {{ var('source_database') }}."contact".contactid     AS person_id,
        {{ var('source_database') }}."contact".email         AS email,
        {{ var('source_database') }}."contact".phone         AS phone,
        regexp_replace({{ var('source_database') }}."contact".phone, '^07', '+447') AS formatted_phone,
        {{ var('source_database') }}."contact".mobile        AS mobile,
        regexp_replace({{ var('source_database') }}."contact".mobile, '^07', '+447') AS formatted_mobile,
        {{ var('source_database') }}."contact".linkedinurl   AS linkedinurl,
        regexp_replace(
            {{ var('source_database') }}."contact".linkedinurl,
            '^(https?://)?(www\.)?', 
            ''
        ) AS formatted_linkedinurl,
        {{ var('source_database') }}."contact".iscandidateonly AS iscandidateonly
    FROM {{ var('source_database') }}."contact"
    WHERE deleted = FALSE
    AND COALESCE(inactive, FALSE) = FALSE
),

people_ja_lookup AS (
    SELECT
        id AS person_id,
        atlas_id
    FROM {{ ref('1_people_ja') }}
),

identities AS (
    --------------------------------------------------------------------------------
    --  Email
    --------------------------------------------------------------------------------
    SELECT
        lower(
            substring(md5('{{ var('clientName') }}' || email), 1, 8) || '-' ||
            substring(md5('{{ var('clientName') }}' || email), 9, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || email), 13, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || email), 17, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || email), 21, 12)
        ) AS id,
        email                      AS value,
        true                       AS favourite,
        true                       AS active,
        'email'                    AS type,
        CASE
            WHEN iscandidateonly THEN 'personal'
            ELSE 'corporate'
        END                        AS identity_type_type,
        'EmailPersonIdentity'      AS class_type,
        '{{ var('agency_id') }}'   AS agency_id,
        person_id,
        'migration'                AS source,
        '{{ var('master_id') }}'   AS created_by_id,
        false                      AS hidden,
        true                       AS verified
    FROM base
    WHERE email IS NOT NULL
      AND email <> ''                      -- exclude blank emails

    UNION ALL

    --------------------------------------------------------------------------------
    --  Phone
    --------------------------------------------------------------------------------
    SELECT
        lower(
            substring(md5('{{ var('clientName') }}' || formatted_phone), 1, 8) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_phone), 9, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_phone), 13, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_phone), 17, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_phone), 21, 12)
        ) AS id,
        formatted_phone           AS value,
        true                      AS favourite,
        true                      AS active,
        'phone'                   AS type,
        'personal'                AS identity_type_type,
        'PhonePersonIdentity'     AS class_type,
        '{{ var('agency_id') }}'  AS agency_id,
        person_id,
        'migration'               AS source,
        '{{ var('master_id') }}'  AS created_by_id,
        false                     AS hidden,
        true                      AS verified
    FROM base
    WHERE formatted_phone IS NOT NULL
      AND formatted_phone <> ''            -- exclude blank phones

    UNION ALL

    --------------------------------------------------------------------------------
    --  Mobile
    --------------------------------------------------------------------------------
    SELECT
        lower(
            substring(md5('{{ var('clientName') }}' || formatted_mobile), 1, 8) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_mobile), 9, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_mobile), 13, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_mobile), 17, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_mobile), 21, 12)
        ) AS id,
        formatted_mobile          AS value,
        true                      AS favourite,
        true                      AS active,
        'phone'                   AS type,
        'personal'                AS identity_type_type,
        'PhonePersonIdentity'     AS class_type,
        '{{ var('agency_id') }}'  AS agency_id,
        person_id,
        'migration'               AS source,
        '{{ var('master_id') }}'  AS created_by_id,
        false                     AS hidden,
        true                      AS verified
    FROM base
    WHERE formatted_mobile IS NOT NULL
      AND formatted_mobile <> ''           -- exclude blank mobiles

    UNION ALL

    --------------------------------------------------------------------------------
    --  LinkedIn
    --------------------------------------------------------------------------------
    SELECT
        lower(
            substring(md5('{{ var('clientName') }}' || formatted_linkedinurl), 1, 8) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_linkedinurl), 9, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_linkedinurl), 13, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_linkedinurl), 17, 4) || '-' ||
            substring(md5('{{ var('clientName') }}' || formatted_linkedinurl), 21, 12)
        ) AS id,
        formatted_linkedinurl     AS value,
        true                      AS favourite,
        true                      AS active,
        'linkedin'                AS type,
        NULL                      AS identity_type_type,
        'LinkedinPersonIdentity'  AS class_type,
        '{{ var('agency_id') }}'  AS agency_id,
        person_id,
        'migration'               AS source,
        '{{ var('master_id') }}'  AS created_by_id,
        false                     AS hidden,
        true                      AS verified
    FROM base
    WHERE formatted_linkedinurl IS NOT NULL
      AND formatted_linkedinurl <> ''      -- exclude blank LinkedIn
)

SELECT
    i.*,
    p.atlas_id AS atlas_person_id
FROM identities i
LEFT JOIN people_ja_lookup p
       ON i.person_id = p.person_id
