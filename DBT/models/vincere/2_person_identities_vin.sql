{{ config(materialized = 'table', alias = 'person_identities_vincere') }}

WITH person_map AS (
    SELECT id AS person_id,
           link_id AS external_id,
           NULLIF(company_contact_id,'')::BIGINT AS contact_id
    FROM {{ ref('1_people_vincere') }}
),

candidate_emails AS (
    SELECT c.id::varchar AS person_id,
           c.user_account_id,
           'personal'       AS person_type,
           LOWER(TRIM(c.email)) AS value
    FROM {{ var('source_database') }}.public_candidate c
    JOIN person_map p ON p.person_id = c.id
    WHERE TRIM(c.email) <> '' AND c.email IS NOT NULL
    UNION ALL
    SELECT c.id::varchar,
           c.user_account_id,
           'personal',
           LOWER(TRIM(c.email2))
    FROM {{ var('source_database') }}.public_candidate c
    JOIN person_map p ON p.person_id = c.id
    WHERE TRIM(c.email2) <> '' AND c.email2 IS NOT NULL
    UNION ALL
    SELECT c.id::varchar,
           c.user_account_id,
           'personal',
           LOWER(TRIM(c.work_email))
    FROM {{ var('source_database') }}.public_candidate c
    JOIN person_map p ON p.person_id = c.id
    WHERE TRIM(c.work_email) <>'' AND c.work_email IS NOT NULL
),

contact_emails AS (
    SELECT COALESCE(c.id::varchar,p.person_id::varchar) AS person_id,
           cc.user_account_id,
           'corporate'       AS person_type,
           LOWER(TRIM(cc.email)) AS value
    FROM {{ var('source_database') }}.public_contact cc
    LEFT JOIN {{ var('source_database') }}.public_candidate c
           ON c.external_id = cc.external_id
          AND c.deleted_reason IS NULL
          AND c.deleted_timestamp IS NULL
    JOIN person_map p ON p.contact_id = cc.id
                      OR (p.external_id = cc.external_id AND p.contact_id IS NULL)
    WHERE cc.deleted_timestamp IS NULL
      AND cc.email IS NOT NULL 
      AND TRIM(cc.email) <> ''
    UNION ALL
    SELECT COALESCE(c.id::varchar,p.person_id::varchar),
           cc.user_account_id,
           'corporate',
           LOWER(TRIM(cc.personal_email))
    FROM {{ var('source_database') }}.public_contact cc
    LEFT JOIN {{ var('source_database') }}.public_candidate c
           ON c.external_id = cc.external_id
          AND c.deleted_reason IS NULL
          AND c.deleted_timestamp IS NULL
    JOIN person_map p ON p.contact_id = cc.id
                      OR (p.external_id = cc.external_id AND p.contact_id IS NULL)
    WHERE cc.deleted_timestamp IS NULL
      AND cc.personal_email IS NOT NULL
      AND TRIM(cc.personal_email) <> ''
),

clean_emails AS (
    SELECT person_id,user_account_id,person_type,value FROM candidate_emails
    UNION ALL
    SELECT person_id,user_account_id,person_type,value FROM contact_emails
    WHERE POSITION('@' IN value) > 0
      AND value NOT ILIKE '%vincere.io%'
      AND value NOT ILIKE '%no_email%'
),

rank_emails AS (
    SELECT person_id,
           user_account_id,
           person_type,
           value,
           ROW_NUMBER() OVER (PARTITION BY person_id,value
                              ORDER BY CASE WHEN person_type='personal' THEN 1 ELSE 2 END) AS rn
    FROM clean_emails
),

dedup_emails AS (
    SELECT person_id,user_account_id,person_type,value
    FROM rank_emails
    WHERE rn = 1
),

candidate_linkedin AS (
    SELECT c.id::varchar AS person_id,
           c.user_account_id,
           'personal',
           REGEXP_REPLACE(
               REGEXP_REPLACE(
                   REGEXP_SUBSTR(c.linked_in_profile,'(https?://)?(www\\.)?linkedin\\.com/[^\\s?]+'),
                   '^https?://(www\\.)?',''),
               '/$','') AS value
    FROM {{ var('source_database') }}.public_candidate c
    JOIN person_map p ON p.person_id = c.id
    WHERE c.linked_in_profile IS NOT NULL
),

contact_linkedin AS (
    SELECT COALESCE(c.id::varchar,p.person_id::varchar) AS person_id,
           cc.user_account_id,
           'corporate',
           REGEXP_REPLACE(
               REGEXP_REPLACE(
                   REGEXP_SUBSTR(c.linked_in_profile,'(https?://)?(www\\.)?linkedin\\.com/[^\\s?]+'),
                   '^https?://(www\\.)?',''),
               '/$','') AS value
    FROM {{ var('source_database') }}.public_contact cc
    LEFT JOIN {{ var('source_database') }}.public_candidate c
           ON c.external_id = cc.external_id
          AND c.deleted_reason IS NULL
          AND c.deleted_timestamp IS NULL
    JOIN person_map p ON p.contact_id = cc.id
                      OR (p.external_id = cc.external_id AND p.contact_id IS NULL)
    WHERE cc.deleted_timestamp IS NULL
      AND cc.linkedin IS NOT NULL
),

clean_linkedin AS (
    SELECT person_id,user_account_id,'personal' AS person_type,value FROM candidate_linkedin
    UNION ALL
    SELECT person_id,user_account_id,'corporate',value FROM contact_linkedin
    WHERE POSITION('linkedin.com/in/' IN value) > 0
),

rank_linkedin AS (
    SELECT person_id,
           user_account_id,
           person_type,
           value,
           ROW_NUMBER() OVER (PARTITION BY person_id,value
                              ORDER BY CASE WHEN person_type='personal' THEN 1 ELSE 2 END) AS rn
    FROM clean_linkedin
),

dedup_linkedin AS (
    SELECT person_id,user_account_id,person_type,value
    FROM rank_linkedin
    WHERE rn = 1
),

phones_raw AS (
    SELECT c.id::varchar AS person_id,
           c.user_account_id,
           'personal',
           {{phone_norm('c.phone')}} AS value
    FROM {{ var('source_database') }}.public_candidate c
    JOIN person_map p ON p.person_id = c.id
    WHERE c.phone IS NOT NULL
    UNION ALL
    SELECT c.id::varchar,
           c.user_account_id,
           'personal',
           {{phone_norm('c.phone2')}} AS value
    FROM {{ var('source_database') }}.public_candidate c
    JOIN person_map p ON p.person_id = c.id
    WHERE c.phone2 IS NOT NULL
    UNION ALL
    SELECT c.id::varchar,
           c.user_account_id,
           'personal',
           {{phone_norm('c.work_phone')}} AS value
    FROM {{ var('source_database') }}.public_candidate c
    JOIN person_map p ON p.person_id = c.id
    WHERE c.work_phone IS NOT NULL
),

candidate_phones AS (
    SELECT person_id,
           user_account_id,
           'personal' AS person_type, 
           value
    FROM phones_raw
    WHERE LENGTH(value) >= 8
),

contact_phones_raw AS (
    SELECT COALESCE(c.id::varchar,p.person_id::varchar) AS person_id,
           cc.user_account_id,
           'corporate',
           {{phone_norm('cc.phone')}} AS value
    FROM {{ var('source_database') }}.public_contact cc
    LEFT JOIN {{ var('source_database') }}.public_candidate c
           ON c.external_id = cc.external_id
          AND c.deleted_reason IS NULL
          AND c.deleted_timestamp IS NULL
    JOIN person_map p ON p.contact_id = cc.id
                      OR (p.external_id = cc.external_id AND p.contact_id IS NULL)
    WHERE cc.deleted_timestamp IS NULL
      AND cc.phone IS NOT NULL
),

contact_phones AS (
    SELECT person_id,
           user_account_id,
           'corporate' AS person_type,
           value
    FROM contact_phones_raw
    WHERE LENGTH(value) >= 8
),

clean_phones AS (
    SELECT person_id,user_account_id,person_type,value FROM candidate_phones
    UNION ALL
    SELECT person_id,user_account_id,person_type,value FROM contact_phones
),

rank_phones AS (
    SELECT person_id,
           user_account_id,
           person_type,
           value,
           ROW_NUMBER() OVER (PARTITION BY person_id,value
                              ORDER BY CASE WHEN person_type='personal' THEN 1 ELSE 2 END) AS rn
    FROM clean_phones
),

dedup_phones AS (
    SELECT person_id,user_account_id,person_type,value
    FROM rank_phones
    WHERE rn = 1
),

internal_persons AS (
    SELECT id::varchar AS person_id,
           atlas_id::varchar AS atlas_person_id
    FROM {{ ref('1_people_vincere') }}
),

emails_i AS (
    SELECT person_id,user_account_id,person_type,value,
           ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY value)=1 AS favourite,
           'email' AS type,
           'EmailPersonIdentity' AS class_type
    FROM dedup_emails
),

linkedin_i AS (
    SELECT person_id,user_account_id,person_type,value,
           FALSE AS favourite,
           'linkedin' AS type,
           'LinkedinPersonIdentity' AS class_type
    FROM dedup_linkedin
),

phones_i AS (
    SELECT person_id,user_account_id,person_type,value,
           ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY value)=1 AS favourite,
           'phone' AS type,
           'PhonePersonIdentity' AS class_type
    FROM dedup_phones
),

all_identities AS (
    SELECT * FROM emails_i
    UNION ALL
    SELECT * FROM linkedin_i
    UNION ALL
    SELECT * FROM phones_i
),

rank_identities AS (
    SELECT person_id,
           user_account_id,
           person_type,
           value,
           favourite,
           type,
           class_type,
           ROW_NUMBER() OVER (
               PARTITION BY value
               ORDER BY CASE WHEN type='email' THEN 1
                             WHEN type='phone' THEN 2
                             ELSE 3 END,
                        CASE WHEN person_type='personal' THEN 1 ELSE 2 END
           ) AS rn
    FROM all_identities
),

deduped_all_identities AS (
    SELECT person_id,user_account_id,person_type,value,favourite,type,class_type
    FROM rank_identities
    WHERE rn = 1
)

SELECT {{ atlas_uuid('person_id || value || type') }} AS atlas_id,
       value,
       favourite,
       TRUE AS active,
       type,
       person_type AS identity_type_type,
       class_type,
       '{{ var("agency_id") }}' AS agency_id,
       person_id,
       'migration' AS source,
       user_account_id AS created_by_id,
       COALESCE(u.atlas_id,'{{ var("master_id") }}') AS created_by_atlas_id,
       FALSE AS hidden,
       TRUE AS verified,
       ip.atlas_person_id
FROM deduped_all_identities
LEFT JOIN internal_persons ip USING (person_id)
LEFT JOIN {{ ref('users_vin') }} u ON u.id = deduped_all_identities.user_account_id
ORDER BY ip.atlas_person_id