{{ config(materialized='table', alias='person_identities_rcrm') }}

{# ───── grab YYYY-MM-DD from dbt_project.yml ───── #}
{% set iso_midnight = var('date') ~ 'T00:00:00' %}

{% set db = var('source_database') %}

WITH match_contact AS (

    SELECT
        ct.slug                                              AS contact_slug, 
        COALESCE(c.slug, 'cc' || ct.slug)                    AS person_id

    FROM {{ db }}.contact_data ct
    LEFT JOIN {{ db }}.candidate_data c
           ON  {{ email_norm('c.email') }}            = {{ email_norm('ct.email') }}
           OR  {{ phone_norm('c.contact_number') }}   = {{ phone_norm('ct.contact_number') }}
           OR  {{ linkedin_norm('c.profile_linkedin') }}
               = {{ linkedin_norm('ct.profile_linkedin') }}
),


candidate_emails AS (
    SELECT slug                              AS person_id,
           {{ email_norm('email') }}         AS value,
           'email'                           AS type,
           'EmailPersonIdentity'             AS class_type,
           1 AS priority,
           'personal' AS identity_type_type
    FROM {{ db }}.candidate_data
    WHERE email IS NOT NULL AND TRIM(email) <> ''
),
candidate_phones AS (
    SELECT slug,
           {{ phone_norm('contact_number') }},
           'phone',
           'PhonePersonIdentity',
           1,
           'personal'
    FROM {{ db }}.candidate_data
    WHERE contact_number IS NOT NULL AND TRIM(contact_number) <> ''
),
candidate_linkedin AS (
    SELECT slug,
           {{ linkedin_norm('profile_linkedin') }},
           'linkedin',
           'LinkedinPersonIdentity',
           1,
           'personal'
    FROM {{ db }}.candidate_data
    WHERE profile_linkedin IS NOT NULL AND TRIM(profile_linkedin) <> ''
),
candidate_github AS (
    SELECT slug,
           TRIM(BOTH '/' FROM
                REPLACE(
                    REPLACE(
                        REPLACE(LOWER(TRIM(profile_github)), 'https://', ''),
                        'http://', ''),
                    'www.', ''))               AS value,
           'github',
           'SocialPersonIdentity',
           1,
           'personal'
    FROM {{ db }}.candidate_data
    WHERE profile_github IS NOT NULL AND TRIM(profile_github) <> ''
),

contact_emails AS (
    SELECT mc.person_id,
           {{ email_norm('ct.email') }},
           'email',
           'EmailPersonIdentity',
           2,
           'corporate'
    FROM {{ db }}.contact_data ct
    JOIN match_contact mc ON mc.contact_slug = ct.slug
    WHERE ct.email IS NOT NULL AND TRIM(ct.email) <> ''
),
contact_phones AS (
    SELECT mc.person_id,
           {{ phone_norm('ct.contact_number') }},
           'phone',
           'PhonePersonIdentity',
           2,
           'corporate'
    FROM {{ db }}.contact_data ct
    JOIN match_contact mc ON mc.contact_slug = ct.slug
    WHERE ct.contact_number IS NOT NULL AND TRIM(ct.contact_number) <> ''
),
contact_linkedin AS (
    SELECT mc.person_id,
           {{ linkedin_norm('ct.profile_linkedin') }},
           'linkedin',
           'LinkedinPersonIdentity',
           2,
           'personal'
    FROM {{ db }}.contact_data ct
    JOIN match_contact mc ON mc.contact_slug = ct.slug
    WHERE ct.profile_linkedin IS NOT NULL AND TRIM(ct.profile_linkedin) <> ''
),


all_identities_raw AS (
    SELECT * FROM candidate_emails UNION ALL
    SELECT * FROM candidate_phones UNION ALL
    SELECT * FROM candidate_linkedin UNION ALL
    SELECT * FROM candidate_github UNION ALL
    SELECT * FROM contact_emails UNION ALL
    SELECT * FROM contact_phones UNION ALL
    SELECT * FROM contact_linkedin
),
dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY LOWER(value) ORDER BY priority) AS rn
        FROM all_identities_raw
    ) r
    WHERE rn = 1
),

joined AS (
    SELECT d.person_id,
           p.atlas_id          AS atlas_person_id,
           d.value,
           d.type,
           d.class_type,
           d.identity_type_type
    FROM dedup d
    INNER JOIN {{ ref('3_people_rcrm') }} p
           ON p.id = d.person_id
)


SELECT
    {{ atlas_uuid('value') }}      AS atlas_id,
    value,
    TRUE                           AS favourite,
    TRUE                           AS active,
    type,
    identity_type_type,
    class_type,
    person_id,
    atlas_person_id,
    '{{ iso_midnight }}'           AS created_at,
    '{{ iso_midnight }}'           AS updated_at,
    'migration'                    AS source
FROM joined
WHERE value IS NOT NULL AND TRIM(value) <> ''
ORDER BY person_id, type 