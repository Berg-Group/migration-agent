{{ config(
    materialized='table',
    alias='person_identities_ezekia'
) }}

WITH internal_people AS (
    SELECT
        id,
        atlas_id
    FROM {{ ref('people_ezekia') }}
),

emails AS (
    SELECT
        e.id::text || '_email' as id,
        {{atlas_uuid('e.id::text || e.address')}} AS atlas_id,
        TO_CHAR(e.created_at::TIMESTAMP, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(e.updated_at::TIMESTAMP, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        e.emailable_id AS person_id,
        pm.atlas_id AS atlas_person_id,
        'email' AS type,
        {{email_norm('e.address')}} AS value,
        CASE WHEN type = 'work' THEN 'corporate' ELSE 'personal' END AS identity_type_type,
        'EmailPersonIdentity' AS class_type,
        'migration' AS source,
        '{{ var("master_id") }}' AS created_by_id,
        FALSE AS bounced,
        FALSE AS hidden,
        CASE WHEN e.is_default = 1 THEN TRUE ELSE FALSE END AS favourite,
        TRUE AS active,
        ROW_NUMBER() OVER (PARTITION BY {{email_norm('e.address')}} ORDER BY created_at)
    FROM {{ var("source_database") }}.emails e
    INNER JOIN internal_people pm ON e.emailable_id = pm.id
    WHERE e.emailable_type = 'person'
),

phones AS (
    SELECT
        p.id || '_phone' AS id,
        {{atlas_uuid('p.id::text || p.number::text')}} AS atlas_id,
        TO_CHAR(p.created_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(p.updated_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        p.phoneable_id AS person_id,
        pm.atlas_id AS atlas_person_id,
        'phone' AS type,
        {{phone_norm('p.number_searchable')}} AS value,
        CASE WHEN p.type = 'work' THEN 'corporate' ELSE 'personal' END AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        'migration' AS source,
        '{{ var("master_id") }}' AS created_by_id,
        FALSE AS bounced,
        FALSE AS hidden,
        CASE WHEN p.is_default = 1 THEN TRUE ELSE FALSE END AS favourite,
        TRUE AS active,
        ROW_NUMBER() OVER (PARTITION BY p.number_searchable ORDER BY p.created_at)
    FROM {{ var("source_database") }}.phones p
    INNER JOIN internal_people pm ON p.phoneable_id = pm.id
    WHERE p.phoneable_type = 'person'
),

links AS (
    SELECT
        l.id::text || '_linkedin' AS id,
        {{atlas_uuid('l.id::text || l.url')}} AS atlas_id,
        TO_CHAR(l.created_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS created_at,
        TO_CHAR(l.updated_at::timestamp, 'YYYY-MM-DD"T"00:00:00') AS updated_at,
        l.linkable_id AS person_id,
        pm.atlas_id AS atlas_person_id,
        'linkedin' AS type,
        {{linkedin_norm('l.url')}} AS value,
        'LinkedinPersonIdentity' AS class_type,
        'migration' AS source,
        '{{ var("master_id") }}' AS created_by_id,
        FALSE AS bounced,
        FALSE AS hidden,
        FALSE AS favourite,
        TRUE  AS active,
        ROW_NUMBER() OVER (PARTITION BY {{linkedin_norm('l.url')}}  ORDER BY created_at)
    FROM {{ var("source_database") }}.links l
    INNER JOIN internal_people pm ON l.linkable_id = pm.id
    WHERE l.linkable_type = 'person' AND l.type = 'linkedin'
),

merged AS (
SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    created_at,
    updated_at,
    type,
    value,
    COALESCE(identity_type_type, 'personal') AS identity_type_type,
    class_type,
    source,
    created_by_id,
    '{{var("master_id")}}' AS created_by_atlas_id,
    '{{var("master_id")}}' AS updated_by_atlas_id,
    bounced,
    hidden,
    favourite,
    active
FROM emails
WHERE row_number = 1

UNION ALL

SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    created_at,
    updated_at,
    type,
    value,
    identity_type_type,
    class_type,
    source,
    created_by_id,
    '{{var("master_id")}}' AS created_by_atlas_id,
    '{{var("master_id")}}' AS updated_by_atlas_id,
    bounced,
    hidden,
    favourite,
    active
FROM phones 
WHERE row_number = 1

UNION ALL

SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    created_at,
    updated_at,
    type,
    value,
    'personal' AS identity_type_type,
    class_type,
    source,
    created_by_id,
    '{{var("master_id")}}' AS created_by_atlas_id,
    '{{var("master_id")}}' AS updated_by_atlas_id,
    bounced,
    hidden,
    favourite,
    active
FROM links
WHERE row_number = 1)

SELECT * FROM merged 
ORDER BY atlas_person_id 

