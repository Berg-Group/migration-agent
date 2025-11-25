{{ config(
    materialized='table',
    alias='person_identities_ff',
    tags=["filefinder"]
) }}

WITH source_person_identities AS (
    SELECT 
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || pe.idperson_eaddress::text") }} AS atlas_id,
        pf.id AS person_id,
        pf.atlas_id AS atlas_person_id,
        'email' AS type, 
        {{ email_norm('e.CommValue') }} AS value,
        CASE
            WHEN pt.Value IN ('Work', 'Company', 'Business') THEN 'corporate'
            WHEN {{ is_personal_email('e.CommValue') }} THEN 'personal'
            ELSE 'corporate'
        END AS identity_type_type,
        'EmailPersonIdentity' AS class_type,
        TO_CHAR(pe.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(pe.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        'migration' AS source,
        FALSE AS hidden,
        FALSE AS bounced,
        TRUE AS active,
        TRUE AS verified,
        CASE
            WHEN pe.isdefault = TRUE THEN TRUE
            ELSE FALSE
        END AS favourite,
        p.iduser AS created_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.Person p 
    INNER JOIN {{ this.schema }}.people_ff pf ON pf.id = p.idPerson
    INNER JOIN {{ var('source_database') }}.Person_EAddress pe ON pe.idPerson = p.idPerson
    INNER JOIN {{ var('source_database') }}.EAddress e ON e.idEAddress = pe.idEAddress
    LEFT JOIN {{ var('source_database') }}.PersonCommunicationType pt ON pt.idPersonCommunicationType = pe.idPersonCommunicationType
    LEFT JOIN {{ this.schema }}.users_ff u ON u.id = p.iduser
    WHERE e.CommValue IS NOT NULL AND e.CommValue LIKE '%@%.%'

    UNION ALL

    SELECT 
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || pe.idperson_eaddress::text") }} AS atlas_id,
        pf.id AS person_id,
        pf.atlas_id AS atlas_person_id, 
        'phone' AS type,
        {{ phone_norm('e.CommValue') }} AS value, 
        CASE
            WHEN pt.Value IN ('Home', 'Direct Line', 'Private', 'Home 2') THEN 'personal'
            ELSE 'corporate'
        END AS identity_type_type,
        'PhonePersonIdentity' AS class_type,
        TO_CHAR(pe.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(pe.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        'migration' AS source,
        FALSE AS hidden,
        FALSE AS bounced,
        TRUE AS active,
        TRUE AS verified,
        CASE
            WHEN pt.Value IN ('Home', 'Direct Line', 'Private', 'Home 2') THEN TRUE
            ELSE FALSE
        END AS favourite,
        p.iduser AS created_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.Person p
    INNER JOIN {{ this.schema }}.people_ff pf ON pf.id = p.idPerson
    INNER JOIN {{ var('source_database') }}.Person_EAddress pe ON pe.idPerson = p.idPerson
    INNER JOIN {{ var('source_database') }}.EAddress e ON e.idEAddress = pe.idEAddress
    LEFT JOIN {{ var('source_database') }}.PersonCommunicationType pt ON pt.idPersonCommunicationType = pe.idPersonCommunicationType 
    LEFT JOIN {{ this.schema }}.users_ff u ON u.id = p.iduser
    WHERE LENGTH(e.CommValue) > 7 AND e.CommValue SIMILAR TO '%[0-9]{3}%'
    AND e.CommValue NOT LIKE '%@%.%' AND e.CommValue NOT LIKE '%www.%' AND 
    e.CommValue NOT LIKE '%http%' AND e.CommValue NOT LIKE '%.com%'

    UNION ALL

    SELECT 
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || pe.idperson_eaddress::text") }} AS atlas_id,
        pf.id AS person_id,
        pf.atlas_id AS atlas_person_id,
        'linkedin' AS type,
        {{ linkedin_norm('e.CommValue') }} AS value,
        'corporate' AS identity_type_type,
        'LinkedinPersonIdentity' AS class_type,
        TO_CHAR(pe.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(pe.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        'migration' AS source,
        FALSE AS hidden,
        FALSE AS bounced,
        TRUE AS active,
        TRUE AS verified,
        FALSE AS favourite,
        p.iduser AS created_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.Person p 
    INNER JOIN {{ this.schema }}.people_ff pf ON pf.id = p.idPerson
    INNER JOIN {{ var('source_database') }}.Person_EAddress pe ON pe.idPerson = p.idPerson
    INNER JOIN {{ var('source_database') }}.EAddress e ON e.idEAddress = pe.idEAddress
    LEFT JOIN {{ var('source_database') }}.PersonCommunicationType pt ON pt.idPersonCommunicationType = pe.idPersonCommunicationType 
    LEFT JOIN {{ this.schema }}.users_ff u ON u.id = p.iduser
    WHERE e.CommValue IS NOT NULL AND e.CommValue NOT LIKE '%@%.%' AND e.CommValue ILIKE '%linkedin.com%'

    UNION ALL

    SELECT 
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || pe.idperson_eaddress::text") }} AS atlas_id,
        pf.id AS person_id,
        pf.atlas_id AS atlas_person_id,
        'website' AS type,
        {{ website_norm('e.CommValue') }} AS value,
        'corporate' AS identity_type_type,
        'SocialPersonIdentity' AS class_type,
        TO_CHAR(pe.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(pe.rowtimestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        'migration' AS source,
        FALSE AS hidden,
        FALSE AS bounced,
        TRUE AS active,
        TRUE AS verified,
        FALSE AS favourite,
        p.iduser AS created_by_id,
        COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
        '{{ var('agency_id') }}' AS agency_id
    FROM {{ var('source_database') }}.Person p 
    INNER JOIN {{ this.schema }}.people_ff pf ON pf.id = p.idPerson
    INNER JOIN {{ var('source_database') }}.Person_EAddress pe ON pe.idPerson = p.idPerson
    INNER JOIN {{ var('source_database') }}.EAddress e ON e.idEAddress = pe.idEAddress
    LEFT JOIN {{ var('source_database') }}.PersonCommunicationType pt ON pt.idPersonCommunicationType = pe.idPersonCommunicationType 
    LEFT JOIN {{ this.schema }}.users_ff u ON u.id = p.iduser
    WHERE e.CommValue IS NOT NULL AND LOWER(e.CommValue) NOT LIKE '%linkedin.com%' AND e.CommValue NOT LIKE '%@%.%' AND
    (LOWER(e.CommValue) LIKE 'http%' OR LOWER(e.CommValue) LIKE 'www.%' OR LOWER(e.CommValue) LIKE '%.com%' OR LOWER(e.CommValue) LIKE '%.in%') 
)
SELECT
    atlas_id,
    person_id,
    atlas_person_id,
    type,
    value,
    identity_type_type,
    class_type,
    created_at,
    updated_at,
    source,
    hidden,
    bounced,
    active,
    verified,
    favourite,
    created_by_id,
    created_by_atlas_id,
    agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY type, value
            ORDER BY
                (identity_type_type IS NULL OR identity_type_type = '') ASC,
                created_at
        ) AS rn
    FROM source_person_identities
) deduped
WHERE rn = 1
ORDER BY atlas_person_id