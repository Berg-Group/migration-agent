--candidate_functional_expertise connects to functional_expertise
{{ config(
    materialized='table',
    alias='person_custom_attribute_values_subfunction_vin'
) }}

WITH custom_attributes AS (
    SELECT
        atlas_id AS atlas_custom_attribute_id,
        name AS attribute_name
    FROM "{{ this.schema }}"."custom_attributes_vincere"
    WHERE attribute_name = 'subfunction'
),

attribute_options AS (
    SELECT
        id AS option_source_id,
        atlas_id AS atlas_option_id
    FROM "{{ this.schema }}"."custom_attribute_options_vincere"
    WHERE name = 'subfunction'
),

candidates AS (
    SELECT
        s.id AS id,
        s.candidate_id AS person_id,
        s.functional_expertise_id AS option_source_id,
        pv.atlas_id AS atlas_person_id,
        ao.atlas_option_id,
        ca.atlas_custom_attribute_id,
        '{{ var("agency_id") }}' AS agency_id,
        '{{ var("date") }}T00:00:00' AS created_at,
        '{{ var("date") }}T00:00:00' AS updated_at,
        {{ atlas_uuid("'vincere_person_attribute_value_subfunction_' || '{{ var(\"clientName\") }}' || s.id::text") }} AS atlas_id
    FROM
        {{ var("source_database") }}."candidate_functional_expertise" s
    LEFT JOIN
        "{{ this.schema }}"."people_vincere" pv
        ON s.candidate_id = pv.id
    LEFT JOIN
        attribute_options ao
        ON s.functional_expertise_id = ao.option_source_id
    CROSS JOIN
        custom_attributes ca
    WHERE
        s.functional_expertise_id IS NOT NULL
),

contacts AS (
    SELECT
        c.id AS id,
        c.contact_id AS person_id,
        c.functional_expertise_id AS option_source_id,
        pv.atlas_id AS atlas_person_id,
        ao.atlas_option_id,
        ca.atlas_custom_attribute_id,
        '{{ var("agency_id") }}' AS agency_id,
        '{{ var("date") }}T00:00:00' AS created_at,
        '{{ var("date") }}T00:00:00' AS updated_at,
        {{ atlas_uuid("'vincere_person_attribute_value_subfunction_' || '{{ var(\"clientName\") }}' || c.id::text") }} AS atlas_id
    FROM
        {{ var("source_database") }}."contact_functional_expertise" c
    LEFT JOIN
        "{{ this.schema }}"."people_vincere" pv
        ON c.contact_id = pv.id
    LEFT JOIN
        attribute_options ao
        ON c.functional_expertise_id = ao.option_source_id
    CROSS JOIN
        custom_attributes ca
    WHERE
        c.functional_expertise_id IS NOT NULL
),

combined_data AS (
    SELECT * FROM candidates
    UNION ALL
    SELECT * FROM contacts
)

SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    atlas_custom_attribute_id,
    atlas_option_id,
    agency_id,
    created_at,
    updated_at,
    'Person' AS class_type,
    '{{ var("master_id") }}' AS created_by_atlas_id
FROM
    combined_data
WHERE
    atlas_person_id IS NOT NULL
    AND atlas_option_id IS NOT NULL
