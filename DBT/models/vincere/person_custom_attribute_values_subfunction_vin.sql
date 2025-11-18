{{ config(
    materialized='table',
    alias='person_custom_attribute_values_vincere_partial_subfunction'
) }}

WITH candidates AS (
    SELECT
        s.id AS id,
        s.candidate_id AS person_id,  -- Mapped to common person_id
        s.sub_functional_expertise_id AS custom_attribute_id,  -- Corrected column name
        '{{ var("agency_id") }}' AS agency_id,  -- Dynamically sourced agency_id from vars
        CURRENT_TIMESTAMP AS created_at,  -- Dynamic timestamp for record creation
        CURRENT_TIMESTAMP AS updated_at,  -- Dynamic timestamp for record update
        pv.external_id AS external_person_id,  -- Retrieved from people_vincere table
        cao.attribute_id AS external_option_id,  -- Retrieved from custom_attribute_options_vincere table
        cao.external_id AS external_attribute_id  -- New field: Retrieved external_id from custom_attribute_options_vincere
    FROM
        {{ var("source_database") }}."public_candidate_functional_expertise" s
    LEFT JOIN
        "{{ this.schema }}"."people_vincere" pv  -- Properly quoted schema and table name
        ON s.candidate_id = pv.id  -- Corrected join condition
    LEFT JOIN
        "{{ this.schema }}"."custom_attribute_options_vincere" cao  -- Properly quoted schema and table name
        ON s.sub_functional_expertise_id = cao.id  -- Corrected join condition
    WHERE
        s.sub_functional_expertise_id IS NOT NULL  -- Exclude rows with NULL sub_functional_expertise_id
),

contacts AS (
    SELECT
        c.id AS id,
        c.contact_id AS person_id,  -- Mapped to common person_id
        c.sub_functional_expertise_id AS custom_attribute_id,  -- Corrected column name
        '{{ var("agency_id") }}' AS agency_id,  -- Dynamically sourced agency_id from vars
        CURRENT_TIMESTAMP AS created_at,  -- Dynamic timestamp for record creation
        CURRENT_TIMESTAMP AS updated_at,  -- Dynamic timestamp for record update
        pv.external_id AS external_person_id,  -- Retrieved from people_vincere table
        cao.attribute_id AS external_option_id,  -- Retrieved from custom_attribute_options_vincere table
        cao.external_id AS external_attribute_id  -- New field: Retrieved external_id from custom_attribute_options_vincere
    FROM
        {{ var("source_database") }}."public_contact_functional_expertise" c
    LEFT JOIN
        "{{ this.schema }}"."people_vincere" pv  -- Properly quoted schema and table name
        ON c.contact_id = pv.id  -- Corrected join condition
    LEFT JOIN
        "{{ this.schema }}"."custom_attribute_options_vincere" cao  -- Properly quoted schema and table name
        ON c.sub_functional_expertise_id = cao.id  -- Corrected join condition
    WHERE
        c.sub_functional_expertise_id IS NOT NULL  -- Exclude rows with NULL sub_functional_expertise_id
),

combined_data AS (
    SELECT * FROM candidates
    UNION ALL
    SELECT * FROM contacts
),

hashed_data AS (
    SELECT
        id,
        person_id,
        external_person_id,
        custom_attribute_id,
        external_option_id,
        external_attribute_id,  -- Include the new external_attribute_id field
        agency_id,
        created_at,
        updated_at
    FROM
        combined_data
)

SELECT
    *
FROM
    hashed_data
