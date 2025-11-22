{{ config(
    materialized='table',
    alias='person_custom_attribute_values_vincere_partial_vertical'
) }}

WITH candidates AS (
    SELECT
        s.id AS id,
        s.candidate_id AS person_id,  -- Mapped to common person_id
        s.vertical_id AS custom_attribute_id,  -- Set to source vertical_id
        '861406b7-6be0-3d08-dbb2-395af45ebf67' AS external_option_id,  -- Fixed UUID for option_id
        '{{ var("agency_id") }}' AS agency_id,  -- Dynamically sourced agency_id from vars
        CURRENT_TIMESTAMP AS created_at,  -- Dynamic timestamp for record creation
        CURRENT_TIMESTAMP AS updated_at,  -- Dynamic timestamp for record update
        pv.external_id AS external_person_id,  -- Retrieved from people_vincere table
        cao.external_id AS external_custom_attribute_id  -- Retrieved external_id from custom_attribute_options_vincere
    FROM
        {{ var("source_database") }}."public_candidate_industry" s
    LEFT JOIN
        "{{ this.schema }}"."people_vincere" pv  -- Properly quoted schema and table name
        ON s.candidate_id = pv.id  -- Corrected join condition
    LEFT JOIN
        "{{ this.schema }}"."custom_attribute_options_vincere" cao  -- Properly quoted schema and table name
        ON s.vertical_id = cao.id  -- Join on vertical_id to get external_custom_attribute_id
    WHERE
        s.vertical_id IS NOT NULL  -- Exclude rows with NULL vertical_id
),

hashed_data AS (
    SELECT
        id,
        person_id,
        external_person_id,
        custom_attribute_id,
        external_option_id,
        external_custom_attribute_id,  -- Include the new external_custom_attribute_id field
        agency_id,
        created_at,
        updated_at
    FROM
        candidates
)

SELECT
    *
FROM
    hashed_data
