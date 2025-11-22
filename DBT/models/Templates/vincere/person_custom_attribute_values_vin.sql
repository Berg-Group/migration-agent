{{ config(
    materialized = 'table',
    alias        = 'person_custom_attribute_values_vin',
    tags         = ['vincere']
) }}

WITH internal_persons AS (
    SELECT
        id::varchar  AS person_id,
        atlas_id     AS atlas_person_id
    FROM {{ ref('1_people_vincere') }}
),

internal_options AS (
    SELECT
        id                 AS old_option_id,
        atlas_attribute_id,
        atlas_id           AS atlas_option_id
    FROM {{ ref('12_custom_attribute_options_vin') }}
),

candidate_expertise AS (
    SELECT
        candidate_id::varchar        AS person_id,
        functional_expertise_id      AS old_option_id
    FROM {{ var('source_database') }}.candidate_functional_expertise

    UNION

    SELECT
        candidate_id::varchar        AS person_id,
        sub_functional_expertise_id  AS old_option_id
    FROM {{ var('source_database') }}.candidate_functional_expertise
    WHERE sub_functional_expertise_id IS NOT NULL
),

mapped AS (        
    SELECT DISTINCT
        ce.person_id,
        io.atlas_attribute_id,
        io.atlas_option_id
    FROM candidate_expertise ce
    JOIN internal_options   io
      ON ce.old_option_id = io.old_option_id
)

SELECT
    {{ atlas_uuid("ip.atlas_person_id || mapped.atlas_attribute_id || mapped.atlas_option_id") }}  AS atlas_id,
    ip.person_id,
    ip.atlas_person_id,
    mapped.atlas_attribute_id    AS atlas_custom_attribute_id,
    mapped.atlas_option_id,
    '2025-07-09 00:00:00'::timestamp AS created_at,
    '2025-07-09 00:00:00'::timestamp AS updated_at
FROM mapped
JOIN internal_persons ip
  ON ip.person_id = mapped.person_id