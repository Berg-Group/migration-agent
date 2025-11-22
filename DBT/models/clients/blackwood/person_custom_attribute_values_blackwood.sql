{{ config(
    materialized='table',
    alias='person_custom_attribute_values_blackwood',
    tags=['blackwood']
) }}

WITH internal_persons AS (
    SELECT id AS person_id, atlas_id AS atlas_person_id
    FROM {{ ref('people_blackwood') }}
),

attribute_id_map AS (
    SELECT name AS attribute_name, atlas_id AS atlas_custom_attribute_id
    FROM {{ ref('custom_attributes_blackwood') }}
    WHERE of = 'person'
),

internal_options AS (
    SELECT atlas_attribute_id, atlas_id AS atlas_option_id, value
    FROM {{ ref('custom_attribute_options_blackwood') }}
),

position_prev AS (
    SELECT pp.cref, 'position previous' AS attribute_name, COALESCE(lp.position_description, lp.position_code) AS value
    FROM {{ var('source_database') }}.candidate_position_previous pp
    LEFT JOIN {{ var('source_database') }}.library_positions lp ON lp.position_code = pp.position
),

position_cur AS (
    SELECT pc.cref, 'position current' AS attribute_name, COALESCE(lp.position_description, lp.position_code) AS value
    FROM {{ var('source_database') }}.candidate_position_current pc
    LEFT JOIN {{ var('source_database') }}.library_positions lp ON lp.position_code = pc.position
),

c_language AS (
    SELECT cl.cref, 'language' AS attribute_name, COALESCE(ll.language_description, ll.language_code) AS value
    FROM {{ var('source_database') }}.candidate_language cl
    LEFT JOIN {{ var('source_database') }}.library_languages ll ON ll.language_code = cl.language
),

nationality AS (
    SELECT n.cref, 'nationality' AS attribute_name, COALESCE(ln.nationality_description, ln.nationality_code) AS value
    FROM {{ var('source_database') }}.candidate_nationality n
    LEFT JOIN {{ var('source_database') }}.library_nationalities ln ON ln.nationality_code = n.nationality
),

product_prev AS (
    SELECT pp.cref, 'product previous' AS attribute_name, COALESCE(lp.product_description, lp.product_code) AS value
    FROM {{ var('source_database') }}.candidate_product_previous pp
    LEFT JOIN {{ var('source_database') }}.library_products lp ON lp.product_code = pp.product
),

product_cur AS (
    SELECT pc.cref, 'product current' AS attribute_name, COALESCE(lp.product_description, lp.product_code) AS value
    FROM {{ var('source_database') }}.candidate_product_current pc
    LEFT JOIN {{ var('source_database') }}.library_products lp ON lp.product_code = pc.product
),

dep_prev AS (
    SELECT dp.cref, 'department previous' AS attribute_name, COALESCE(ld.department_description, ld.department_code) AS value
    FROM {{ var('source_database') }}.candidate_department_previous dp
    LEFT JOIN {{ var('source_database') }}.library_departments ld ON ld.department_code = dp.department
),

dep_cur AS (
    SELECT dc.cref, 'department current' AS attribute_name, COALESCE(ld.department_description, ld.department_code) AS value
    FROM {{ var('source_database') }}.candidate_department_current dc
    LEFT JOIN {{ var('source_database') }}.library_departments ld ON ld.department_code = dc.department
),

all_values AS (
    SELECT * FROM position_prev
    UNION ALL SELECT * FROM position_cur
    UNION ALL SELECT * FROM c_language
    UNION ALL SELECT * FROM nationality
    UNION ALL SELECT * FROM product_prev
    UNION ALL SELECT * FROM product_cur
    UNION ALL SELECT * FROM dep_prev
    UNION ALL SELECT * FROM dep_cur
),

mapped AS (
    SELECT DISTINCT
        ip.person_id,
        ip.atlas_person_id,
        aim.atlas_custom_attribute_id,
        io.atlas_option_id
    FROM all_values v
    LEFT JOIN internal_persons ip ON ip.person_id = v.cref
    LEFT JOIN attribute_id_map aim ON aim.attribute_name = v.attribute_name
    LEFT JOIN internal_options io
      ON io.atlas_attribute_id = aim.atlas_custom_attribute_id
     AND lower(trim(io.value)) = lower(trim(v.value))
    WHERE v.value IS NOT NULL AND v.value <> ''
)

SELECT
    {{ atlas_uuid('m.atlas_person_id || m.atlas_custom_attribute_id || m.atlas_option_id') }} AS atlas_id,
    m.person_id,
    m.atlas_person_id,
    m.atlas_custom_attribute_id,
    m.atlas_option_id,
    '2025-06-25T00:00:00' AS created_at,
    '2025-06-25T00:00:00' AS updated_at
FROM mapped m
WHERE m.person_id IS NOT NULL
  AND m.atlas_option_id IS NOT NULL
