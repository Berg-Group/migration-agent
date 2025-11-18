{{ config(
    materialized='table',
    alias='projects_custom_attribute_values_blackwood',
    tags=["blackwood"]
) }}

WITH internal_projects AS (
    SELECT 
        id AS project_id,
        atlas_id AS atlas_project_id 
    FROM {{ ref('projects_blackwood') }}
),  

internal_options AS (
    SELECT 
        atlas_attribute_id,
        atlas_id AS option_id,
        atlas_attribute_id AS atlas_custom_attribute_id,
        atlas_id AS atlas_option_id,
        value
    FROM {{ ref('custom_attribute_options_blackwood') }}
),



attribute_id_map AS (
    SELECT 
        name AS attribute_name,
        atlas_id AS atlas_custom_attribute_id,
        of 
    FROM {{ref('custom_attributes_blackwood')}}
),

job_function AS (
    SELECT jobid AS project_id, 
          'job function' AS attribute_name, 
          lp.position_description AS value
    FROM 
        {{ var('source_database')}}.jobfile jf
    INNER JOIN  {{ var('source_database')}}.library_positions lp ON lp.position_code = jf.jobfunction),

job_product AS (
    SELECT jobid AS project_id,
            'job product' AS attribute_name,
           lp.product_description AS value
    FROM {{ var('source_database')}}.jobfile jf
    INNER JOIN {{ var('source_database')}}.library_products lp 
        ON lp.product_code = jf.jobproduct
),

job_practice AS (
    SELECT jobid AS project_id,
            'job practice' AS attribute_name,
            lpa.practicearea_description AS value
    FROM {{ var('source_database')}}.jobfile jf
    INNER JOIN {{ var('source_database')}}.library_practicearea   lpa 
        ON lpa.practicearea_code = jf.jobpractice
),

job_sector AS (
    SELECT jobid AS project_id,
            'job sector' AS attribute_name,
            ls.sector_description AS value
    FROM {{ var('source_database')}}.jobfile jf
    INNER JOIN   {{ var('source_database')}}.library_sectors ls ON ls.sector_code = jf.jobsector
),


all_values AS (
    SELECT * FROM job_function
    UNION ALL SELECT * FROM job_product
    UNION ALL SELECT * FROM job_practice
    UNION ALL SELECT * FROM job_sector
),

mapped as (
    select
        ip.project_id,
        ip.atlas_project_id,
        aim.atlas_custom_attribute_id,
        io.atlas_option_id
    FROM all_values v
    LEFT JOIN internal_projects  ip USING (project_id) 
    LEFT JOIN attribute_id_map  aim on aim.attribute_name = v.attribute_name
        AND of = 'project'
    LEFT JOIN internal_options  io
           ON io.value = v.value
           AND io.atlas_custom_attribute_id = aim.atlas_custom_attribute_id
    WHERE TRIM(v.value) <> '' AND v.value IS NOT NULL
)

select  
    {{ atlas_uuid('m.atlas_project_id || m.atlas_custom_attribute_id || m.atlas_option_id') }} as atlas_id,
    m.project_id,
    m.atlas_project_id,
    m.atlas_custom_attribute_id,
    m.atlas_option_id,
    '2025-06-25T00:00:00' as created_at,
    '2025-06-25T00:00:00' as updated_at
from mapped m
where m.project_id is not null
  and m.atlas_option_id is not null
