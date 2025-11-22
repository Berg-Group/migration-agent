{{ config(
    materialized = 'table',
    alias        = 'custom_attribute_options_blackwood',
    tags         = ['blackwood']
) }}

with internal_attributes as (
    select atlas_id, name
    from {{ ref('custom_attributes_blackwood') }}
),

position_prev as (
    select distinct coalesce(lp.position_description, lp.position_code) as value,
           'position previous' as attribute_name
    from {{ var('source_database') }}.candidate_position_previous pp
    left join {{ var('source_database') }}.library_positions lp
           on lp.position_code = pp.position
),
position_cur as (
    select distinct coalesce(lp.position_description, lp.position_code) as value,
           'position current' as attribute_name
    from {{ var('source_database') }}.candidate_position_current pc
    left join {{ var('source_database') }}.library_positions lp
           on lp.position_code = pc.position
),
c_language as (
    select distinct coalesce(ll.language_description, ll.language_code) as value,
           'language' as attribute_name
    from {{ var('source_database') }}.candidate_language cl
    left join {{ var('source_database') }}.library_languages ll
           on ll.language_code = cl.language
),
nationality as (
    select distinct coalesce(ln.nationality_description, ln.nationality_code) as value,
           'nationality' as attribute_name
    from {{ var('source_database') }}.candidate_nationality n
    left join {{ var('source_database') }}.library_nationalities ln
           on ln.nationality_code = n.nationality
),
product_prev as (
    select distinct lp.product_description as value,
           'product previous' as attribute_name
    from {{ var('source_database') }}.candidate_product_previous pp
    left join {{ var('source_database') }}.library_products lp
           on lp.product_code = pp.product
),
product_cur as (
    select distinct lp.product_description as value,
           'product current' as attribute_name
    from {{ var('source_database') }}.candidate_product_current pc
    left join {{ var('source_database') }}.library_products lp
           on lp.product_code = pc.product
),
dep_prev as (
    select distinct ld.department_description as value,
           'department previous' as attribute_name
    from {{ var('source_database') }}.candidate_department_previous dp
    left join {{ var('source_database') }}.library_departments ld
           on ld.department_code = dp.department
),
dep_cur as (
    select distinct ld.department_description as value,
           'department current' as attribute_name
    from {{ var('source_database') }}.candidate_department_current dc
    left join {{ var('source_database') }}.library_departments ld
           on ld.department_code = dc.department
),

job_product AS (
    select distinct lp.product_description as value,
            'job product' as attribute_name 
    from {{ var('source_database')}}.library_products lp 
),

job_sector AS(
    select distinct ls.sector_description as value,
            'job sector' as attribute_name
    from {{ var('source_database')}}.library_sectors ls
),

job_practice AS (
    select distinct practicearea_description as value,
            'job practice' as attribute_name
     from {{ var('source_database')}}.library_practicearea
),

job_function AS (
    select distinct position_description as value,
            'job function' as attribute_name 
     from {{ var('source_database')}}.library_positions
        
),

all_values_raw as (
    select * from position_prev
    union all select * from position_cur
    union all select * from c_language
    union all select * from nationality
    union all select * from product_prev
    union all select * from product_cur
    union all select * from dep_prev
    union all select * from dep_cur
    union all select * from job_product 
    union all select * from job_sector
    union all select * from job_practice 
    union all select * from job_function
),

all_values as (
    select
        av.value,
        av.attribute_name,
        ia.atlas_id as atlas_attribute_id
    from all_values_raw av
    left join internal_attributes ia
           on ia.name = av.attribute_name
    where av.value is not null
      and av.value <> ''
),

final as (
    select
        md5(atlas_attribute_id || '::' || value) as id,
        atlas_attribute_id,
        value,
        row_number() over (partition by atlas_attribute_id order by value) as position
    from all_values
)

select
    lower(
        substring(md5(id::text),  1, 8) || '-' ||
        substring(md5(id::text),  9, 4) || '-' ||
        substring(md5(id::text), 13, 4) || '-' ||
        substring(md5(id::text), 17, 4) || '-' ||
        substring(md5(id::text), 21, 12)
    )                              as atlas_id,
    atlas_attribute_id,
    value,
    position,
    '{{ var("agency_id") }}'       as agency_id,
    '2025-06-25T00:00:00'          as created_at,
    '2025-06-25T00:00:00'          as updated_at
from final
order by atlas_attribute_id, position