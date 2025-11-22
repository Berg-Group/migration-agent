{{ config(
    materialized = 'table',
    alias        = 'custom_attribute_options_forgetalent',
    tags         = ['forgetalent']
) }}

with internal_attributes as (
    select atlas_id, name
    from  {{ ref('custom_attributes_ft') }}
),

candidate as (
    select distinct trim(lower(occupation)) as value, 
           'Candidate' as attribute_name
    from {{ var('source_database') }}."bh_usercontact"
),

qualification as (
    select distinct customtext21 as value,
           'Qualification' as attribute_name
    from {{ var('source_database') }}."bh_useradditionalcustomfields"
),

employment_preference as (
    select distinct employmentpreference as value,
         'Employment Preference' as attribute_name
    from {{ var('source_database') }}."bh_usercontact"
),

immediately_available as (
    select distinct customtext1 as value,
           'Immediately Available' as attribute_name
    from {{ var('source_database') }}."bh_usercontact"
),

notice_period as ( 
    select distinct customtext4 as value,
           'Notice Period' as attribute_name
    from  {{ var('source_database') }}."bh_usercontact"
),

status as (
    select distinct status as value,
            'Status' as attribute_name 
    from {{var('source_database')}}."bh_candidate"
    where status IN ('Active', 'Inactive', 'Placed', 'DNU')
),

all_values_raw as (
              select * from candidate
    union all select * from qualification
    union all select * from employment_preference
    union all select * from immediately_available
    union all select * from notice_period
    union all select * from status

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
)

    select
        {{atlas_uuid('atlas_attribute_id || value || attribute_name')}} as atlas_id,
        atlas_attribute_id,
        value,
        row_number() over (partition by atlas_attribute_id order by value) as position,
        '{{ var("agency_id") }}'       as agency_id,
        '2025-08-04T00:00:00'          as created_at,
        '2025-08-04T00:00:00'          as updated_at
    from all_values
    order by atlas_attribute_id, position