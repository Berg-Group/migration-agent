{{ config(
    materialized = 'table',
    alias        = 'company_relationships_blackwood',
    tags         = ['blackwood']
) }}

with mapping as (
    select original_company_id,
           new_company_id
    from {{ ref('companies_mapping_blackwood') }}
),

companies as (
    select id,
           atlas_id,
           name,
           parent_company_id
    from {{ ref('companies_blackwood') }}
),

parent_child_relationships as (
    select
        c.name,
        1                                           as relationship,
        c.id                                        as subsidiary_id,
        p.id                                        as company_id,
        c.atlas_id                                  as atlas_subsidiary_id,
        p.atlas_id                                  as atlas_company_id,
        'parent'                                    as relationship_type,
        '2025-05-21T00:00:00Z'                      as created_at,
        ('parent_'||p.atlas_id||'_'||c.atlas_id)    as uuid_input
    from companies c
    join companies p on p.id = c.parent_company_id
),

date_parser as (
    select sponsorlevel1companyid,
           level2companyid,
           investmentmade,
           split_part(investmentmade,'/',1)         as month_part,
           split_part(investmentmade,'/',2)         as year_part
    from {{ var('source_database') }}.sponsoradvisor
    where investmentmade is not null
      and investmentmade <> ''
      and level2companyid is not null
      and level2companyid <> 0
),

parsed_dates as (
    select sponsorlevel1companyid,
           level2companyid,
           case
               when length(year_part)=4
                and try_cast(year_part as integer) between 1900 and 2100
                and try_cast(month_part as integer) between 1 and 12
               then year_part||'-'||lpad(month_part,2,'0')||'-01T00:00:00Z'
               else '2025-05-21T00:00:00Z'
           end                                       as iso_date
    from date_parser
),

investment_raw as (
    select
        sa.sponsorlevel1companyid::varchar          as l1_raw,
        'l2-'||sa.level2companyid                   as l2_raw,
        pd.iso_date                                 as created_at
    from {{ var('source_database') }}.sponsoradvisor sa
    join parsed_dates pd
      on pd.sponsorlevel1companyid = sa.sponsorlevel1companyid
     and pd.level2companyid        = sa.level2companyid
    where sa.level2companyid is not null
      and sa.level2companyid <> 0
      and sa.sponsorlevel1companyid is not null
),

investment_map as (
    select
        coalesce(mp_l2.new_company_id, ir.l2_raw) as subsidiary_id,
        coalesce(mp_l1.new_company_id, ir.l1_raw) as company_id,
        ir.created_at
    from investment_raw ir
    left join mapping mp_l2 on mp_l2.original_company_id = ir.l2_raw
    left join mapping mp_l1 on mp_l1.original_company_id = ir.l1_raw
),

investment_relationships as (
    select
        c_sub.name,
        1                                           as relationship,
        im.subsidiary_id,
        im.company_id,
        c_sub.atlas_id                              as atlas_subsidiary_id,
        c_par.atlas_id                              as atlas_company_id,
        'investment'                               as relationship_type,
        im.created_at,
        ('investment_'||c_par.atlas_id||'_'||c_sub.atlas_id) as uuid_input
    from investment_map im
    join companies c_sub on c_sub.id = im.subsidiary_id
    join companies c_par on c_par.id = im.company_id
),

combined as (
    select
        {{ atlas_uuid('uuid_input') }} as atlas_id,
        name,
        relationship,
        subsidiary_id,
        company_id,
        atlas_subsidiary_id,
        atlas_company_id,
        relationship_type,
        created_at,
        'parent'                      as source_type
    from parent_child_relationships

    union all

    select
        {{ atlas_uuid('uuid_input') }} as atlas_id,
        name,
        relationship,
        subsidiary_id,
        company_id,
        atlas_subsidiary_id,
        atlas_company_id,
        relationship_type,
        created_at,
        'investment'                  as source_type
    from investment_relationships
),

ranked as (
    select *,
           row_number() over (
               partition by atlas_id
               order by case when source_type='parent' then 0 else 1 end,
                        created_at
           ) as rn
    from combined
)

select
    atlas_id,
    name,
    relationship,
    subsidiary_id,
    company_id,
    atlas_subsidiary_id,
    atlas_company_id,
    relationship_type,
    created_at,
    '{{ var("master_id") }}' as created_by_atlas_id
from ranked
where rn = 1
