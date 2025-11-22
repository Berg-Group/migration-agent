{{ config(materialized='table', alias='companies_mapping_blackwood', tags=['blackwood']) }}

with projects as (
    select distinct joblevel1companyid
    from {{ var('source_database') }}.jobfile
),

level1 as (
    select  c.level1companyid::varchar                id,
            lower(trim(c.company_description))        name_norm,
            c.company_description                     company_name,
            null::varchar                             parent_name_norm,
            1                                         lvl,
            null::varchar                             parent_id
    from {{ var('source_database') }}.level1company c
    left join projects p on p.joblevel1companyid = c.level1companyid
),

level2 as (
    select  ('l2-'||l2.level2companyid)::varchar      id,
            lower(trim(l2.company_description))       name_norm,
            l2.company_description                    company_name,
            lower(trim(l1.company_description))       parent_name_norm,
            2                                         lvl,
            l2.level1companyid::varchar               parent_id
    from {{ var('source_database') }}.level2company l2
    left join {{ var('source_database') }}.level1company l1
           on l1.level1companyid = l2.level1companyid
),

combined as (
    select * from level1
    union all
    select * from level2
),

canon_pick as (
    select  parent_id,
            name_norm,
            min(case when lvl = 1 then id end) as lvl1_id,
            min(id)                            as min_any_id
    from combined
    group by parent_id, name_norm
),

dedup as (
    select  c.id                              as original_company_id,
            case
                 when c.lvl = 2 and c.name_norm = c.parent_name_norm then c.parent_id
                 when c.lvl = 2 and c.id <> cp.min_any_id            then cp.min_any_id
                 else c.id
            end                                as new_company_id,
            c.company_name
    from combined c
    join canon_pick cp USING (parent_id, name_norm)
    where c.id <> case
                      when c.lvl = 2 and c.name_norm = c.parent_name_norm then c.parent_id
                      when c.lvl = 2 and c.id <> cp.min_any_id            then cp.min_any_id
                      else c.id
                  end
)

select  row_number() over ()            as mapping_id,
        company_name,
        original_company_id,
        new_company_id
from dedup
