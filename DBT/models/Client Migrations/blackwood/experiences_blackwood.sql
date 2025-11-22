{{ config(materialized='table', alias='experiences_blackwood', tags=['blackwood']) }}

with internal_persons as (
    select id as person_id,
           atlas_id as atlas_person_id
    from {{ ref('people_blackwood') }}
),

internal_companies as (
    select id as company_id,
           atlas_id as atlas_company_id,
           name as company_name
    from {{ ref('companies_blackwood') }}
),

role_notes_first as (
    select employmenthistoryuid,
           notes
    from (
        select employmenthistoryuid,
               nullif(trim(notes),'') as notes,
               row_number() over (partition by employmenthistoryuid order by roleorder) rn
        from {{ var('source_database') }}.candidate_employmenthistory_role
    ) t
    where rn = 1
      and notes is not null
),

candidates_with_exp as (
    select distinct cref
    from {{ var('source_database') }}.candidate_employmenthistory
    where fromyear > 1900
       or toyear   > 1900
),

hist as (
    select h.uid::text                                       as uid,
           h.cref,
           'l2-'||h.level2companyid                          as level2companyid,
           h.fromyear,
           h.frommonth,
           h.toyear,
           h.tomonth,
           rnf.notes                                         as title,
           case 
               when h.employment_order = 1 
               then nullif(trim(cn.jobsummary),'') 
           end                                               as job_summary,
           case 
               when h.employment_order = 1 then 1 else 2 
           end                                               as src_rank,
           case 
               when h.employment_order = 1 then 1 else 0 
           end                                               as is_current
    from {{ var('source_database') }}.candidate_employmenthistory h
    left join role_notes_first rnf 
           on rnf.employmenthistoryuid = h.uid
    left join {{ var('source_database') }}.candidate_notes cn 
           on cn.cref = h.cref
    where h.level2companyid is not null
),

no_exp as (
    select 'noexp_'||c.cref                                  as uid,
           c.cref,
           'l2-'||c.currentcompany_level2companyid           as level2companyid,
           2025                                              as fromyear,
           6                                                 as frommonth,
           null::int                                         as toyear,
           null::int                                         as tomonth,
           null::text                                        as title,
           nullif(trim(cn.jobsummary),'')                    as job_summary,
           3                                                 as src_rank,
           1                                                 as is_current
    from {{ var('source_database') }}.candidate c
    left join {{ var('source_database') }}.candidate_notes cn 
           on cn.cref = c.cref
    left join candidates_with_exp e 
           on e.cref = c.cref
    where c.currentcompany_level2companyid is not null
      and e.cref is null
),

src as (
    select * from hist
    union all
    select * from no_exp
),

mapped as (
    select s.*,
           coalesce(mp.new_company_id, s.level2companyid) as company_id_final
    from src s
    left join {{ ref('companies_mapping_blackwood') }} mp
           on mp.original_company_id = s.level2companyid
),

enriched as (
    select m.*,
           case 
               when m.fromyear > 1900 
               then to_date(to_char(m.fromyear,'FM0000')||lpad(coalesce(m.frommonth::text,'01'),2,'0')||'01','yyyymmdd') 
           end                                               as started_at_date
    from mapped m
),

ranked_current as (
    select e.*,
           case 
               when e.is_current = 1 
               then row_number() over (
                        partition by e.cref 
                        order by e.started_at_date desc nulls last, e.uid
                    ) 
           end                                               as current_rnk
    from enriched e
),

ranked_company as (
    select rc.*,
           row_number() over (
               partition by rc.cref, rc.company_id_final 
               order by rc.src_rank, rc.started_at_date nulls last, rc.uid
           )                                                as company_rnk,
           lag(rc.started_at_date) over (
               partition by rc.cref 
               order by rc.started_at_date desc nulls last, rc.uid
           )                                                as next_start_date
    from ranked_current rc
)

select rc.uid                                              as id,
       {{ atlas_uuid('rc.uid') }}                          as atlas_id,
       rc.cref                                             as person_id,
       ip.atlas_person_id,
       rc.company_id_final                                 as company_id,
       ic.atlas_company_id,
       case 
           when rc.fromyear > 1900 
           then to_char(rc.fromyear,'FM0000')||'-'||lpad(coalesce(rc.frommonth::text,'01'),2,'0')||'-01'
           when rc.is_current = 1 and rc.current_rnk = 1 
           then '2025-01-01' 
       end                                                 as started_at,
       case 
           when rc.toyear > 1900 
           then to_char(rc.toyear,'FM0000')||'-'||lpad(coalesce(rc.tomonth::text,'01'),2,'0')||'-01'
           when rc.is_current = 0 and rc.next_start_date is not null 
           then to_char(rc.next_start_date, 'YYYY-MM-DD') 
       end                                                 as finished_at,
       coalesce(rc.job_summary, rc.title, 'Employment')    as title,
       ic.company_name,
       '{{ var("agency_id") }}'                            as agency_id,
       'migration'                                         as source,
       to_char(current_date, 'YYYY-MM-DD"T"00:00:00')      as created_at,
       to_char(current_date, 'YYYY-MM-DD"T"00:00:00')      as updated_at
from ranked_company rc
left join internal_persons  ip on ip.person_id   = rc.cref
left join internal_companies ic on ic.company_id = rc.company_id_final
where rc.company_rnk = 1
  and (
        (rc.is_current = 1 and rc.current_rnk = 1)
        or rc.fromyear > 1900
      )