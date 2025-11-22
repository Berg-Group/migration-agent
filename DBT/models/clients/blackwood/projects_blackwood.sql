{{ config(
    materialized = 'table',
    alias        = 'projects_blackwood',
    tags         = ['blackwood']
) }}

with projects_dedupl as ( 
    select jobid,
           userid,
           row_number() over (partition by jobid) as rn
    from {{ var('source_database') }}.jobfile_leadconsultant
),

feedback as (
    select jobid,
           coalesce(
                case when summaryofsearchoutcome  is not null and trim(summaryofsearchoutcome)  <> '' then '<p><strong>Summary of search outcome</strong></p><p>'  || {{ clean_html('summaryofsearchoutcome')  }} || '</p>' else '' end ||
                case when summaryofsearchfeedback is not null and trim(summaryofsearchfeedback) <> '' then '<p><strong>Summary of search feedback</strong></p><p>' || {{ clean_html('summaryofsearchfeedback') }} || '</p>' else '' end ||
                case when shortlistedfeedback     is not null and trim(shortlistedfeedback)     <> '' then '<p><strong>Shortlisted feedback</strong></p><p>'    || {{ clean_html('shortlistedfeedback')     }} || '</p>' else '' end ||
                case when lessonstocapture        is not null and trim(lessonstocapture)        <> '' then '<p><strong>Lessons to capture</strong></p><p>'       || {{ clean_html('lessonstocapture')        }} || '</p>' else '' end ||
                case when clientfeedback          is not null and trim(clientfeedback)          <> '' then '<p><strong>Client feedback</strong></p><p>'          || {{ clean_html('clientfeedback')          }} || '</p>' else '' end ,
                ''
           ) as notes
    from {{ var('source_database') }}.jobfile_searchfeedback
),

company_pick as (  
    select  j.jobid,
            j.joblevel1companyid::varchar                                         as orig_company_id,
            coalesce(mp.new_company_id, j.joblevel1companyid::varchar)      as final_company_id
    from {{ var('source_database') }}.jobfile j
    left join {{ ref('companies_mapping_blackwood') }} mp
           on mp.original_company_id = j.joblevel1companyid::varchar
)

select
    j.jobid                                                                                 as id,
    lower(
        substring(md5(j.jobid::text || '{{ var("clientName") }}'), 1,  8) || '-' ||
        substring(md5(j.jobid::text || '{{ var("clientName") }}'), 9,  4) || '-' ||
        substring(md5(j.jobid::text || '{{ var("clientName") }}'), 13, 4) || '-' ||
        substring(md5(j.jobid::text || '{{ var("clientName") }}'), 17, 4) || '-' ||
        substring(md5(j.jobid::text || '{{ var("clientName") }}'), 21,12)
    )                                                                                       as atlas_id,
    to_char(coalesce(j.jobsearchcommenced, jr.createddate, current_date),'YYYY-MM-DD"T00:00:00"') as created_at,
    jr.createdbywho                                                                          as created_by,
    coalesce(u2.atlas_id,'{{ var("created_by_id") }}')                                       as created_by_atlas_id,
    j.jobname                                                                                as job_role,
    u.id                                                                                     as owner_id,
    coalesce(u.atlas_id, '{{var("master_id")}}')                                             as atlas_owner_id,
    case when js.status_sortorder in (3,4,5,6) then 'closed'
         when js.status_sortorder = 7                  then 'on_hold'
         else 'active' end                                                                   as state,
    case when js.status_sortorder in (3,4) then 'cancelled'
         when js.status_sortorder in (5,6) then 'won' end                                    as close_reason,
    case when js.status_sortorder in (3,4,5,6)
         then to_char(coalesce(j.jobsearchclosed,current_timestamp),'YYYY-MM-DD"T00:00:00"')
    end                                                                                     as closed_at,
    'full_time'                                                                              as contract_type,
    '1'                                                                                      as hire_targed,
    coalesce(c.final_company_id,'592')                                                       as company_id,
    coalesce(cb.atlas_id,'4486cb9d-a216-d924-0e88-94aa25b870b9')                             as atlas_company_id,
    '{{ var("agency_id") }}'                                                                 as agency_id,
    false                                                                                    as public,
    'project'                                                                                as class_type,
    coalesce(f.notes,'')                                                                     as notes,
    j.jobnumber                                                                              as job_number
from {{ var('source_database') }}.jobfile j
left join {{ var('source_database') }}.library_job_status js on js.status_code = j.jobstatus
left join {{ var('source_database') }}.jobfile_recordinformation jr using (jobid)
left join projects_dedupl jl on jl.jobid = j.jobid and jl.rn = 1
left join {{ ref('users_blackwood') }} u  on u.id  = jl.userid
left join {{ ref('users_blackwood') }} u2 on u2.id = jr.createdbywho
left join company_pick               c   on c.jobid = j.jobid
left join {{ ref('companies_blackwood') }} cb on cb.id = c.final_company_id
left join feedback f                        on f.jobid = j.jobid
group by
    j.jobid,j.jobsearchcommenced,jr.createddate,jr.createdbywho,
    js.status_sortorder,j.jobsearchclosed,j.jobname,
    u.id,u.atlas_id,u2.atlas_id,
    c.final_company_id,cb.atlas_id,f.notes,j.jobnumber
