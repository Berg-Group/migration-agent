{{ config(
    materialized = 'table',
    alias        = 'person_notes_blackwood',
    tags         = ['blackwood']
) }}

with internal_persons as (
    select id as person_id,
           atlas_id as atlas_person_id
    from {{ ref('people_blackwood') }}
),

users as (
    select id::varchar as user_id,
           atlas_id    as user_atlas_id
    from {{ ref('users_blackwood') }}
),

first_consultant as (
    select
        contactlog_id,
        contact_consultant::varchar as consultant_id,
        row_number() over (partition by contactlog_id order by contactlog_id) as rn
    from {{ var('source_database') }}.candidate_contactlog_consultant
    where contact_consultant is not null
),

all_contacts as (
    select
        cl.contactlog_id,
        cl.cref,
        cl.contact_date,
        fc.consultant_id,
        trim(
            case when contact_generalnotes is not null and trim(contact_generalnotes) <> ''
                 then 'Notes:' || chr(10) ||
                      {{ clean_html('cl.contact_generalnotes') }} || chr(10) || chr(10)
            else '' end ||
            case when contact_assessment is not null and trim(contact_assessment) <> ''
                 then 'Assessment:' || chr(10) ||
                      {{ clean_html('cl.contact_assessment') }} || chr(10) || chr(10)
            else '' end ||
            case when contact_internalnotes is not null and trim(contact_internalnotes) <> ''
                 then 'Internal notes:' || chr(10) ||
                      {{ clean_html('cl.contact_internalnotes') }} || chr(10) || chr(10)
            else '' end ||
            case when contact_recruitmentconsiderations is not null
                      and trim(contact_recruitmentconsiderations) <> ''
                 then 'Recruitment considerations:' || chr(10) ||
                      {{ clean_html('cl.contact_recruitmentconsiderations') }}
            else '' end
        )  as combined_note_text
    from {{ var('source_database') }}.candidate_contactlog cl
    left join first_consultant fc
           on fc.contactlog_id = cl.contactlog_id
          and fc.rn = 1
)

select distinct
    ac.contactlog_id                                    as id,
    {{ atlas_uuid('ac.cref || ac.contactlog_id') }}     as atlas_id,
    ac.cref                                             as person_id,
    ip.atlas_person_id,
    ac.combined_note_text                               as text,
    'manual'                                            as type,
    to_char(ac.contact_date,'YYYY-MM-DD"T"00:00:00')    as created_at,
    to_char(ac.contact_date,'YYYY-MM-DD"T"00:00:00')    as updated_at,
    '{{ var("agency_id") }}'                            as agency_id,
    coalesce(ac.consultant_id,'{{ var("master_id") }}') as created_by_id,
    coalesce(u.user_atlas_id, '{{ var('master_id') }}')  as created_by_atlas_id
from all_contacts ac
inner join internal_persons ip on ip.person_id = ac.cref
left join users u            on u.user_id    = ac.consultant_id
where ac.combined_note_text <> '' and ac.combined_note_text <> ' ' and ac.combined_note_text notnull