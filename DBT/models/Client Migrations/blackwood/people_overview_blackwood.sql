{{ config(
    materialized = 'table',
    alias        = 'people_overview_blackwood',
    tags         = ['blackwood']
) }}

with non_empty_notes as (
    select
        cref,
        jobsummary,
        description,
        futuredirection,
        proscons,
        education,
        attributes,
        evidence,
        externalroles,
        expertise
    from {{ var('source_database') }}.candidate_notes
    where coalesce(jobsummary, description, futuredirection, proscons,
                   education, attributes, evidence, externalroles, expertise) is not null
),

note_fields as (
    select cref,
           '<b>jobsummary</b>'      || '\n\n' || trim(jobsummary)      as note
    from non_empty_notes
    where jobsummary      is not null and trim(jobsummary)      <> ''

    union all

    select cref,
           '<b>description</b>'     || '\n\n' || trim(description)     as note
    from non_empty_notes
    where description     is not null and trim(description)     <> ''

    union all

    select cref,
           '<b>futuredirection</b>' || '\n\n' || trim(futuredirection) as note
    from non_empty_notes
    where futuredirection is not null and trim(futuredirection) <> ''

    union all

    select cref,
           '<b>proscons</b>'        || '\n\n' || trim(proscons)        as note
    from non_empty_notes
    where proscons        is not null and trim(proscons)        <> ''

    union all

    select cref,
           '<b>education</b>'       || '\n\n' || trim(education)       as note
    from non_empty_notes
    where education       is not null and trim(education)       <> ''

    union all

    select cref,
           '<b>attributes</b>'      || '\n\n' || trim(attributes)      as note
    from non_empty_notes
    where attributes      is not null and trim(attributes)      <> ''

    union all

    select cref,
           '<b>evidence</b>'        || '\n\n' || trim(evidence)        as note
    from non_empty_notes
    where evidence        is not null and trim(evidence)        <> ''

    union all

    select cref,
           '<b>externalroles</b>'   || '\n\n' || trim(externalroles)   as note
    from non_empty_notes
    where externalroles   is not null and trim(externalroles)   <> ''

    union all

    select cref,
           '<b>expertise</b>'       || '\n\n' || trim(expertise)       as note
    from non_empty_notes
    where expertise       is not null and trim(expertise)       <> ''
),

plain_overview as (
    select
        cref,
        listagg(trim(note), '\n\n') within group (order by note) as overview
    from note_fields
    group by cref
),

internal_persons as (
    select id as person_id, atlas_id as atlas_person_id
    from {{ ref('people_blackwood') }}
)

select
    ip.person_id,
    ip.atlas_person_id,
    po.overview
from plain_overview       po
inner join internal_persons ip on ip.person_id = po.cref