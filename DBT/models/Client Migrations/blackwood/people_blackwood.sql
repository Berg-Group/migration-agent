{{ config(
    materialized='table',
    alias='people_blackwood',
    tags=["blackwood"]
) }}

with company_loc as (
    select c.cref,
           loc.location_description as comp_location
    from {{ var('source_database') }}.candidate c
    left join {{ var('source_database') }}.level2company l2
           on l2.level2companyid = c.currentcompany_level2companyid
    left join {{ var('source_database') }}.library_locations loc
           on loc.location_code = l2.company_locationcode
),

non_empty_notes as (
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
           '**jobsummary**'      || '\n\n' || trim(jobsummary)      as note
    from non_empty_notes
    where jobsummary      is not null and trim(jobsummary)      <> ''

    union all

    select cref,
           '**description**'     || '\n\n' || trim(description)     as note
    from non_empty_notes
    where description     is not null and trim(description)     <> ''

    union all

    select cref,
           '**futuredirection**' || '\n\n' || trim(futuredirection) as note
    from non_empty_notes
    where futuredirection is not null and trim(futuredirection) <> ''

    union all

    select cref,
           '**proscons**'        || '\n\n' || trim(proscons)        as note
    from non_empty_notes
    where proscons        is not null and trim(proscons)        <> ''

    union all

    select cref,
           '**education**'       || '\n\n' || trim(education)       as note
    from non_empty_notes
    where education       is not null and trim(education)       <> ''

    union all

    select cref,
           '**attributes**'      || '\n\n' || trim(attributes)      as note
    from non_empty_notes
    where attributes      is not null and trim(attributes)      <> ''

    union all

    select cref,
           '**evidence**'        || '\n\n' || trim(evidence)        as note
    from non_empty_notes
    where evidence        is not null and trim(evidence)        <> ''

    union all

    select cref,
           '**externalroles**'   || '\n\n' || trim(externalroles)   as note
    from non_empty_notes
    where externalroles   is not null and trim(externalroles)   <> ''

    union all

    select cref,
           '**expertise**'       || '\n\n' || trim(expertise)       as note
    from non_empty_notes
    where expertise       is not null and trim(expertise)       <> ''
),


plain_overview as (
    select
        cref,
        listagg(trim(note), '\n\n') within group (order by note) as overview
    from note_fields
    group by cref
)

SELECT
    c.cref AS id,
    c.guid AS atlas_id,
    c.firstname AS first_name,
    c.lastname AS last_name,
    TO_CHAR(cr.createddate, 'YYYY-MM-DD"T"00:00:00') AS created_at,
    TO_CHAR(COALESCE(cr.updateddate, '{{var("date")}}'::timestamp), 'YYYY-MM-DD"T"00:00:00') AS updated_at,
    cr.createdbywho AS created_by_id,
    COALESCE(u.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
    cr.updatedbywho AS updated_by_id,
    COALESCE(u2.atlas_id, '{{ var("master_id") }}') AS updated_by_atlas_id,
    'active' AS responsiveness,
    'regular' AS candidate_status,
    coalesce(a.address, cl.comp_location) AS location_locality,
    po.overview
FROM 
    {{ var('source_database') }}."candidate" c
LEFT JOIN 
    {{ var('source_database') }}."candidate_address" a USING (cref)
LEFT JOIN 
     {{ var('source_database') }}."candidate_recordinformation" cr USING (cref)
LEFT JOIN 
    {{ ref('users_blackwood')}} u ON u.id = cr.createdbywho
LEFT JOIN 
     {{ ref('users_blackwood')}} u2 ON u2.id = cr.updatedbywho
LEFT JOIN company_loc cl USING (cref)
LEFT JOIN plain_overview po USING (cref)