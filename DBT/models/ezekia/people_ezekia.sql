{{ config(
    materialized='table',
    alias='people_ezekia'
) }}

with people_data as (
    select
        p.id,
        p.firstname as first_name,
        p.lastname  as last_name,
        to_char(p.created_at::timestamp, 'YYYY-MM-DD"T"00:00:00') as created_at,
        to_char(p.updated_at::timestamp, 'YYYY-MM-DD"T"00:00:00') as updated_at,
        {{ atlas_uuid('p.id') }} as atlas_id
    from {{ var("source_database") }}.people p
),
positions as (
    select person_id, location_id
    from (
        select
            pp.person_id,
            pp.location_id,
            row_number() over (
                partition by pp.person_id
                order by
                    case
                        when pp.location_id is not null and pp."primary" then 0
                        when pp.location_id is not null and not pp."primary" then 1
                        when pp.location_id is null and pp."primary" then 2
                        else 3
                    end,
                    coalesce(pp.updated_at, pp.created_at) desc nulls last,
                    pp.id desc
            ) as rn
        from {{ var("source_database") }}.people_positions pp
    ) s
    where rn = 1
),
locations as (
    select
        l.id as location_id,
        l.location_name
    from {{ var("source_database") }}.locations l
),
joined as (
    select
        pd.id,
        pd.atlas_id,
        pd.first_name,
        pd.last_name,
        pd.created_at,
        pd.updated_at,
        loc.location_name as location_locality,
        '{{ var("master_id") }}' as created_by_atlas_id,
        '{{ var("master_id") }}' as updated_by_atlas_id
    from people_data pd
    left join positions pos on pd.id = pos.person_id
    left join locations loc on pos.location_id = loc.location_id
)

select
    id,
    atlas_id,
    first_name,
    last_name,
    created_at,
    updated_at,
    location_locality,
    'active'  as responsiveness,
    'regular' as candidate_status,
    '1' as created_by_id,
    '1' as updated_by_id,
    created_by_atlas_id,
    updated_by_atlas_id
from joined
