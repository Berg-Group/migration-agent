{{ config(
    materialized='table',
    alias='person_references_blackwood',
    tags=["blackwood"]
) }}

with internal_persons as (
    select
        id       as person_id,
        atlas_id as atlas_person_id
    from {{ ref('people_blackwood') }}
),


src as (
    select cr.*
    from {{ var('source_database') }}.candidate_reference cr
    where cr.referencetext is not null
),

internal_relationships as (
    select id as relationship_id, 
           atlas_id as atlas_relationship_id 
    from 
        {{ref('people_relationships_blackwood')}}
),

prepared as (
    select
        cr.referenceid as id,
        {{ atlas_uuid('cr.referenceid::text') }} as atlas_id,
        ir.atlas_relationship_id,
        cr.cref                          as person_id,
        ip.atlas_person_id               as atlas_person_id,
        '{{ var("created_by_id") }}'     as created_by_atlas_id,
        'other'                          as source,
        cr.referencetext                 as text,
        row_number() over (
            partition by cr.referenceid
            order by cr.referenceid nulls last
        ) as rn
    from src cr
    inner join internal_persons ip
        on ip.person_id = cr.cref
    inner join internal_relationships ir 
        on ir.relationship_id = cr.referenceid
)

select *
from prepared
where rn = 1