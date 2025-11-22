{{ config(
    materialized='table',
    alias='person_relationships_blackwood',
    tags=["blackwood"]
) }}

with internal_persons as (
    select
        id          as person_id,
        atlas_id    as atlas_person_id
    from {{ ref('people_blackwood') }}
),

src as (
    select
        cr.*,
        lower(trim(cr.historiccompanyname)) as historic_company_norm
    from {{ var('source_database') }}.candidate_reference cr
    where cr.historiccompanyname is not null
),

prepared as (
    select
        cr.referenceid as id,
        {{ atlas_uuid('cr.referenceid::text || cr.cref_referee::text') }} as atlas_id,
        CASE 
            WHEN cr.referenceid = 4 THEN cr.cref_referee
            ELSE cr.cref
        END as subordinate_id,
        CASE 
            WHEN cr.referenceid = 4 THEN ip2.atlas_person_id
            ELSE ip.atlas_person_id
        END as atlas_subordinate_id,
        CASE 
            WHEN cr.referenceid = 4 THEN cr.cref
            ELSE cr.cref_referee
        END as person_id,
        CASE 
            WHEN cr.referenceid = 4 THEN ip.atlas_person_id
            ELSE ip2.atlas_person_id
        END as atlas_person_id,
        'other' as relationship_type,

        null as company_id,

        '{{ var("created_by_id") }}' as created_by_atlas_id,

        row_number() over (
            partition by cr.cref,
                         cr.cref_referee
            order by cr.referenceid desc nulls last
        ) as rn
    from src cr
    inner join internal_persons ip
        on ip.person_id = cr.cref
    inner join internal_persons ip2
        on ip2.person_id = cr.cref_referee
    left join {{ var('source_database') }}.library_referencerelationship rr
        on rr.id = cr.relationshipid
    )


select *
from prepared
where rn = 1
and atlas_person_id notnull 