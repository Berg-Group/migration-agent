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

internal_relations as (
    select
        external_subordinate_id AS person_id,
        subordinate_id,
        atlas_id as relationship_id
    from {{ ref('people_relationships_blackwood') }}
),

src as (
    select cr.*
    from {{ var('source_database') }}.candidate_reference cr
    inner join  {{ var('source_database') }}."candidates_slice" cs ON cs.cref = cr.cref
    where cr.referencetext is not null
),

prepared as (
    select
        cr.referenceid as id,
        lower(
            substring(md5(cr.referenceid::text),  1, 8) || '-' ||
            substring(md5(cr.referenceid::text),  9, 4) || '-' ||
            substring(md5(cr.referenceid::text), 13, 4) || '-' ||
            substring(md5(cr.referenceid::text), 17, 4) || '-' ||
            substring(md5(cr.referenceid::text), 21, 12)
        )                                as atlas_id,
        ir.relationship_id,
        cr.cref                          as external_person_id,
        ir.person_id                     as person_id,
        '{{ var("created_by_id") }}'    as created_by_id,
        'candidate'                      as source,
        cr.referencetext                 as text
    from src cr
    left join internal_persons ip_sub
        on ip_sub.person_id = cr.cref
    left join internal_relations ir
        on coalesce(ir.subordinate_id, ir.person_id) = ip_sub.atlas_person_id

)

select *
from prepared