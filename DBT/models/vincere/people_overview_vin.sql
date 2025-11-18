
{{ config(
    materialized='table',
    alias='people_overview_vin'
) }}

with internal_persons as (
    select 
        id as person_id,
        atlas_id as atlas_person_id 
    from
        {{ref('1_people_vincere')}}
)

select
    ip.person_id,
    ip.atlas_person_id,
    {{clean_html('note_backup')}} as overview 
from {{var('source_database')}}."candidate" c
inner join internal_persons ip on ip.person_id = c.id
where note_backup notnull and trim(note_backup) <> ''