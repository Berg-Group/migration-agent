{{ config(
    materialized='table',
    alias='ryton_delete_people'
) }}

with latest_notes as (

    -- newest note per person
    select
        person_id,
        max(_created_at) as most_recent_note
    from {{ ref('7_people_notes_vin') }}
    group by person_id

),

people_filtered as (

    -- restrict to the agency & exclude recent notes
    select
        p.id,
        p.first_name,
        p.last_name,
        ln.most_recent_note
    from {{ ref('1_people_vincere') }} p
    left join latest_notes ln on ln.person_id = p.id
    where p.agency_id = '24d91589-9acf-4375-a2d6-846d36716a3b'
      and (
            ln.most_recent_note is null          -- keep people with no notes
         or ln.most_recent_note < '2024-12-16'   -- latest note before cutoff
      )

)

select *
from people_filtered
