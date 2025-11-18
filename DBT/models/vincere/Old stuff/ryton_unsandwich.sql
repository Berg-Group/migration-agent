{{ config(
    materialized='table',
    alias='people_vincere_unsandwich',
    tags=['ryton_migration']
) }}

-- --------------------------------------------------------------------
-- Unsandwich: map ryton_emails → existing person_identities_vincere
-- --------------------------------------------------------------------

with emails as (
    select lower(value) as email
    from {{ ref('ryton_emails') }}                      -- staging list
),
matched as (
    select
        piv.atlas_person_id as atlas_id,               -- keep atlas_id
        lower(piv.value)    as email                   -- matched e‑mail
    from "{{ this.schema }}"."person_identities_vincere" piv   -- physical table already exists
    join emails e
      on lower(piv.value) = e.email                    -- case‑insensitive join
)

select distinct *
from matched
