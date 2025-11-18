-- models/vincere/company_contacts_vin.sql
{{ config(materialized='table', alias='company_contacts_vincere') }}

{% set src = var('source_database') %}

/*───────────────────────────────────────────────────────────────────────
❶  people_vincere → map every contact‑id to its person + atlas id
───────────────────────────────────────────────────────────────────────*/
WITH person_xref AS (               -- one row per contact id in people_vincere
    SELECT DISTINCT
        regexp_replace(company_contact_id, '[^0-9]', '')::BIGINT   AS contact_id,
        id                                                         AS person_id,
        atlas_id                                                   AS atlas_person_id
    FROM   {{ref('1_people_vincere')}}
    WHERE  company_contact_id IS NOT NULL
      AND  regexp_replace(company_contact_id, '[^0-9]', '') <> ''
),

/*───────────────────────────────────────────────────────────────────────
❷  company → atlas map
───────────────────────────────────────────────────────────────────────*/
company_xref AS (
    SELECT  id        AS company_id,
            atlas_id  AS atlas_company_id
    FROM   {{ref('3_companies_vin')}}
),

/*───────────────────────────────────────────────────────────────────────
❸  active contacts (must exist in person_xref)
───────────────────────────────────────────────────────────────────────*/
base AS (
    SELECT
        cc.id                                            AS id,            -- contact PK
        px.person_id,                                                    -- people_vincere.id
        {{ atlas_uuid("'vincere_company_contact_' || '{{ var(\"clientName\") }}' || cc.id::text") }} AS atlas_id,
        to_char(cc.insert_timestamp::timestamp(0),
                'YYYY-MM-DD"T"HH24:MI:SS')              AS created_at,
        to_char(current_timestamp,
                'YYYY-MM-DD"T"HH24:MI:SS')              AS updated_at,
        '{{ var("agency_id") }}'                        AS agency_id,
        cc.company_id,
        'prospect'                                      AS contact_role,
        COALESCE(cc.job_title, 'contact')               AS title
    FROM   {{ src }}."public_contact" cc
    JOIN   person_xref px ON px.contact_id = cc.id          -- mandatory mapping
    WHERE  cc.deleted_timestamp IS NULL                     -- active contacts only
)
SELECT
       b.*,
       px.atlas_person_id,
       cx.atlas_company_id,
       'prospect' AS relationship
FROM   base           b
INNER JOIN person_xref px ON px.contact_id = b.id
INNER JOIN company_xref cx ON cx.company_id = b.company_id