{{ config(
    materialized = 'table',
    alias        = 'project_company_contacts_vincere'
) }}

{# ------------------------------------------------------------ #
   1 ▸ gather the "main" contact/person for every project
# ------------------------------------------------------------ #}
WITH contact_ids AS (
    SELECT
        s.id                       AS project_id,
        s.insert_timestamp,
        s.contact_id               AS contact_id,          -- raw Vincere contact PK
        /* if the contact is linked to a candidate row, use that,
           otherwise fall back to the contact‑only row we'll create */
        CASE
            WHEN c.id IS NOT NULL THEN c.id::varchar
            ELSE pc.id::varchar
        END                         AS person_id,
        s.company_id
    FROM {{ var('source_database') }}."public_position_description" s
    LEFT JOIN {{ var('source_database') }}."public_contact"   pc
           ON s.contact_id = pc.id
    LEFT JOIN {{ var('source_database') }}."public_candidate" c
           ON c.external_id = pc.external_id
          AND c.deleted_reason IS NULL
    WHERE pc.deleted_timestamp IS NULL
),

{# ------------------------------------------------------------ #
   2 ▸ enrich with atlas ids for project / person / company
# ------------------------------------------------------------ #}
base AS (
    SELECT
        TO_CHAR(ci.insert_timestamp::timestamp(0),
                'YYYY-MM-DD"T"HH24:MI:SS')             AS created_at,
        TO_CHAR(ci.insert_timestamp::timestamp(0),
                'YYYY-MM-DD"T"HH24:MI:SS')             AS updated_at,
        '{{ var("agency_id") }}'                       AS agency_id,

        ci.project_id,
        pv.atlas_id            AS atlas_project_id,

        /* contact FK we'll join on in the final step */
        ci.contact_id::varchar AS company_contact_id,

        ci.person_id,
        pe.atlas_id            AS atlas_person_id,

        ci.company_id,
        cv.atlas_id            AS atlas_company_id,

        'client'               AS role
    FROM contact_ids                    ci
    LEFT JOIN {{ref('8_projects_vin')}}  pv  ON ci.project_id = pv.id
    LEFT JOIN {{ref('1_people_vincere')}} pe ON ci.person_id  = pe.id
    LEFT JOIN {{ref('3_companies_vin')}} cv  ON ci.company_id = cv.id
)

SELECT
    b.project_id,
    b.atlas_project_id,

    b.company_contact_id,
    cc.atlas_id                      AS atlas_company_contact_id
FROM   base                         b
LEFT   JOIN {{ref('5_company_contacts_vin')}} cc
       ON cc.id::varchar = b.company_contact_id
