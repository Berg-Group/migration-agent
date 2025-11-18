{{ config(
    materialized = 'table',
    alias        = 'unmerged_people_fr',
    tags         = ['seven20', 'dedup']
) }}

WITH identities AS (
    SELECT DISTINCT m.person_id, m.value
    FROM {{ ref('person_identities_fr') }} m
),

people AS (
    SELECT
        id  AS person_id,
        atlas_id AS atlas_person_id,
        first_name,
        last_name,
        lower(trim(first_name)) AS fn,
        lower(trim(last_name))  AS ln
    FROM {{ ref('people_fr') }}
),

core AS (
    SELECT p.person_id,
           p.atlas_person_id,
           p.first_name,
           p.last_name,
           p.fn,
           p.ln,
           i.value
    FROM people p
    JOIN identities i USING (person_id)
),

pairs AS (
    SELECT
        c1.atlas_person_id  AS atlas_person_id_1,
        c1.person_id        AS person_id_1,
        c2.atlas_person_id  AS atlas_person_id_2,
        c2.person_id        AS person_id_2,
        c1.first_name       AS first_name_1,
        c2.first_name       AS first_name_2,
        c1.last_name        AS last_name_1,
        c2.last_name        AS last_name_2,
        c1.value
    FROM core c1
    JOIN core c2
      ON c1.value = c2.value
     AND c1.person_id < c2.person_id
     AND (c1.fn <> c2.fn OR c1.ln <> c2.ln)
)

SELECT
    atlas_person_id_1,
    person_id_1,
    atlas_person_id_2,
    person_id_2,
    first_name_1,
    first_name_2,
    last_name_1,
    last_name_2,
    value
FROM pairs
ORDER BY value, person_id_1, person_id_2