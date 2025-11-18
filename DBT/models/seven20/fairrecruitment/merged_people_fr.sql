{{ config(
    materialized = 'table',
    alias        = 'merged_people_fr',
    tags         = ['seven20', 'dedup']
) }}

WITH identities AS (
    SELECT m.person_id, m.value
    FROM {{ ref('person_identities_fr') }} m
),

people AS (
    SELECT
        id AS person_id,
        atlas_id AS atlas_person_id,
        lower(trim(first_name)) AS fn,
        lower(trim(last_name))  AS ln,
        created_at
    FROM {{ ref('people_fr') }}
),

core AS (
    SELECT p.*, i.value
    FROM people p
    JOIN identities i USING (person_id)
),

ranked AS (
    SELECT
        fn,
        ln,
        value,
        person_id,
        atlas_person_id,
        row_number() OVER (PARTITION BY fn, ln, value ORDER BY created_at) AS rn,
        count(*)     OVER (PARTITION BY fn, ln, value) AS cnt
    FROM core
),

canon AS (
    SELECT
        fn,
        ln,
        value,
        person_id   AS new_person_id,
        atlas_person_id AS new_atlas_person_id
    FROM ranked
    WHERE rn = 1 AND cnt > 1
),

exploded AS (
    SELECT
        orig.atlas_person_id  AS original_atlas_person_id,
        orig.person_id        AS original_person_id,
        c.new_atlas_person_id,
        c.new_person_id
    FROM canon c
    JOIN ranked orig USING (fn, ln, value)
    WHERE orig.cnt > 1
)

SELECT
    original_atlas_person_id,
    original_person_id,
    new_atlas_person_id,
    new_person_id
FROM exploded
WHERE original_person_id <> new_person_id
ORDER BY new_person_id, original_person_id