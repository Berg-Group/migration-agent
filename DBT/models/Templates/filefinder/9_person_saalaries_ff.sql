{{ config(
    materialized='table',
    alias='person_salaries_ff',
    tags=["filefinder"]
) }}

SELECT
    {{ atlas_uuid("'" ~ var('clientName') ~ "' || 'ACTUAL-' || r.idremuneration::text") }} AS atlas_id,
    pf.id AS person_id,
    pf.atlas_id AS atlas_person_id,
    TO_CHAR(cp.createdon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
    TO_CHAR(cp.modifiedon::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
    TO_CHAR(cp.createdon::timestamp(0), 'YYYY-MM-DD') AS relevant_date,
    NULLIF(SPLIT_PART(REGEXP_REPLACE(TRIM(COALESCE(r.salary::varchar, '')), '[^-0-9.]', ''), '.', 1), '')::bigint AS basic_salary,
    NULLIF(SPLIT_PART(REGEXP_REPLACE(TRIM(COALESCE(r.bonus::varchar, '')), '[^-0-9.]', ''), '.', 1), '')::bigint AS bonus_salary,
    NULLIF(SPLIT_PART(REGEXP_REPLACE(TRIM(COALESCE(r.package::varchar, '')), '[^-0-9.]', ''), '.', 1), '')::bigint AS total_salary,
    NULL AS expected_salary_min,
    NULL AS expected_salary_max,
    NULL AS expected_bonus_salary_min,
    NULL AS expected_bonus_salary_max,
    CASE
        WHEN char_length(TRIM(c.value)) = 3 THEN UPPER(TRIM(c.value))
        ELSE 'GBP'
    END AS currency,
    r.packagenote AS notes,
    'gross' AS tax_method,
    'total' AS expected_salary_type,
    'migration' AS source,
    'actual' AS type,
    uf.id AS created_by_id,
    COALESCE(uf.atlas_id, '{{ var("master_id") }}') AS created_by_atlas_id,
    '{{ var("agency_id") }}' AS agency_id
FROM {{ var('source_database') }}.Remuneration r
INNER JOIN {{ var('source_database') }}.Company_Person cp ON cp.idCompany_Person = r.idCompany_Person 
INNER JOIN {{ this.schema }}.people_ff pf ON pf.id = cp.idperson 
LEFT JOIN {{ var('source_database') }}.Currency c ON c.idCurrency = r.idCurrency 
LEFT JOIN {{ this.schema }}.users_ff uf ON LOWER(uf.name) = LOWER(r.createdby)
WHERE COALESCE(r.salary, 0) <> 0 OR COALESCE(r.bonus, 0) <> 0 OR COALESCE(r.package, 0) <> 0