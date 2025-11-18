{{ config(
    materialized = 'table',
    alias        = 'person_salaries_invenias',
    tags         = ['invenias']
) }}

WITH internal_persons AS (

    SELECT DISTINCT
        id         AS person_id,
        atlas_id   AS atlas_person_id
    FROM {{ ref('people_invenias') }}

)

SELECT
    pp.itemid                                                           AS id,
    lower(
        substring(md5(pp.itemid::text || r.personid::text),  1,  8) || '-' ||
        substring(md5(pp.itemid::text || r.personid::text),  9,  4) || '-' ||
        substring(md5(pp.itemid::text || r.personid::text), 13,  4) || '-' ||
        substring(md5(pp.itemid::text || r.personid::text), 17,  4) || '-' ||
        substring(md5(pp.itemid::text || r.personid::text), 21, 12)
    )                                                                  AS atlas_id,
    to_char(pp.datecreated , 'YYYY-MM-DD"T"00:00:00')                  AS created_at,
    to_char(pp.datemodified, 'YYYY-MM-DD"T"00:00:00')                  AS updated_at,
    '{{ var("agency_id") }}'                                           AS agency_id,
    pp.creatorid                                                       AS created_by_id,
    '{{ var("created_by_id") }}'                                       AS created_by_atlas_id,
    r.personid                                                         AS external_person_id,
    ip.atlas_person_id,
    NULL                                                               AS experience_id,
    COALESCE(pp.amountfrom, pp.amountto)                               AS basic_salary,
    NULL                                                               AS bonus_salary,
    pp.amountto                                                        AS total_salary,
    pp.amountfrom                                                      AS expected_salary_min,
    pp.amountto                                                        AS expected_salary_max,
    c.currencycode                                                     AS currency,
    TRIM(SPLIT_PART(c.currencyname, ',', 1))                           AS country,
    pp.notes,
    'gross'                                                            AS tax_method,
    'total'                                                            AS expected_salary_type,
    'migration'                                                        AS source,
    to_char(pp.datemodified, 'YYYY-MM-DD"T"00:00:00')                  AS relevant_date

FROM {{ var('source_database') }}."permanentpackages"                pp
JOIN {{ var('source_database') }}."permanentpackagessettings"       pps ON pps.itemid   = pp.settingid
JOIN {{ var('source_database') }}."relation_positiontopermanentpackage" rpp ON rpp.packageid = pp.itemid
JOIN {{ var('source_database') }}."positions"                  p  ON p.itemid      = rpp.positionid
JOIN {{ var('source_database') }}."relation_persontoposition"        r  ON r.positionid  = p.itemid
LEFT JOIN {{ var('source_database') }}."currencies"                  c  ON c.itemid      = pp.currency
INNER JOIN internal_persons                                           ip ON ip.person_id  = r.personid

WHERE
    (pp.amountfrom IS NOT NULL OR pp.amountto IS NOT NULL)
    AND pps."name" = 'Basic Salary'