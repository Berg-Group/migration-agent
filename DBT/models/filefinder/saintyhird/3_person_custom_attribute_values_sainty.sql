{{ config(
    materialized='table',
    alias='person_custom_attribute_values_sainty',
    tags=["saintyhird"]
) }}

WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id
    FROM {{ ref('2_people_ff') }}
),
internal_options AS (
    SELECT 
        ca.atlas_id AS atlas_attribute_id,
        ca.alias AS attribute_alias,
        cao.atlas_id AS option_id,
        cao.value AS option_value
    FROM {{ ref('2_custom_attribute_options_sainty') }} cao
    INNER JOIN {{ ref('1_custom_attributes_sainty') }} ca 
        ON ca.atlas_id = cao.atlas_attribute_id
    WHERE ca.of = 'person'
),
status_values AS (
    SELECT DISTINCT p.idperson AS person_id, 'Successful Candidate' AS value, 'status' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personx p2 ON p2.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.previouscandidate s ON LOWER(s.idpreviouscandidate) = p2.idpreviouscandidate_string
    WHERE LOWER(TRIM(s.value)) = 'placed'
    UNION
    SELECT DISTINCT p.idperson AS person_id, 'Successful Candidate' AS value, 'status' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personx p2 ON p2.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.personstatus s ON LOWER(s.idpersonstatus) = p2.idpersonstatus_string
    WHERE LOWER(TRIM(s.value)) = 'successful candidate'
),
nationality_values AS (
    SELECT DISTINCT p.idperson AS person_id, n.value AS value, 'nationality' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.nationality n ON n.idnationality = pc.codeid
    WHERE n.isactive = 1 AND n.value IS NOT NULL AND TRIM(n.value) != ''
),
current_location_values AS (
    SELECT DISTINCT p.idperson AS person_id, loc.value AS value, 'current_location' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}."location" loc ON loc.idlocation = pc.codeid
    WHERE loc.isactive = 1 AND loc.value IS NOT NULL AND TRIM(loc.value) != ''
),
language_values AS (
    SELECT DISTINCT p.idperson AS person_id, l.value AS value, 'language' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}."language" l ON l.idlanguage = pc.codeid
    WHERE l.isactive = 1 AND l.value IS NOT NULL AND TRIM(l.value) != ''
),
gender_values AS (
    SELECT DISTINCT p.idperson AS person_id, g.value AS value, 'gender' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.gender g ON g.idgender = p.idgender
    WHERE g.isactive = 1 AND g.value IS NOT NULL AND TRIM(g.value) != ''
),
industry_values AS (
    SELECT DISTINCT p.idperson AS person_id, ind.value AS value, 'industry' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.industry ind ON ind.idindustry = pc.codeid
    WHERE ind.isactive = 1 AND ind.value IS NOT NULL AND TRIM(ind.value) != ''
),
job_function_values AS (
    SELECT DISTINCT p.idperson AS person_id, jf.value AS value, 'job_function' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.jobfunction jf ON jf.idjobfunction = pc.codeid
    WHERE jf.isactive = 1 AND jf.value IS NOT NULL AND TRIM(jf.value) != ''
),
qualification_values AS (
    SELECT DISTINCT p.idperson AS person_id, q.value AS value, 'qualification' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.qualification q ON q.idqualification = pc.codeid
    WHERE q.isactive = 1 AND q.value IS NOT NULL AND TRIM(q.value) != ''
),
international_values AS (
    SELECT DISTINCT p.idperson AS person_id, intl.value AS value, 'international' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.international intl ON intl.idinternational = pc.codeid
    WHERE intl.isactive = 1 AND intl.value IS NOT NULL AND TRIM(intl.value) != ''
),
title_values AS (
    SELECT DISTINCT p.idperson AS person_id, u.value AS value, 'title' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.udskill2 u ON u.idudskill2 = pc.codeid
    WHERE u.isactive = 1 AND u.value IS NOT NULL AND TRIM(u.value) != ''
),
product_values AS (
    SELECT DISTINCT p.idperson AS person_id, u.value AS value, 'product' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.udskill1 u ON u.idudskill1 = pc.codeid
    WHERE u.isactive = 1 AND u.value IS NOT NULL AND TRIM(u.value) != ''
),
client_type_values AS (
    SELECT DISTINCT p.idperson AS person_id, u.value AS value, 'client_type' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.udskill3 u ON u.idudskill3 = pc.codeid
    WHERE u.isactive = 1 AND u.value IS NOT NULL AND TRIM(u.value) != ''
),
style_theme_values AS (
    SELECT DISTINCT p.idperson AS person_id, u.value AS value, 'style_theme' AS attribute_alias
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.udskill4 u ON u.idudskill4 = pc.codeid
    WHERE u.isactive = 1 AND u.value IS NOT NULL AND TRIM(u.value) != ''
),
combined_person_values AS (
    SELECT * FROM status_values
    UNION ALL SELECT * FROM nationality_values
    UNION ALL SELECT * FROM current_location_values
    UNION ALL SELECT * FROM language_values
    UNION ALL SELECT * FROM gender_values
    UNION ALL SELECT * FROM industry_values
    UNION ALL SELECT * FROM job_function_values
    UNION ALL SELECT * FROM qualification_values
    UNION ALL SELECT * FROM international_values
    UNION ALL SELECT * FROM title_values
    UNION ALL SELECT * FROM product_values
    UNION ALL SELECT * FROM client_type_values
    UNION ALL SELECT * FROM style_theme_values
)
SELECT DISTINCT
    {{ atlas_uuid('ip.person_id::text || io.atlas_attribute_id::text || io.option_id::text') }} AS atlas_id,
    ip.person_id,
    ip.atlas_person_id,
    io.atlas_attribute_id AS atlas_custom_attribute_id,
    io.option_id AS atlas_option_id,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM combined_person_values cpv
INNER JOIN internal_persons ip ON ip.person_id = cpv.person_id
INNER JOIN internal_options io 
    ON io.attribute_alias = cpv.attribute_alias 
    AND io.option_value = cpv.value
ORDER BY ip.person_id, io.atlas_attribute_id


