{{ config(
    materialized='table',
    alias='custom_attribute_options_sainty',
    tags=["saintyhird"]
) }}

WITH internal_attributes AS (
    SELECT
        atlas_id,
        alias,
        of
    FROM
        {{ ref('1_custom_attributes_sainty') }}
),
statuses AS (
    SELECT 'Successful Candidate' AS value
),
nationalities AS (
    SELECT DISTINCT n.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.nationality n ON n.idnationality = pc.codeid
    WHERE n.isactive = 1 AND n.value IS NOT NULL AND TRIM(n.value) != ''
),
current_locations AS (
    SELECT DISTINCT loc.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}."location" loc ON loc.idlocation = pc.codeid
    WHERE loc.isactive = 1 AND loc.value IS NOT NULL AND TRIM(loc.value) != ''
),
languages AS (
    SELECT DISTINCT l.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}."language" l ON l.idlanguage = pc.codeid
    WHERE l.isactive = 1 AND l.value IS NOT NULL AND TRIM(l.value) != ''
),
genders AS (
    SELECT DISTINCT g.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.gender g ON g.idgender = p.idgender
    WHERE g.isactive = 1 AND g.value IS NOT NULL AND TRIM(g.value) != ''
),
industries AS (
    SELECT DISTINCT ind.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.industry ind ON ind.idindustry = pc.codeid
    WHERE ind.isactive = 1 AND ind.value IS NOT NULL AND TRIM(ind.value) != ''
),
job_functions AS (
    SELECT DISTINCT jf.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.jobfunction jf ON jf.idjobfunction = pc.codeid
    WHERE jf.isactive = 1 AND jf.value IS NOT NULL AND TRIM(jf.value) != ''
),
qualifications AS (
    SELECT DISTINCT q.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.qualification q ON q.idqualification = pc.codeid
    WHERE q.isactive = 1 AND q.value IS NOT NULL AND TRIM(q.value) != ''
),
internationals AS (
    SELECT DISTINCT intl.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.international intl ON intl.idinternational = pc.codeid
    WHERE intl.isactive = 1 AND intl.value IS NOT NULL AND TRIM(intl.value) != ''
),
titles AS (
    SELECT DISTINCT u.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.udskill2 u ON u.idudskill2 = pc.codeid
    WHERE u.isactive = 1 AND u.value IS NOT NULL AND TRIM(u.value) != ''
),
products AS (
    SELECT DISTINCT u.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.udskill1 u ON u.idudskill1 = pc.codeid
    WHERE u.isactive = 1 AND u.value IS NOT NULL AND TRIM(u.value) != ''
),
client_types AS (
    SELECT DISTINCT u.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.udskill3 u ON u.idudskill3 = pc.codeid
    WHERE u.isactive = 1 AND u.value IS NOT NULL AND TRIM(u.value) != ''
),
style_themes AS (
    SELECT DISTINCT u.value AS value
    FROM {{ var('source_database') }}.person p
    INNER JOIN {{ var('source_database') }}.personcode pc ON pc.idperson = p.idperson
    INNER JOIN {{ var('source_database') }}.udskill4 u ON u.idudskill4 = pc.codeid
    WHERE u.isactive = 1 AND u.value IS NOT NULL AND TRIM(u.value) != ''
),
project_industries AS (
    SELECT DISTINCT i.value AS value
    FROM {{ var('source_database') }}."assignment" a
    INNER JOIN {{ var('source_database') }}.assignmentcode ac ON ac.idassignment = a.idassignment
    INNER JOIN {{ var('source_database') }}.industry i ON i.idindustry = ac.codeid
    WHERE i.isactive = 1 AND i.value IS NOT NULL AND TRIM(i.value) != ''
),
project_functions AS (
    SELECT DISTINCT j.value AS value
    FROM {{ var('source_database') }}."assignment" a
    INNER JOIN {{ var('source_database') }}.assignmentcode ac ON ac.idassignment = a.idassignment
    INNER JOIN {{ var('source_database') }}.jobfunction j ON j.idjobfunction = ac.codeid
    WHERE j.isactive = 1 AND j.value IS NOT NULL AND TRIM(j.value) != ''
),
project_internationals AS (
    SELECT DISTINCT i.value AS value
    FROM {{ var('source_database') }}."assignment" a
    INNER JOIN {{ var('source_database') }}.assignmentcode ac ON ac.idassignment = a.idassignment
    INNER JOIN {{ var('source_database') }}.international i ON i.idinternational = ac.codeid
    WHERE i.isactive = 1 AND i.value IS NOT NULL AND TRIM(i.value) != ''
),
company_industries AS (
    SELECT DISTINCT i.value AS value
    FROM {{ var('source_database') }}.company c
    INNER JOIN {{ var('source_database') }}.companycode cc ON cc.idcompany = c.idcompany
    INNER JOIN {{ var('source_database') }}.industry i ON i.idindustry = cc.codeid
    WHERE i.isactive = 1 AND i.value IS NOT NULL AND TRIM(i.value) != ''
),
combined_values AS (
    SELECT 'status' AS alias, 'person' AS of, value FROM statuses
    UNION ALL
    SELECT 'nationality' AS alias, 'person' AS of, value FROM nationalities
    UNION ALL
    SELECT 'current_location' AS alias, 'person' AS of, value FROM current_locations
    UNION ALL
    SELECT 'gender' AS alias, 'person' AS of, value FROM genders
    UNION ALL
    SELECT 'industry' AS alias, 'person' AS of, value FROM industries
    UNION ALL
    SELECT 'job_function' AS alias, 'person' AS of, value FROM job_functions
    UNION ALL
    SELECT 'qualification' AS alias, 'person' AS of, value FROM qualifications
    UNION ALL
    SELECT 'international' AS alias, 'person' AS of, value FROM internationals
    UNION ALL
    SELECT 'language' AS alias, 'person' AS of, value FROM languages
    UNION ALL
    SELECT 'client_type' AS alias, 'person' AS of, value FROM client_types
    UNION ALL
    SELECT 'title' AS alias, 'person' AS of, value FROM titles
    UNION ALL
    SELECT 'product' AS alias, 'person' AS of, value FROM products
    UNION ALL
    SELECT 'style_theme' AS alias, 'person' AS of, value FROM style_themes
    UNION ALL
    SELECT 'project_industry' AS alias, 'project' AS of, value FROM project_industries
    UNION ALL
    SELECT 'project_function' AS alias, 'project' AS of, value FROM project_functions
    UNION ALL
    SELECT 'project_international' AS alias, 'project' AS of, value FROM project_internationals
    UNION ALL
    SELECT 'company_industry' AS alias, 'company' AS of, value FROM company_industries
)
SELECT
    cv.alias || '_' || value AS id,
    {{ atlas_uuid("cv.alias || value") }} AS atlas_id,
    ia.atlas_id AS atlas_attribute_id,
    value AS value,
    ROW_NUMBER() OVER (PARTITION BY ia.atlas_id ORDER BY value ASC) AS position,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS created_at,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T00:00:00"') AS updated_at,
    '{{ var('agency_id') }}' AS agency_id
FROM 
    combined_values cv
INNER JOIN 
    internal_attributes ia ON ia.alias = cv.alias AND ia.of = cv.of
ORDER BY
    atlas_attribute_id,
    position