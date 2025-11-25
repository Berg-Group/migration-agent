{{ config(
    materialized='table',
    alias='company_contacts_ff',
    tags=["filefinder"]
) }}

WITH company_contacts AS (
    SELECT 
        p.idperson AS person_id,
        c.idcompany AS company_id
    FROM {{ var('source_database') }}.assignmentcontact a 
    INNER JOIN {{ var('source_database') }}.person p ON p.idperson = a.idperson
    INNER JOIN {{ var('source_database') }}."assignment" a2 ON a2.idassignment = a.idassignment 
    INNER JOIN {{ var('source_database') }}.company c ON c.idcompany = a2.idcompany 
    GROUP BY 1, 2
),
titles AS (
    SELECT 
        p.idperson AS person_id,
        c.idcompany AS company_id,
        cp.jobtitle AS title
    FROM {{ var('source_database') }}.Company_Person cp 
    INNER JOIN {{ var('source_database') }}.Person p ON p.idPerson = cp.idPerson 
    INNER JOIN {{ var('source_database') }}.Company c ON c.idCompany = cp.idCompany 
    WHERE cp.isactiveemployment = true
)
SELECT
    id,
    atlas_id,
    person_id,
    atlas_person_id,
    company_id,
    atlas_company_id,
    created_at,
    updated_at,
    relationship,
    COALESCE(title, 'Missing Title') AS title,
    agency_id
FROM (
    SELECT
        {{ "'" ~ var('clientName') ~ "' || '_' || cc.person_id::text || 'companycontact' || cc.company_id::text" }} AS id,
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || '_' || cc.person_id::text || 'companycontact' || cc.company_id::text") }} AS atlas_id,
        cc.person_id,
        pl.atlas_id AS atlas_person_id,
        cc.company_id,
        cl.atlas_id AS atlas_company_id,
        TO_CHAR(current_timestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
        TO_CHAR(current_timestamp::timestamp(0), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
        CASE 
            WHEN t.person_id IS NOT NULL THEN 'client' 
            ELSE 'none' 
        END AS relationship,
        t.title,
        '{{ var('agency_id') }}' AS agency_id,
        ROW_NUMBER() OVER (
            PARTITION BY cc.person_id 
            ORDER BY 
            CASE 
                WHEN t.person_id IS NOT NULL THEN 0 
                ELSE 1 
            END, cc.company_id
        ) AS rn
    FROM company_contacts cc
    INNER JOIN {{ this.schema }}.people_ff pl ON pl.id = cc.person_id
    INNER JOIN {{ this.schema }}.companies_ff cl ON cl.id = cc.company_id
    LEFT JOIN titles t ON t.person_id = cc.person_id AND t.company_id = cc.company_id
) deduped
WHERE rn = 1