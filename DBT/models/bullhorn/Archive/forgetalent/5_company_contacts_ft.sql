{{ config(
    materialized = 'table',
    alias = 'company_contacts_ft',
    tags=["bullhorn"]
) }}

WITH dupe_people_company_contacts AS (
    SELECT
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || 'companycontact' || bh.ClientID::TEXT") }} AS atlas_id,
        bh.ClientID AS id,
        p.id AS person_id,
        bh.ClientCorporationID AS company_id,
        c.atlas_id AS atlas_company_id,
        p.atlas_id AS atlas_person_id,
        TO_CHAR(bh.DateAdded::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS created_at,
        TO_CHAR(bh.DateAdded::timestamp(0), 'YYYY-MM-DD"T00:00:00Z"') AS updated_at,
        CASE
            WHEN bh.Status IN ('Client') THEN 'client'
            WHEN bh.Status IN ('Prospect','New Lead') THEN 'prospect'
            ELSE 'none'
        END AS relationship,
        uc.occupation AS title,
        '{{ var("agency_id") }}' AS agency_id
    FROM {{ var('source_database') }}."bh_client" bh
    INNER JOIN {{ ref('3_companies_bh') }} c ON bh.ClientCorporationID = c.id
    INNER JOIN {{ var('source_database') }}."bh_usercontact" uc on uc.UserID = bh.userid
    INNER JOIN {{ ref('people_dupes_bh') }} pd ON pd.contact_id = uc.userid
    INNER JOIN {{ ref('1_people_ft') }} p ON p.id = pd.candidate_id
    WHERE bh.isdeleted <> 1 AND uc.occupation IS NOT NULL
),
regular_company_contacts AS (
    SELECT
        {{ atlas_uuid("'" ~ var('clientName') ~ "' || 'companycontact' || bh.ClientID::TEXT") }} AS atlas_id,
        bh.ClientID AS id,
        bh.UserID AS person_id,
        bh.ClientCorporationID AS company_id,
        c.atlas_id AS atlas_company_id,
        p.atlas_id AS atlas_person_id,
        LEFT(bh.DateAdded::TEXT, 10) || 'T00:00:00Z' AS created_at,
        LEFT(bh.DateAdded::TEXT, 10) || 'T00:00:00Z' AS updated_at,
        CASE
            WHEN bh.Status IN ('Client') THEN 'client'
            WHEN bh.Status IN ('Prospect','New Lead') THEN 'prospect'
            ELSE 'none'
        END AS relationship,
        uc.occupation AS title,
        '{{ var("agency_id") }}' AS agency_id
    FROM {{ var('source_database') }}."bh_client" bh
    INNER JOIN {{ ref('3_companies_bh') }} c ON bh.ClientCorporationID = c.id
    INNER JOIN {{ ref('1_people_ft') }} p ON bh.UserID = p.id
    INNER JOIN {{ var('source_database') }}."bh_usercontact" uc ON bh.UserID = uc.UserID
    WHERE bh.isdeleted <> 1 AND uc.occupation IS NOT NULL
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
    title,
    agency_id
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id, person_id, company_id ORDER BY created_at DESC) AS rn
    FROM (
        SELECT * FROM dupe_people_company_contacts
        UNION ALL
        SELECT * FROM regular_company_contacts
    ) combined
) final
WHERE rn = 1