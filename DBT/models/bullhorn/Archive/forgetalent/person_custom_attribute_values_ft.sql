{{ config(
    materialized='table',
    alias='person_custom_attribute_values_forgetalent',
    tags=["forgetalent"]
) }}


WITH internal_persons AS (
    SELECT 
        id AS person_id,
        atlas_id AS atlas_person_id 
    FROM {{ ref('1_people_ft') }}
),  

internal_options AS (
    SELECT 
        atlas_attribute_id,
        atlas_id AS option_id,
        atlas_attribute_id AS atlas_custom_attribute_id,
        atlas_id AS atlas_option_id,
        value
    FROM 
        {{ref('custom_attribute_options_ft')}}
),

candidate AS (
    SELECT 
        userid AS person_id, 
        trim(lower(occupation)) AS value,
        ca.atlas_id AS atlas_custom_attribute_id
    FROM 
        {{ var('source_database') }}."bh_usercontact"
    CROSS JOIN 
        {{ref('custom_attributes_ft')}} ca
    WHERE 
        occupation NOTNULL AND TRIM(occupation) <> ''
        AND ca.name = 'Candidate'
),

immediately_available AS (
    SELECT 
        userid AS person_id,
        customtext1 AS value,
        ca.atlas_id AS atlas_custom_attribute_id 
    FROM
        {{ var('source_database') }}."bh_usercontact"
    CROSS JOIN 
        {{ref('custom_attributes_ft')}} ca
    WHERE 
        customtext1 NOTNULL AND TRIM(customtext1) <> ''
        AND ca.name = 'Immediately Available'
),

employment_preference AS (
    SELECT 
        userid AS person_id,
        employmentpreference AS value,
        ca.atlas_id AS atlas_custom_attribute_id
    FROM 
        {{ var('source_database') }}."bh_usercontact"
    CROSS JOIN 
        {{ref('custom_attributes_ft')}} ca
    WHERE 
        employmentpreference NOTNULL AND TRIM(employmentpreference) <> ''
    AND ca.name = 'Employment Preference'
),

notice_period AS (
    SELECT 
        userid AS person_id,
        customtext4 AS value,
        ca.atlas_id AS atlas_custom_attribute_id
    FROM 
        {{ var('source_database') }}."bh_usercontact"
    CROSS JOIN 
        {{ref('custom_attributes_ft')}} ca
    WHERE 
        customtext4 NOTNULL AND TRIM(customtext4) <> ''
    AND ca.name = 'Notice Period'
),

qualification AS (
    SELECT 
        userid AS person_id,
        customtext21 AS value,
        ca.atlas_id AS atlas_custom_attribute_id 
    FROM 
        {{ var('source_database') }}."bh_useradditionalcustomfields"
    CROSS JOIN 
        {{ref('custom_attributes_ft')}} ca
    WHERE 
        customtext21 NOTNULL AND TRIM(customtext21) <> ''
    AND ca.name = 'Qualification'
),

status AS (
    SELECT 
        userid AS person_id,
        status AS value,
        ca.atlas_id AS atlas_custom_attribute_id
    FROM 
        {{ var('source_database') }}."bh_candidate"
    CROSS JOIN 
        {{ref('custom_attributes_ft')}} ca
    WHERE 
        status IN ('Active', 'Inactive', 'Placed', 'DNU')
    AND ca.name = 'Status'
),

all_values AS (
    SELECT * FROM candidate
    UNION ALL SELECT * FROM immediately_available
    UNION ALL SELECT * FROM employment_preference
    UNION ALL SELECT * FROM notice_period
    UNION ALL SELECT * FROM qualification
    UNION ALL SELECT * FROM status
),

mapped as (
    select
        ip.person_id,
        ip.atlas_person_id,
        io.atlas_custom_attribute_id,
        io.atlas_option_id
    from all_values v
    left join internal_persons  ip  USING (person_id)
    left join internal_options  io USING (value, atlas_custom_attribute_id)
    where v.value is not null and v.value <> ''
)

select  
    {{ atlas_uuid('m.atlas_person_id || m.atlas_custom_attribute_id || m.atlas_option_id') }} as atlas_id,
    m.person_id,
    m.atlas_person_id,
    m.atlas_custom_attribute_id,
    m.atlas_option_id,
    '2025-08-04T00:00:00' as created_at,
    '2025-08-04T00:00:00' as updated_at
from mapped m
where m.person_id is not null
  and m.atlas_option_id is not null
