{{ config(materialized='table', alias='users_rcrm') }}

{% set db = var('source_database') %}

SELECT 
    id,
    {{ atlas_uuid('id') }} AS atlas_id, 
    COALESCE(email, lower(first_name) || '@claritycbs.com') AS email,
    first_name || ' ' || last_name AS name,
    'active' AS status,
    to_char(current_date,  'YYYY-MM-DDTHH24:MI:SS') AS created_at,
    to_char(current_date,  'YYYY-MM-DDTHH24:MI:SS') AS updated_at  
FROM {{ db }}.user_data