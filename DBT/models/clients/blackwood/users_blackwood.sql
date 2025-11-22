{{ config(
    materialized='table',
    alias='users_blackwood',
    tags=["blackwood"]
) }}

with b_users AS (
    SELECT 
        id::text, 
        username AS name,
        CASE 
            WHEN TRIM(email) = '' OR email ISNULL 
                THEN LOWER(regexp_replace(username, ' ', '.')) || '@' || '{{var('domain')}}' 
            ELSE lower(email) 
        END AS email,
        'disabled' AS status
    FROM {{ var('source_database') }}."user"
)

select
    u.id,
    CASE WHEN u.id = '16' THEN '{{ var("master_id") }}' ELSE {{atlas_uuid('u.id')}} END as atlas_id,
    u.name,
    u.email,
    u.status
from b_users u
