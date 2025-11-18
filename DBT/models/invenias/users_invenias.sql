{{ config(
    materialized = 'table',
    alias = 'users_invenias',
    tags=["invenias"]
) }}

SELECT 
    u."itemid" AS id,
    COALESCE(u1.id, {{ atlas_uuid('u.itemid') }}) AS atlas_id,
    COALESCE(u1.email, u."username") AS email,
    COALESCE(u1.name, p."fileas") AS name,
    COALESCE(u1.status, 
    CASE WHEN u.disabled = FALSE THEN 'active' ELSE 'disabled' END) AS status
FROM 
    {{ var('source_database') }}."users" u 
LEFT JOIN "{{ this.schema }}"."users_prod" u1 
       ON lower(u1.email) = lower(trim(u."username"))
INNER JOIN {{ var('source_database') }}."relation_usertoperson" up 
        ON up."userid" = u."itemid"
INNER JOIN {{ var('source_database') }}."people" p 
        ON p.itemid = up."personid"
WHERE u."username" ILIKE '%campbellreed.com'