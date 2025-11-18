{% macro atlas_uuid(val) %}
lower(
       substring(md5('{{ var("clientName") }}' || {{ val }}::text),  1,  8) || '-' ||
       substring(md5('{{ var("clientName") }}' || {{ val }}::text),  9,  4) || '-' ||
       substring(md5('{{ var("clientName") }}' || {{ val }}::text), 13,  4) || '-' ||
       substring(md5('{{ var("clientName") }}' || {{ val }}::text), 17,  4) || '-' ||
       substring(md5('{{ var("clientName") }}' || {{ val }}::text), 21, 12)
)
{% endmacro %}
