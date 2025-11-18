{% macro is_personal_email(email_field) %}
(
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@gmail%' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@hotmail%' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@outlook%' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@yahoo%' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@aol%' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@icloud%' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@me.com' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@protonmail%' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@live.com' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@msn.com' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@comcast.net' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@mail.com' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@zoho.com' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@gmx%' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@yandex%' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@qq.com' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@naver.com' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@web.com' OR
    LOWER(REGEXP_SUBSTR({{ email_field }}, '@([^@]+)$')) LIKE '%@t-mobile.com'
)
{% endmacro %} 