{% macro email_norm(col) -%}
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REPLACE(LOWER(TRIM({{ col }})), ' ', ''),
            '^[^a-zA-Z0-9@._-]+', ''
        ),
        '[^a-zA-Z0-9@._-]+$', ''
    )
{%- endmacro %}

{% macro phone_norm(col) -%}
nullif(
  case
    when regexp_instr({{ col }}::varchar, '^[[:space:]]*[+]') = 1
      then '+' || regexp_replace({{ col }}::varchar, '[^0-9]', '')
    when regexp_instr({{ col }}::varchar, '^[[:space:]]*00') = 1
      then '+' || regexp_replace(regexp_replace({{ col }}::varchar, '[^0-9]', ''), '^00', '')
    else regexp_replace({{ col }}::varchar, '[^0-9]', '')
  end,
  ''
)
{%- endmacro %}

{% macro linkedin_norm(col) -%}
    TRIM(
        BOTH '/'
        FROM REPLACE(
               REPLACE(
                 REPLACE(
                   REPLACE(
                     REPLACE(
                       REPLACE(TRIM({{ col }}), ' ', ''),
                       'https://www.', ''
                     ),
                     'http://www.', ''
                   ),
                   'https://', ''
                 ),
                 'http://', ''
               ),
               'www.', ''
             )
    )
{%- endmacro %}

{% macro website_norm(col) -%}
    TRIM(
        BOTH '/'
        FROM REPLACE(
               REPLACE(
                 REPLACE(
                   REPLACE(
                     REPLACE(
                       REPLACE(LOWER(TRIM({{ col }})), ' ', ''),
                       'https://www.', ''
                     ),
                     'http://www.', ''
                   ),
                   'https://', ''
                 ),
                 'http://', ''
               ),
               'www.', ''
             )
    )
{%- endmacro %}