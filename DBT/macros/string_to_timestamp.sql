-- Example input [ 09/09/2020 12:06:47 ]
{% macro string_to_timestamp(datetime_expr, output_format='YYYY-MM-DD"T"HH24:MI:SS') %}
    TO_CHAR(
        (
            CASE
                WHEN regexp_instr({{ datetime_expr }}::varchar, '^[0-9]{2}/[0-9]{2}/[0-9]{4}') = 1
                    THEN to_timestamp({{ datetime_expr }}::varchar, 'DD/MM/YYYY HH24:MI:SS')
                WHEN regexp_instr({{ datetime_expr }}::varchar, '^[0-9]{4}-[0-9]{2}-[0-9]{2}') = 1
                    THEN to_timestamp({{ datetime_expr }}::varchar, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END
        )::timestamp(0),
        '{{ output_format }}'
    )
{% endmacro %}