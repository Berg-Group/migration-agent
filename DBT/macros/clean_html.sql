{% macro clean_html(col) %}
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            {{ col }},
                            '<(script|style)[\\s\\S]*?</\\1>', ' ', 1, 'ip'
                        ),
                        '<br\\s*/?>', '\n', 1, 'ip'
                    ),
                    '<[^>]+>', ' ', 1, 'p'
                ),
                '&[A-Za-z0-9#]+;', ' ', 1
            ),
            '\\s+', ' ', 1
        )
    )
{% endmacro %}
