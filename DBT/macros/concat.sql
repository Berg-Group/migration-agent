{% macro concat(fields) %}
    {% set fields_list = fields.split(',') %}
    
    COALESCE(
        NULLIF(
            TRIM(
                {% for field in fields_list %}
                    {% if not loop.first %}|| ' ' || {% endif %}
                    COALESCE({{ field }}, '')
                {% endfor %}
            ),
            ''
        ),
        ''
    )
{% endmacro %} 