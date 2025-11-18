{% macro get_agency_filter(filter_name) %}
    {% set filter_config = var(
        'agency_filters',{}
    ) %}
    {% if filter_name in filter_config %}
        {% set where_clause = filter_config[filter_name]['where_clause'] %}
        {% if where_clause and where_clause.strip() %}
            {{ where_clause }}
        {% else %}
            ('')
        {% endif %}
    {% else %}
        ('')
    {% endif %}
{% endmacro %}