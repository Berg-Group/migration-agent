-- macros/get_client_config.sql
{% macro get_client_config() %}


    {% set client_name = var('client_name') %}
    {% set clients_dict = var('clients') %}
    {% if client_name not in clients_dict.keys() %}
      {{ exceptions.raise_compiler_error("No config found for client '" ~ client_name ~ "'") }}
    {% endif %}

    {{ return(clients_dict[client_name]) }}
{% endmacro %}
