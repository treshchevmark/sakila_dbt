{% macro delete_from(table_name) %}
    delete from {{table_name}};
{% endmacro %}