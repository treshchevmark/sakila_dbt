{% macro clean_string(col) %}
    lower(regexp_replace({{ col }}, '[^a-zA-Z0-9]', '', 'g'))
{% endmacro %}