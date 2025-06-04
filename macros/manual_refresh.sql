{% macro manual_refresh(table_name) %}
  update {{ source('stg', 'z_refresh_from') }} set to_refresh = 0 where table_name = '{{ table_name }}'
{% endmacro %}