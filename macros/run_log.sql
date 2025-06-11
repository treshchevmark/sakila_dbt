{% macro run_log(log_type, model_name) %}
    {% if log_type == 'insert' %}
        INSERT INTO dwh.log_table (id, start_at, model_name) VALUES ('{{ invocation_id}}', now(), '{{model_name}}');
    {% endif %}
        UPDATE dwh.log_table SET end_at = now(), run_duration = extract(epoch from (now() - start_at)) WHERE id = '{{ invocation_id}}' and model_name = '{{model_name}}';
{% endmacro %}