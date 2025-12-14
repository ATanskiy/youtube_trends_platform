{% macro drop_table_if_exists(relation) %}
  {% if execute %}
    {{ adapter.execute(
        "DROP TABLE IF EXISTS " ~ relation,
        auto_begin=True
    ) }}
  {% endif %}
{% endmacro %}