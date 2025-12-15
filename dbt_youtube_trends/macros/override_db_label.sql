{% macro dbt__doc_catalog_label(model) %}
{{ return("[" ~ model.database ~ "] " ~ model.name) }}
{% endmacro %}