{#-
    Models land in the database named by their `schema` configuration, exactly as written:
    staging, intermediate, marts, ops, seeds (blueprint 7.1). dbt's default would prefix the
    target schema, giving names such as marts_staging.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
