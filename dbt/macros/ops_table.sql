{#-
    A table of the schema ops in the operational database (PostgreSQL), read through the named
    collection pg_ops (week 2 plan, WP-08). The collection holds host, port, database, user and
    password of the read-only role gbmi_ro; the service clickhouse-init creates it. A model
    names the collection and never a password.
-#}
{% macro ops_table(name) -%}
    postgresql(pg_ops, schema = 'ops', table = '{{ name }}')
{%- endmacro %}
