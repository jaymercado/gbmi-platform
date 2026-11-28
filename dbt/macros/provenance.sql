{#-
    The provenance columns of a mart which is rebuilt in full (blueprint 7.12).

    silver_table is the main silver table of the mart. The task pin_lake of the DAG marts_build
    passes the Iceberg snapshot of each pinned table in the variable silver_snapshot_ids, and
    the version of the pipeline code in the variable pipeline_version. A build without the
    variables (make dbt-build) writes 0 and 'unknown'.
-#}
{% macro silver_snapshot_id(silver_table) -%}
    {%- set snapshot_ids = var('silver_snapshot_ids', none) -%}
    {%- if snapshot_ids is mapping and snapshot_ids.get(silver_table) is not none -%}
        {{ snapshot_ids.get(silver_table) | int }}
    {%- else -%}
        0
    {%- endif -%}
{%- endmacro %}

{% macro mart_provenance(silver_table) -%}
    toInt64({{ silver_snapshot_id(silver_table) }}) as _silver_snapshot_id,
    '{{ var("pipeline_version", "unknown") }}' as _pipeline_version,
    now64(3, 'UTC') as _built_at,
    '{{ invocation_id }}' as _dbt_invocation_id
{%- endmacro %}
