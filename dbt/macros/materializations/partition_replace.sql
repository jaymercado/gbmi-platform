{#-
    partition_replace: the incremental contract for large facts (blueprint 7.7; week 1 plan,
    spike SK3).

    Each run replaces exactly the partitions that were requested, whether or not the new data
    contains rows for them:

      1. Build the model into a staging table with the target's structure.
      2. Refuse to continue if the staging table holds rows outside the requested partitions.
      3. ALTER TABLE target REPLACE PARTITION ID ... FROM staging, for every partition that
         the staging table holds and every requested partition that exists in the target.
         A requested partition with no new rows is replaced from an empty source and ends empty.
      4. Drop the staging table.

    Step 3 is what the adapter's own insert_overwrite strategy does not do: it replaces only
    the partitions present in the new data, so a partition whose rebuild returns no rows keeps
    its old rows.

    Each REPLACE PARTITION is atomic. If the run stops before step 3, the target is unchanged
    and the staging table is left behind under a name that carries the invocation id; the next
    run uses a new name and is not affected.

    Configuration:
      partition_by          required; the table's partition expression
      partition_expression  the expression that selects partitions in a WHERE clause
                            (default: partition_by)
      partitions            the requested partition values (default: var('partitions'))
      engine, order_by, ... as for any dbt-clickhouse table

    The first build and --full-refresh build the whole table and ignore `partitions`.
-#}
{% materialization partition_replace, adapter='clickhouse' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set partition_by = config.require('partition_by') -%}
  {%- set partition_expression = config.get('partition_expression', partition_by) -%}
  {%- set partitions = config.get('partitions', requested_partitions()) -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set full_refresh_mode = (should_full_refresh() or (existing_relation is not none and existing_relation.is_view)) -%}

  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {{ drop_relation_if_exists(load_cached_relation(intermediate_relation)) }}
  {{ drop_relation_if_exists(load_cached_relation(backup_relation)) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% set to_drop = [] %}

  {% if existing_relation is none %}
    {#- First build: create the table with every partition the model returns. -#}
    {% call statement('main') %}
        {{ get_create_table_as_sql(False, target_relation, sql) }}
    {% endcall %}

  {% elif full_refresh_mode %}
    {#- Full refresh: build beside the old table and swap atomically. -#}
    {% call statement('main') %}
        {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {% endcall %}
    {% if existing_relation.can_exchange %}
      {% do adapter.rename_relation(intermediate_relation, backup_relation) %}
      {% do exchange_tables_atomic(backup_relation, target_relation) %}
    {% else %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
    {% endif %}
    {% do to_drop.append(backup_relation) %}

  {% elif partitions is none %}
    {% do exceptions.raise_compiler_error(
        "partition_replace model " ~ this ~ " needs the partitions to replace: pass --vars "
        ~ "'{\"partitions\": [...]}', or use --full-refresh to rebuild the whole table."
    ) %}

  {% elif partitions | length == 0 %}
    {#- Nothing is stale: leave the table as it is. -#}
    {% call statement('main') %}
        select 1
    {% endcall %}

  {% else %}
    {% do partition_replace__swap(existing_relation, partition_expression, partitions) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or full_refresh_mode %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}

  {% for relation in to_drop %}
    {% do adapter.drop_relation(relation) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}


{% macro partition_replace__swap(existing_relation, partition_expression, partitions) %}
    {%- set staging_relation = existing_relation.incorporate(path={
        "identifier": existing_relation.identifier ~ '__dbt_new_data_' ~ invocation_id.replace('-', '_')
    }) -%}
    {%- set requested_filter = partition_in_filter(partition_expression, partitions) -%}
    {{ drop_relation_if_exists(staging_relation) }}

    {#- 1. Build the new data with the target's structure. -#}
    {% call statement('main') %}
        {{ get_create_table_as_sql(False, staging_relation, sql) }}
    {% endcall %}

    {#- 2. The model must return rows of the requested partitions only; otherwise a partition
           that nobody asked for would be replaced. -#}
    {% set outside_query %}
        select count() as outside from {{ staging_relation }} where not ({{ requested_filter }})
    {% endset %}
    {% set outside = run_query(outside_query).rows[0]['outside'] %}
    {% if outside > 0 %}
        {% do adapter.drop_relation(staging_relation) %}
        {% do exceptions.raise_compiler_error(
            "partition_replace model " ~ this ~ " returned " ~ outside ~ " rows outside the requested "
            ~ "partitions " ~ partitions ~ ". Restrict its input with requested_partitions_filter()."
        ) %}
    {% endif %}

    {#- 3. Partitions to replace: those with new data, and requested ones already in the
           target. The second set is what empties a partition whose rebuild returned no rows. -#}
    {% set partition_query %}
        select distinct partition_id
        from (
            select _partition_id as partition_id from {{ staging_relation }}
            union all
            select _partition_id as partition_id from {{ existing_relation }} where {{ requested_filter }}
        )
        order by partition_id
    {% endset %}
    {% set partition_ids = run_query(partition_query).rows | map(attribute='partition_id') | list %}

    {% if partition_ids %}
        {% call statement('replace_partitions') %}
            alter table {{ existing_relation }}
            {%- for partition_id in partition_ids %}
                replace partition id '{{ partition_id }}' from {{ staging_relation }}
                {{- ',' if not loop.last }}
            {%- endfor %}
            {#- ClickHouse 26.6 and later refuse to replace from a source that has no parts in
                the partition unless this is set; older servers do not know the setting. -#}
            {%- if adapter.is_at_or_after_version('26.6') %}
                settings allow_replace_partition_from_empty_source = 1
            {%- endif %}
        {% endcall %}
    {% endif %}

    {#- 4. The staging table has served its purpose. -#}
    {% do adapter.drop_relation(staging_relation) %}
{% endmacro %}
