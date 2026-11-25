{#-
    Helpers for partition-replaced models (blueprint 7.7).

    The orchestrating task decides which partitions are stale and passes them to dbt as the
    variable `partitions`: a list of partition values, for example [202611] for a model
    partitioned by toYYYYMM(date) or ['2026-11-25'] for one partitioned by day. A model reads
    exactly those partitions from its input and the materialisation replaces exactly those
    partitions in the mart.
-#}

{#-
    The partitions requested for this run, or none when the variable is not set. An
    orchestrator may hand the list over as a JSON string ("[202611]"); it is parsed here so
    that a model never iterates over the characters of a string.
-#}
{% macro requested_partitions() -%}
    {%- set partitions = var('partitions', none) -%}
    {%- if partitions is string -%}
        {%- set partitions = fromjson(partitions) -%}
    {%- endif -%}
    {{ return(partitions) }}
{%- endmacro %}

{#- One partition value as a SQL literal: numbers as they are, everything else quoted. -#}
{% macro partition_literal(value) -%}
    {%- if value is number -%}
        {{ value }}
    {%- else -%}
        '{{ value | string | replace("\\", "\\\\") | replace("'", "\\'") }}'
    {%- endif -%}
{%- endmacro %}

{#- A predicate that is true for rows of the given partitions. An empty list matches nothing. -#}
{% macro partition_in_filter(expression, partitions) -%}
    {%- if partitions | length == 0 -%}
        0 = 1
    {%- else -%}
        {{ expression }} in (
            {%- for value in partitions -%}
                {{ partition_literal(value) }}{{ ', ' if not loop.last }}
            {%- endfor -%}
        )
    {%- endif -%}
{%- endmacro %}

{#-
    True when this run replaces partitions of an existing table; false on the first build and
    on a full refresh, when the whole table is built. The counterpart of dbt's is_incremental()
    for the partition_replace materialisation.
-#}
{% macro is_partition_run() -%}
    {%- if not execute -%}
        {{ return(false) }}
    {%- endif -%}
    {%- set relation = load_relation(this) -%}
    {{ return(relation is not none and not relation.is_view and not should_full_refresh()) }}
{%- endmacro %}

{#-
    For a model's WHERE clause: restricts the input to the requested partitions on a partition
    run, and to everything on a first build or a full refresh.
-#}
{% macro requested_partitions_filter(expression) -%}
    {%- set partitions = requested_partitions() -%}
    {%- if is_partition_run() and partitions is not none -%}
        {{ partition_in_filter(expression, partitions) }}
    {%- else -%}
        1 = 1
    {%- endif -%}
{%- endmacro %}
