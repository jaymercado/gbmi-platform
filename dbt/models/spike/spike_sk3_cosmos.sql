{#-
    Spike SK3, part B (week 1 plan, WP-08): a partition_replace model over generated rows.
    Each row records the dbt invocation that built it, which shows whether a partition list
    passed at trigger time reached dbt. Removed when the tracer DAG supersedes the spike.
-#}
{{ config(
    materialized='partition_replace',
    schema='staging',
    partition_by='toYYYYMM(day)',
    order_by='(day, id)',
    engine='MergeTree()'
) }}

select
    toDate32('2026-10-30') + number as day,
    number as id,
    '{{ invocation_id }}' as built_by
from numbers(5)
where {{ requested_partitions_filter('toYYYYMM(day)') }}
