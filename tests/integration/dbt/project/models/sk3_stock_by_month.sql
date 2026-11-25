{#- Stock dbt-clickhouse strategy, partitioned by month. Kept to show why it was not adopted:
    it replaces only the partitions present in the new data (spike SK3, test T4). -#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by='toYYYYMM(slot_date)',
    order_by='(slot_date, category)'
) }}

select
    slot_date,
    category,
    count() as events,
    sum(amount) as amount,
    max(id) as max_id
from {{ source('sk3', 'source_events') }}
{% if is_incremental() and var('partitions', none) is not none %}
    where {{ partition_in_filter('toYYYYMM(slot_date)', var('partitions')) }}
{% endif %}
group by
    slot_date,
    category
