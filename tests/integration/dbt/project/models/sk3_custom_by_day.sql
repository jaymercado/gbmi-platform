{#- The partition_replace materialisation, partitioned by day (spike SK3). -#}
{{ config(
    materialized='partition_replace',
    partition_by='slot_date',
    order_by='(slot_date, category)'
) }}

select
    slot_date,
    category,
    count() as events,
    sum(amount) as amount,
    max(id) as max_id
from {{ source('sk3', 'source_events') }}
where {{ requested_partitions_filter('slot_date') }}
group by
    slot_date,
    category
