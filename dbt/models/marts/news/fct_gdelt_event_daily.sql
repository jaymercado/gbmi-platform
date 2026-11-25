{#-
    Events per UTC day, action country, CAMEO root code and first-actor type (blueprint 7.6).

    The day is the slot date: the UTC day on which GDELT published the event (blueprint 7.11).
    GDELT's own event date can lie years in the past, so it is not used for partitioning.

    Partition-replaced by month (blueprint 7.7): a run rebuilds exactly the months named in
    the variable `partitions`. The provenance columns say which Iceberg snapshot and which
    dbt run a partition came from (blueprint 7.12).
-#}
{{ config(
    materialized='partition_replace',
    partition_by='toYYYYMM(date)',
    order_by='(date, action_geo_country_code, event_root_code, actor1_type1_code)',
    engine='MergeTree()'
) }}

select
    slot_date as date,
    coalesce(action_geo_country_code, '') as action_geo_country_code,
    coalesce(event_root_code, '') as event_root_code,
    coalesce(actor1_type1_code, '') as actor1_type1_code,
    count() as events,
    sum(coalesce(num_mentions, 0)) as mentions,
    avg(avg_tone) as mean_tone,
    toInt64({{ var('silver_snapshot_id', 0) }}) as _silver_snapshot_id,
    '{{ var("pipeline_version", "unknown") }}' as _pipeline_version,
    now64(3, 'UTC') as _built_at,
    '{{ invocation_id }}' as _dbt_invocation_id
from {{ ref('stg_gdelt__event') }}
where {{ requested_partitions_filter('toYYYYMM(slot_date)') }}
group by
    date,
    action_geo_country_code,
    event_root_code,
    actor1_type1_code
