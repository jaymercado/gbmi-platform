{#-
    Slot status and completeness for each UTC day (blueprint 8.2; week 2 plan, WP-08).

    One row for each source, slot date and feed, and one row for each source and slot date
    over all feeds (feed = 'all'). A day of one feed has 96 slots in the calendar:

        completeness = loaded slots / (calendar slots - confirmed-missing slots)

    A day is complete when every slot of the calendar is loaded or confirmed missing. The
    model is rebuilt in full from the ledger on every run, so a completeness value cannot
    go stale when the state of a slot changes (blueprint 7.6).
-#}
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(source, slot_date, feed)'
) }}

with by_feed as (
    select
        source,
        slot_date,
        feed,
        toUInt32(1) as feeds,
        toUInt32(countIf(state = 'expected')) as expected_slots,
        toUInt32(countIf(state = 'listed')) as listed_slots,
        toUInt32(countIf(state = 'downloaded')) as downloaded_slots,
        toUInt32(countIf(state = 'verified')) as verified_slots,
        toUInt32(countIf(state = 'loaded')) as loaded_slots,
        toUInt32(countIf(state = 'quarantined')) as quarantined_slots,
        toUInt32(countIf(state = 'confirmed_missing')) as confirmed_missing_slots,
        toUInt32(count()) as ledger_slots,
        sum(coalesce(raw_lines, 0)) as raw_lines,
        sum(coalesce(parsed_rows, 0)) as parsed_rows,
        sum(coalesce(quarantined_rows, 0)) as quarantined_rows
    from {{ ref('stg_ops__source_slot') }}
    group by
        source,
        slot_date,
        feed
),

all_feeds as (
    select
        source,
        slot_date,
        'all' as feed,
        toUInt32({{ var('gdelt_feeds') | length }}) as feeds,
        sum(expected_slots) as expected_slots,
        sum(listed_slots) as listed_slots,
        sum(downloaded_slots) as downloaded_slots,
        sum(verified_slots) as verified_slots,
        sum(loaded_slots) as loaded_slots,
        sum(quarantined_slots) as quarantined_slots,
        sum(confirmed_missing_slots) as confirmed_missing_slots,
        sum(ledger_slots) as ledger_slots,
        sum(raw_lines) as raw_lines,
        sum(parsed_rows) as parsed_rows,
        sum(quarantined_rows) as quarantined_rows
    from by_feed
    group by
        source,
        slot_date
),

combined as (
    select * from by_feed
    union all
    select * from all_feeds
)

select
    source,
    slot_date,
    feed,
    toUInt32({{ var('slots_per_day') }} * feeds) as calendar_slots,
    toUInt32(ledger_slots) as ledger_slots,
    toUInt32(expected_slots) as expected_slots,
    toUInt32(listed_slots) as listed_slots,
    toUInt32(downloaded_slots) as downloaded_slots,
    toUInt32(verified_slots) as verified_slots,
    toUInt32(loaded_slots) as loaded_slots,
    toUInt32(quarantined_slots) as quarantined_slots,
    toUInt32(confirmed_missing_slots) as confirmed_missing_slots,
    -- A day on which every slot is confirmed missing has nothing left to load.
    if(
        calendar_slots = confirmed_missing_slots,
        1.0,
        loaded_slots / (calendar_slots - confirmed_missing_slots)
    ) as completeness,
    loaded_slots + confirmed_missing_slots = calendar_slots as is_complete,
    toUInt64(raw_lines) as raw_lines,
    toUInt64(parsed_rows) as parsed_rows,
    toUInt64(quarantined_rows) as quarantined_rows,
    now64(3, 'UTC') as _built_at,
    '{{ invocation_id }}' as _dbt_invocation_id
from combined
