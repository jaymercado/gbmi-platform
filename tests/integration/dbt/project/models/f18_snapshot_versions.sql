{#-
    Test-only model for finding F18 of the week 3 plan: does a dbt unit test run on
    dbt-clickhouse? The model is the interval logic of int_company__snapshot_versions over a
    table which the test creates, so the workflow dbt holds the proof without a lake.
-#}
{{ config(materialized='view') }}

with changes as (
    select
        cik,
        snapshot_date,
        attr_hash,
        name,
        lagInFrame(attr_hash, 1, '') over company_days as previous_hash
    from {{ source('sk3', 'source_snapshots') }}
    window company_days as (
        partition by cik
        order by snapshot_date
        rows between unbounded preceding and unbounded following
    )
),

versions as (
    select *
    from changes
    where previous_hash = '' or previous_hash != attr_hash
)

select
    cik,
    snapshot_date as valid_from,
    leadInFrame(snapshot_date, 1, toDate32('9999-12-31')) over company_versions as valid_to,
    attr_hash,
    name
from versions
window company_versions as (
    partition by cik
    order by snapshot_date
    rows between unbounded preceding and unbounded following
)
