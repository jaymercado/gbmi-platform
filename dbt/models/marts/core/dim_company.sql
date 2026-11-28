{#-
    The company dimension, type 2: one row for each company of the universe and validity
    interval (blueprint 7.6 and 7.10).

    The dimension is derived, not accumulated: the rows are a function of the daily snapshots
    and of the former names of EDGAR, so the model is rebuilt in full on every run. An interval
    starts on valid_from and ends on valid_to; the day valid_to itself is not part of the
    interval. The open end of the current row is 9999-12-31.

    A row with the source snapshot carries the attributes which the platform saw on its own
    snapshot dates. A row with the source former_name describes a time before the first
    snapshot and carries only the name: the other attributes of that time are not known.
-#}
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(cik, valid_from)'
) }}

with versions as (
    select
        cik,
        valid_from,
        valid_to,
        'snapshot' as history_source,
        name,
        primary_ticker,
        primary_exchange,
        sic,
        sic_description,
        state_of_incorporation,
        fiscal_year_end,
        category,
        entity_type
    from {{ ref('int_company__snapshot_versions') }}

    union all

    select
        cik,
        valid_from,
        valid_to,
        'former_name' as history_source,
        name,
        cast(null as Nullable(String)) as primary_ticker,
        cast(null as Nullable(String)) as primary_exchange,
        cast(null as Nullable(String)) as sic,
        cast(null as Nullable(String)) as sic_description,
        cast(null as Nullable(String)) as state_of_incorporation,
        cast(null as Nullable(String)) as fiscal_year_end,
        cast(null as Nullable(String)) as category,
        cast(null as Nullable(String)) as entity_type
    from {{ ref('int_company__former_name_versions') }}
    where range_status = 'kept'
)

select
    cik,
    valid_from,
    valid_to,
    cast(valid_to = toDate32('9999-12-31') as Bool) as is_current,
    history_source,
    name,
    primary_ticker,
    primary_exchange,
    sic,
    sic_description,
    state_of_incorporation,
    fiscal_year_end,
    category,
    entity_type,
    cast(cik in (select watchlist.cik from {{ ref('int_company__watchlist') }} as watchlist) as Bool) as is_watchlist,
    {{ mart_provenance('sec_company_snapshot') }}
from versions
