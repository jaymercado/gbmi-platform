{#-
    The validity intervals of a company from the daily snapshots (blueprint 7.10).

    Silver holds one row for each company and snapshot date. Consecutive snapshots of a company
    with the same attr_hash describe the same attributes: name, primary ticker, primary
    exchange, SIC code, state and end of the fiscal year. Each run of equal hashes becomes one
    interval. An interval starts on the snapshot date of its first row and ends on the start of
    the next interval of the company; the end date itself is not part of the interval. The last
    interval of a company is open and ends on 9999-12-31.

    The window frames are explicit: with the default frame, leadInFrame would see no row after
    the current row and would return its default value for every row.
-#}
with changes as (
    select
        cik,
        snapshot_date,
        attr_hash,
        name,
        primary_ticker,
        primary_exchange,
        sic,
        sic_description,
        state_of_incorporation,
        fiscal_year_end,
        category,
        entity_type,
        lagInFrame(attr_hash, 1, '') over company_days as previous_hash
    from {{ ref('stg_sec__company_snapshot') }}
    window company_days as (
        partition by cik
        order by snapshot_date
        rows between unbounded preceding and unbounded following
    )
),

versions as (
    select *
    from changes
    -- An attr_hash is never empty, so the empty text marks the first snapshot of a company.
    where previous_hash = '' or previous_hash != attr_hash
)

select
    cik,
    snapshot_date as valid_from,
    leadInFrame(snapshot_date, 1, toDate32('9999-12-31')) over company_versions as valid_to,
    attr_hash,
    name,
    primary_ticker,
    primary_exchange,
    sic,
    sic_description,
    state_of_incorporation,
    fiscal_year_end,
    category,
    entity_type
from versions
window company_versions as (
    partition by cik
    order by snapshot_date
    rows between unbounded preceding and unbounded following
)
