-- The validity intervals of a company in dim_company follow each other without an overlap, and
-- each interval holds one day or more (blueprint 7.10). valid_to is not part of an interval, so
-- an interval may start on the valid_to of the interval before.
-- The test fails if this query returns a row.
with ordered as (
    select
        cik,
        valid_from,
        valid_to,
        history_source,
        leadInFrame(valid_from, 1, toDate32('9999-12-31')) over company_intervals as next_valid_from
    from {{ ref('dim_company') }}
    window company_intervals as (
        partition by cik
        order by valid_from, valid_to
        rows between unbounded preceding and unbounded following
    )
)

select
    cik,
    valid_from,
    valid_to,
    history_source,
    next_valid_from
from ordered
where
    valid_to <= valid_from
    or next_valid_from < valid_to
