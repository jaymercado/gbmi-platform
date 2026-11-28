{#-
    The standard metrics of the watchlist companies by fiscal period (blueprint 7.6 and 8.7).

    One row for each company, metric, unit and period. The seven metrics are revenue, net
    income, operating income, total assets, equity, cash and employees. A flow metric has rows
    for quarters, half years, periods of nine months and years; a stock metric has one row for
    each balance date (period_kind instant, period_start = period_end).

    Each row carries two values: value_first_reported, from the first filing which reported
    the period, and value_latest, the newest known value. is_restated says that the two differ.
    A quarter without a three-month fact is derived as "year minus nine months" and carries
    is_derived (model int_financials__quarters).

    change_year_on_year compares value_latest with the latest value of the same kind of
    period one year before: the period whose end lies within 7 days of the date one year
    earlier, because a fiscal year can have 52 or 53 weeks. The change is the difference
    divided by the absolute value one year before, so a loss which shrinks shows a positive
    change. The value is NULL when no such period exists or when its value is 0.
-#}
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(cik, metric, period_kind, period_end, period_start, unit)'
) }}

with periods as (
    select
        cik,
        metric,
        period_kind,
        period_start,
        period_end,
        period_days,
        unit,
        taxonomy,
        concept,
        value_first_reported,
        first_filed_date,
        first_known_at,
        value_latest,
        latest_filed_date,
        latest_known_at,
        false as is_derived
    from {{ ref('int_financials__metric_facts') }}
    where period_kind in ('instant', 'half_year', 'nine_months', 'year')

    union all

    select
        cik,
        metric,
        'quarter' as period_kind,
        period_start,
        period_end,
        period_days,
        unit,
        taxonomy,
        concept,
        value_first_reported,
        first_filed_date,
        first_known_at,
        value_latest,
        latest_filed_date,
        latest_known_at,
        is_derived
    from {{ ref('int_financials__quarters') }}
),

year_ago as (
    -- The closest period end inside the 7 days wins.
    select
        periods.cik,
        periods.metric,
        periods.period_kind,
        periods.period_start,
        periods.period_end,
        periods.unit,
        toNullable(argMin(
            before.value_latest,
            (abs(dateDiff('day', before.period_end, addYears(periods.period_end, -1))), before.period_start)
        )) as value_year_ago,
        toNullable(argMin(
            before.period_end,
            (abs(dateDiff('day', before.period_end, addYears(periods.period_end, -1))), before.period_start)
        )) as year_ago_period_end
    from periods
    inner join periods as before
        on
            periods.cik = before.cik
            and periods.metric = before.metric
            and periods.period_kind = before.period_kind
            and periods.unit = before.unit
    where abs(dateDiff('day', before.period_end, addYears(periods.period_end, -1))) <= 7
    group by
        periods.cik,
        periods.metric,
        periods.period_kind,
        periods.period_start,
        periods.period_end,
        periods.unit
)

select
    periods.cik,
    periods.metric,
    periods.period_kind,
    periods.period_start,
    periods.period_end,
    periods.period_days,
    periods.unit,
    periods.taxonomy,
    periods.concept,
    periods.value_first_reported,
    periods.first_filed_date,
    periods.first_known_at,
    periods.value_latest,
    periods.latest_filed_date,
    periods.latest_known_at,
    cast(periods.value_latest != periods.value_first_reported as Bool) as is_restated,
    cast(periods.is_derived as Bool) as is_derived,
    year_ago.year_ago_period_end,
    year_ago.value_year_ago,
    if(
        year_ago.value_year_ago is null or year_ago.value_year_ago = 0,
        null,
        toFloat64(periods.value_latest - assumeNotNull(year_ago.value_year_ago))
        / abs(toFloat64(assumeNotNull(year_ago.value_year_ago)))
    ) as change_year_on_year,
    {{ mart_provenance('sec_xbrl_fact') }}
from periods
left join year_ago
    on
        periods.cik = year_ago.cik
        and periods.metric = year_ago.metric
        and periods.period_kind = year_ago.period_kind
        and periods.period_start = year_ago.period_start
        and periods.period_end = year_ago.period_end
        and periods.unit = year_ago.unit
