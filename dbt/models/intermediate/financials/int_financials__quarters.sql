{#-
    The quarter values of the flow metrics (blueprint 8.7; week 3 plan 3.3).

    A quarter value is the three-month fact where a filing reports one. An annual report gives
    the year and no fourth quarter, so a missing quarter is derived in one way only:

        quarter = year - nine months

    for a year and a period of nine months of the same company, metric, unit and concept which
    start on the same day. The derived quarter starts on the day after the nine months and ends
    with the year, and is_derived says so. No other derivation exists: a half year is never
    split, and a quarter is never the difference of two other periods. A quarter is derived
    only when no reported quarter of the company, metric and unit ends on the same day.

    The first value of a derived quarter uses the first reported values of the year and of the
    nine months, and the latest value uses the newest values. The derived value is known from
    the moment at which both parts are known.
-#}
with metric_facts as (
    select *
    from {{ ref('int_financials__metric_facts') }}
    where flow_or_stock = 'flow'
),

reported as (
    select
        cik,
        metric,
        unit,
        period_start,
        period_end,
        period_days,
        taxonomy,
        concept,
        value_first_reported,
        first_filed_date,
        first_known_at,
        value_latest,
        latest_filed_date,
        latest_known_at,
        false as is_derived
    from metric_facts
    where period_kind = 'quarter'
),

candidates as (
    select
        years.cik,
        years.metric,
        years.unit,
        nine_months.period_end + 1 as period_start,
        years.period_end,
        toInt32(dateDiff('day', nine_months.period_end, years.period_end)) as period_days,
        years.taxonomy,
        years.concept,
        years.value_first_reported - nine_months.value_first_reported as value_first_reported,
        greatest(years.first_filed_date, nine_months.first_filed_date) as first_filed_date,
        greatest(years.first_known_at, nine_months.first_known_at) as first_known_at,
        years.value_latest - nine_months.value_latest as value_latest,
        greatest(years.latest_filed_date, nine_months.latest_filed_date) as latest_filed_date,
        greatest(years.latest_known_at, nine_months.latest_known_at) as latest_known_at,
        -- One year can stand beside two periods of nine months: the later one wins.
        row_number() over (
            partition by years.cik, years.metric, years.unit, years.period_start, years.period_end
            order by nine_months.period_end desc
        ) as choice
    from metric_facts as years
    inner join metric_facts as nine_months
        on
            years.cik = nine_months.cik
            and years.metric = nine_months.metric
            and years.unit = nine_months.unit
            and years.taxonomy = nine_months.taxonomy
            and years.concept = nine_months.concept
            and years.period_start = nine_months.period_start
    where
        years.period_kind = 'year'
        and nine_months.period_kind = 'nine_months'
        and dateDiff('day', nine_months.period_end, years.period_end) between 80 and 100
),

derived as (
    select
        cik,
        metric,
        unit,
        period_start,
        period_end,
        period_days,
        taxonomy,
        concept,
        value_first_reported,
        first_filed_date,
        first_known_at,
        value_latest,
        latest_filed_date,
        latest_known_at,
        true as is_derived
    from candidates
    where
        choice = 1
        and (cik, metric, unit, period_end) not in (
            select
                reported.cik,
                reported.metric,
                reported.unit,
                reported.period_end
            from reported
        )
)

select * from reported

union all

select * from derived
