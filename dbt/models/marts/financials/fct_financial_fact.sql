{#-
    The financial facts of the watchlist companies, bitemporal (blueprint 7.6 and 7.10).

    One row for each company, concept, unit, period (start and end) and filing. A fact has two
    times. The valid time is the period which the fact describes: period_start to period_end.
    The knowledge time is the moment at which the fact became public: known_at. A restatement is
    a new row with another accession number and a later known_at; no row is overwritten.

    known_at is the moment at which EDGAR made the filing public. known_at_source names the rule:

      acceptance_time     The submissions document of the company holds the acceptance time of
                          the filing, and the filing was accepted on its filing date: EDGAR
                          publishes a filing when it accepts the filing.
      dissemination_time  The filing was accepted after 17:30 New York time. Such a filing
                          receives the filing date of the next business day, and EDGAR publishes
                          it at 06:00 New York time on that date. known_at is that time: the
                          acceptance time would lie up to a night, or a weekend, too early.
      filing_date         No acceptance time is known: 05:00 UTC of the day after the filing
                          date (week 3 plan, difference D48).

    A knowledge time which is too early is the look-ahead error which the two times exist to
    prevent, so each rule gives the later of the possible moments. The second rule is change
    C23 of week 3: the first build on live data showed 156 such filings. The rule in one line:
    known_at is the later of the acceptance time and 06:00 New York time on the filing date.

    An instant fact (a balance at one moment) has period_start = period_end.
-#}
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(cik, taxonomy, concept, unit, period_end, period_start, accession_number)'
) }}

with acceptance as (
    select
        cik,
        accession_number,
        accepted_at
    from {{ ref('stg_sec__filing_detail') }}
    where accepted_at is not null
),

timed as (
    select
        facts.cik,
        facts.taxonomy,
        facts.concept,
        facts.unit,
        facts.period_start,
        facts.period_end,
        facts.accession_number,
        facts.is_instant,
        facts.value,
        facts.form,
        facts.filed_date,
        facts.fiscal_year,
        facts.fiscal_period,
        facts.frame,
        acceptance.accepted_at is not null as has_acceptance_time,
        -- A fact without an acceptance time never reads the value of this column.
        toDateTime(assumeNotNull(acceptance.accepted_at), 'UTC') as accepted_time,
        -- 06:00 in New York on the filing date, as a UTC time: 10:00 UTC in summer, 11:00 UTC
        -- in winter.
        toTimeZone(
            makeDateTime(
                toYear(facts.filed_date), toMonth(facts.filed_date), toDayOfMonth(facts.filed_date),
                6, 0, 0, 'America/New_York'
            ),
            'UTC'
        ) as published_from
    from {{ ref('stg_sec__xbrl_fact') }} as facts
    left join acceptance
        on
            facts.cik = acceptance.cik
            and facts.accession_number = acceptance.accession_number
)

select
    cik,
    taxonomy,
    concept,
    unit,
    period_start,
    period_end,
    accession_number,
    is_instant,
    value,
    form,
    filed_date,
    fiscal_year,
    fiscal_period,
    frame,
    multiIf(
        not has_acceptance_time, toDateTime(filed_date + 1, 'UTC') + toIntervalHour(5),
        accepted_time < published_from, published_from,
        accepted_time
    ) as known_at,
    multiIf(
        not has_acceptance_time, 'filing_date',
        accepted_time < published_from, 'dissemination_time',
        'acceptance_time'
    ) as known_at_source,
    {{ mart_provenance('sec_xbrl_fact') }}
from timed
