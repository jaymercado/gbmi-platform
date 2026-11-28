{#-
    The financial facts of the watchlist companies, bitemporal (blueprint 7.6 and 7.10).

    One row for each company, concept, unit, period (start and end) and filing. A fact has two
    times. The valid time is the period which the fact describes: period_start to period_end.
    The knowledge time is the moment at which the fact became public: known_at. A restatement is
    a new row with another accession number and a later known_at; no row is overwritten.

    known_at is the acceptance time of the filing when the submissions document of the company
    holds the filing. Otherwise known_at is 05:00 UTC of the day after the filing date, and
    known_at_source says filing_date (week 3 plan, difference D48): midnight of the filing date
    lies before the filing was public, and a knowledge time which is too early is the
    look-ahead error which the two times exist to prevent.

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
)

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
    coalesce(
        toDateTime(acceptance.accepted_at, 'UTC'),
        toDateTime(facts.filed_date + 1, 'UTC') + toIntervalHour(5)
    ) as known_at,
    if(acceptance.accepted_at is null, 'filing_date', 'acceptance_time') as known_at_source,
    {{ mart_provenance('sec_xbrl_fact') }}
from {{ ref('stg_sec__xbrl_fact') }} as facts
left join acceptance
    on
        facts.cik = acceptance.cik
        and facts.accession_number = acceptance.accession_number
