{#-
    The financial facts as standard metrics (blueprint 8.7; week 3 plan 3.3).

    The seed metric_concept_map lists, for each of the seven metrics, the candidate concepts in
    the order of their priority. For each company, metric, unit and period, the model keeps the
    first concept of the seed which has a value, with two values of the concept: the value of
    the first filing which reported the period, and the newest known value. A restated period
    shows two different values.

    The unit is part of the key: a company which changes its reporting currency restates old
    periods in the new currency, and two values in two currencies are never compared.

    The kind of a period comes from its two bounds, never from the fiscal year or the fiscal
    period of the filing (week 3 plan, finding F6). The length counts both bounds:

        instant       a balance at one moment (a stock metric)
        quarter       80 to 100 days        half_year  170 to 190 days
        nine_months   260 to 290 days       year       350 to 380 days
        other         every other length, and a fact whose shape does not fit the metric
                      (a flow at one moment, a stock over a period)

    The variable facts_as_of builds the model as it was known at a past time (macro
    facts_as_of). Without the variable the model holds everything which is known now.
-#}
with known as (
    {{ facts_as_of(facts_as_of_time()) }}
),

candidates as (
    select
        known.cik,
        concept_map.metric,
        concept_map.flow_or_stock,
        concept_map.priority,
        known.taxonomy,
        known.concept,
        known.unit,
        known.period_start,
        known.period_end,
        known.is_instant,
        known.value_first_reported,
        known.first_accession_number,
        known.first_filed_date,
        known.first_known_at,
        known.value_known as value_latest,
        known.known_accession_number as latest_accession_number,
        known.known_filed_date as latest_filed_date,
        known.known_since as latest_known_at,
        known.reports
    from known
    inner join {{ ref('metric_concept_map') }} as concept_map
        on
            known.taxonomy = concept_map.taxonomy
            and known.concept = concept_map.concept
),

ranked as (
    select
        *,
        row_number() over (
            partition by cik, metric, unit, period_start, period_end
            order by priority, taxonomy, concept
        ) as choice
    from candidates
)

select
    cik,
    metric,
    flow_or_stock,
    unit,
    period_start,
    period_end,
    toInt32(dateDiff('day', period_start, period_end) + 1) as period_days,
    multiIf(
        flow_or_stock = 'stock' and is_instant, 'instant',
        flow_or_stock != 'flow' or is_instant, 'other',
        period_days between 80 and 100, 'quarter',
        period_days between 170 and 190, 'half_year',
        period_days between 260 and 290, 'nine_months',
        period_days between 350 and 380, 'year',
        'other'
    ) as period_kind,
    taxonomy,
    concept,
    value_first_reported,
    first_accession_number,
    first_filed_date,
    first_known_at,
    value_latest,
    latest_accession_number,
    latest_filed_date,
    latest_known_at,
    reports
from ranked
where choice = 1
