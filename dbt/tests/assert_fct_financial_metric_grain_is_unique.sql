-- The declared grain of fct_financial_metric holds: no combination of company, metric, kind of
-- period, period and unit appears twice.
-- The test fails if this query returns a row.
select
    cik,
    metric,
    period_kind,
    period_start,
    period_end,
    unit,
    count() as occurrences
from {{ ref('fct_financial_metric') }}
group by
    cik,
    metric,
    period_kind,
    period_start,
    period_end,
    unit
having occurrences > 1
