-- The declared grain of fct_financial_fact holds: no combination of company, concept, unit,
-- period and filing appears twice.
-- The test fails if this query returns a row.
select
    cik,
    taxonomy,
    concept,
    unit,
    period_start,
    period_end,
    accession_number,
    count() as occurrences
from {{ ref('fct_financial_fact') }}
group by
    cik,
    taxonomy,
    concept,
    unit,
    period_start,
    period_end,
    accession_number
having occurrences > 1
