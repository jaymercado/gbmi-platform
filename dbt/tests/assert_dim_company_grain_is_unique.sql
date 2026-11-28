-- The declared grain of dim_company holds: no combination of company and start of an interval appears twice.
-- The test fails if this query returns a row.
select
    cik,
    valid_from,
    count() as occurrences
from {{ ref('dim_company') }}
group by
    cik,
    valid_from
having occurrences > 1
