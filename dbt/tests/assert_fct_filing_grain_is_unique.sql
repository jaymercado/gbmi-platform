-- The declared grain of fct_filing holds: no combination of accession number and filer appears twice.
-- The test fails if this query returns a row.
select
    accession_number,
    cik,
    count() as occurrences
from {{ ref('fct_filing') }}
group by
    accession_number,
    cik
having occurrences > 1
