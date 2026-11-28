-- The declared grain of fct_filing_item holds: no combination of accession number, filer and item appears twice.
-- The test fails if this query returns a row.
select
    accession_number,
    cik,
    item_code,
    count() as occurrences
from {{ ref('fct_filing_item') }}
group by
    accession_number,
    cik,
    item_code
having occurrences > 1
