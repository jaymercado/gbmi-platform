-- Each company of dim_company has exactly one current row, and the current row comes from the
-- snapshots: a row from former names describes the time before the first snapshot.
-- The test fails if this query returns a row.
select
    cik,
    countIf(is_current) as current_rows,
    countIf(is_current and history_source != 'snapshot') as current_rows_from_former_names
from {{ ref('dim_company') }}
group by cik
having
    current_rows != 1
    or current_rows_from_former_names > 0
