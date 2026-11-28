-- fct_filing holds every pair of filing and filer of the index files and of the submissions
-- documents, and no other pair.
-- The test fails if this query returns a row.
with expected as (
    select
        accession_number,
        cik
    from {{ ref('stg_sec__filing_index') }}

    union distinct

    select
        accession_number,
        cik
    from {{ ref('stg_sec__filing_detail') }}
),

built as (
    select
        accession_number,
        cik
    from {{ ref('fct_filing') }}
)

select
    expected.accession_number,
    expected.cik,
    'a filing of silver which the mart lacks' as problem
from expected
where
    (expected.accession_number, expected.cik) not in (
        select
            built.accession_number,
            built.cik
        from built
    )

union all

select
    built.accession_number,
    built.cik,
    'a filing of the mart which silver lacks' as problem
from built
where
    (built.accession_number, built.cik) not in (
        select
            expected.accession_number,
            expected.cik
        from expected
    )
