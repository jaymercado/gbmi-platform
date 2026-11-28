-- dim_company covers the universe: every company of the snapshots has a current row, and every
-- current row belongs to a company of the snapshots.
-- The test fails if this query returns a row.
with snapshot_companies as (
    select distinct cik
    from {{ ref('stg_sec__company_snapshot') }}
),

current_companies as (
    select cik
    from {{ ref('dim_company') }}
    where is_current
)

select
    snapshot_companies.cik,
    'a company of the snapshots without a current row' as problem
from snapshot_companies
where snapshot_companies.cik not in (select current_companies.cik from current_companies)

union all

select
    current_companies.cik,
    'a current row without a company of the snapshots' as problem
from current_companies
where current_companies.cik not in (select snapshot_companies.cik from snapshot_companies)
