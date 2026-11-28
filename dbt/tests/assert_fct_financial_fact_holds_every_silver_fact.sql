-- fct_financial_fact holds exactly one row for each fact of the silver table: the join with the
-- acceptance times neither drops nor doubles a fact.
-- The test fails if this query returns a row.
with mart as (
    select count() as mart_rows
    from {{ ref('fct_financial_fact') }}
),

silver as (
    select count() as silver_rows
    from {{ ref('stg_sec__xbrl_fact') }}
)

select
    mart.mart_rows,
    silver.silver_rows
from mart
cross join silver
where mart.mart_rows != silver.silver_rows
