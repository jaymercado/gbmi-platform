{#-
    One row per company of the universe and snapshot date, as silver holds it: renamed and
    cast, nothing else. The dates stay Date32, the type of an Iceberg date in ClickHouse: the
    open end of a validity interval (9999-12-31) does not fit the type Date.
-#}
select
    snapshot_date,
    cik,
    name,
    tickers,
    exchanges,
    primary_ticker,
    primary_exchange,
    sic,
    sic_description,
    state_of_incorporation,
    fiscal_year_end,
    category,
    entity_type,
    lei,
    attr_hash,
    attributes_as_of,
    is_stale,
    is_inactive,
    _source_file,
    _source_md5,
    _ingested_at,
    _run_id,
    _pipeline_version
from {{ source('lake', 'sec_company_snapshot') }}
