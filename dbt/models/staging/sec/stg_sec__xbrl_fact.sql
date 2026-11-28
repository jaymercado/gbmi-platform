{#-
    One row per financial fact of a watchlist company, as silver holds it: renamed and cast,
    nothing else. fiscal_year and fiscal_period describe the filing which reported the fact,
    not the period of the fact (week 3 plan, finding F6).
-#}
select
    cik,
    taxonomy,
    concept,
    unit,
    period_start,
    period_end,
    accession_number,
    is_instant,
    value,
    value_text,
    fiscal_year,
    fiscal_period,
    form,
    filed_date,
    frame,
    _source_file,
    _source_md5,
    _ingested_at,
    _run_id,
    _pipeline_version
from {{ source('lake', 'sec_xbrl_fact') }}
