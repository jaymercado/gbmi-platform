{#-
    One row per filing of a watchlist company, from the submissions document, as silver holds
    it: renamed and cast, nothing else.
-#}
select
    cik,
    accession_number,
    form,
    filing_date,
    report_date,
    acceptance_ts as accepted_at,
    act,
    file_number,
    film_number,
    items,
    core_type,
    size_bytes,
    is_xbrl,
    is_inline_xbrl,
    is_xbrl_numeric,
    primary_document,
    primary_doc_description,
    _source_file,
    _source_md5,
    _ingested_at,
    _run_id,
    _pipeline_version
from {{ source('lake', 'sec_filing_detail') }}
