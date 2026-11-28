{#-
    One row per filing and filer of one daily index file of EDGAR, as silver holds it: renamed
    and cast, nothing else. One filing has one row for each filer, so the key holds the CIK.
-#}
select
    index_date,
    accession_number,
    cik,
    form,
    company_name,
    filing_date,
    file_name,
    line_number,
    _source_file,
    _source_md5,
    _ingested_at,
    _run_id,
    _pipeline_version
from {{ source('lake', 'sec_filing_index') }}
