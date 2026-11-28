{#-
    One row per former name of a company, as silver holds it: renamed and cast, nothing else.
    The two dates are the dates which the SEC gives for the name (first and last use).
-#}
select
    cik,
    former_name,
    valid_from as name_from,
    valid_to as name_to,
    _source_file,
    _source_md5,
    _ingested_at,
    _run_id,
    _pipeline_version
from {{ source('lake', 'sec_company_former_name') }}
