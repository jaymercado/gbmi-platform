{#-
    One row for each filing and filer (blueprint 7.6; use case UC-1).

    The grain is (accession_number, cik): one filing has one row for each company which files
    the document (week 3 plan, finding F11). The model holds every row of the daily index files
    and every row of the submissions documents which no loaded index file names. When a pair
    stands in two index files, the row of the earlier index date wins.

    The index gives the form, the filing date and the name of the filer for every company of
    EDGAR. The submissions document gives the detail (acceptance time, items of a form 8-K,
    main document) for the watchlist companies only. detail_source says which of the two the
    platform holds for the row.

    The addresses follow the page "Accessing EDGAR Data" of the SEC: the index page of a
    filing is <accession number>-index.html in the folder of the filer, and the documents of
    the filing stand in the folder named by the accession number without dashes.
-#}
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(cik, filing_date, accession_number)'
) }}

with index_rows as (
    select
        accession_number,
        cik,
        -- The names differ from the column names: ClickHouse reads a name inside an aggregate
        -- function as the alias of the same SELECT, and an aggregate inside an aggregate fails.
        min(index_date) as first_index_date,
        argMin(form, (index_date, line_number)) as first_form,
        argMin(company_name, (index_date, line_number)) as first_company_name,
        argMin(filing_date, (index_date, line_number)) as first_filing_date
    from {{ ref('stg_sec__filing_index') }}
    group by
        accession_number,
        cik
),

joined as (
    -- A side without a row gives the default value of each column: an empty accession number.
    select
        index_rows.accession_number != '' as in_index,
        detail.accession_number != '' as in_detail,
        index_rows.accession_number as index_accession_number,
        index_rows.cik as index_cik,
        index_rows.first_index_date as index_date,
        index_rows.first_form as index_form,
        index_rows.first_company_name as index_company_name,
        index_rows.first_filing_date as index_filing_date,
        detail.accession_number as detail_accession_number,
        detail.cik as detail_cik,
        detail.form as detail_form,
        detail.filing_date as detail_filing_date,
        detail.report_date,
        detail.accepted_at,
        detail.items,
        detail.primary_document,
        detail.primary_doc_description,
        detail.size_bytes,
        detail.is_xbrl,
        detail.is_inline_xbrl
    from index_rows
    full outer join {{ ref('stg_sec__filing_detail') }} as detail
        on
            index_rows.accession_number = detail.accession_number
            and index_rows.cik = detail.cik
)

select
    if(in_index, index_accession_number, detail_accession_number) as accession_number,
    if(in_index, index_cik, detail_cik) as cik,
    if(in_index, index_form, detail_form) as form,
    if(in_index, index_filing_date, detail_filing_date) as filing_date,
    if(in_index, index_date, null) as index_date,
    if(in_index, index_company_name, null) as filer_name,
    report_date,
    accepted_at,
    items,
    primary_document,
    primary_doc_description,
    size_bytes,
    is_xbrl,
    is_inline_xbrl,
    multiIf(
        in_index and in_detail, 'index_and_submissions',
        in_index, 'index_only',
        'submissions_only'
    ) as detail_source,
    concat(
        'https://www.sec.gov/Archives/edgar/data/', toString(cik), '/', accession_number, '-index.html'
    ) as filing_url,
    if(
        primary_document is null or primary_document = '',
        null,
        concat(
            'https://www.sec.gov/Archives/edgar/data/', toString(cik), '/',
            replaceAll(accession_number, '-', ''), '/', assumeNotNull(primary_document)
        )
    ) as primary_document_url,
    {{ mart_provenance('sec_filing_index') }}
from joined
