{#-
    One row for each item of a filing of the form 8-K (blueprint 7.6).

    A form 8-K reports events under numbered items: 2.02 is "results of operations", 5.02 a
    change of an officer or director. The submissions document lists the items of a filing, so
    the model covers the watchlist companies. The signal rules of a later phase read the item
    codes (blueprint 8.9). The forms of the 8-K family (8-K, 8-K/A, 8-K12B, ...) count.
-#}
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(cik, filing_date, accession_number, item_code)'
) }}

with items as (
    select
        accession_number,
        cik,
        form,
        filing_date,
        accepted_at,
        arrayJoin(arrayDistinct(items)) as item_code
    from {{ ref('fct_filing') }}
    where startsWith(form, '8-K')
)

select
    accession_number,
    cik,
    item_code,
    form,
    filing_date,
    accepted_at,
    {{ mart_provenance('sec_filing_detail') }}
from items
