-- The counts of a row are consistent: the states add up to the slots of the ledger, the ledger
-- holds no more slots than the calendar, completeness lies between 0 and 1, and the
-- accounting of the loaded files holds (raw lines = parsed rows + quarantined rows).
-- The test fails if this query returns a row.
select
    source,
    slot_date,
    feed,
    calendar_slots,
    ledger_slots,
    completeness
from {{ ref('ops_slot_status_daily') }}
where
    ledger_slots != expected_slots + listed_slots + downloaded_slots + verified_slots
    + loaded_slots + quarantined_slots + confirmed_missing_slots
    or ledger_slots > calendar_slots
    or completeness < 0
    or completeness > 1
    or raw_lines != parsed_rows + quarantined_rows
