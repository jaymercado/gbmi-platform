{#-
    The ledger as ClickHouse reads it: one row per expected file of a source (blueprint 7.3).
    The view holds no rows of its own; each query reads the operational database at that moment,
    so a model which is built from the view sees the current state of every slot.
-#}
select
    source,
    feed,
    slot_ts,
    toDate(slot_ts, 'UTC') as slot_date,
    state,
    expected_bytes,
    attempts,
    listed_at,
    published_at,
    verified_at,
    loaded_at,
    silver_snapshot_id,
    raw_lines,
    parsed_rows,
    quarantined_rows,
    decided_by,
    reserved_for,
    updated_at
from {{ ops_table('source_slot') }}
