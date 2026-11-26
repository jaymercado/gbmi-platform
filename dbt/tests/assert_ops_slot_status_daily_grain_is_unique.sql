-- The declared grain of ops_slot_status_daily holds: no combination of source, slot date and
-- feed appears twice. The test fails if this query returns a row.
select
    source,
    slot_date,
    feed,
    count() as occurrences
from {{ ref('ops_slot_status_daily') }}
group by
    source,
    slot_date,
    feed
having occurrences > 1
