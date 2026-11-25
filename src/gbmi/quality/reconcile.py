"""Count reconciliation between the layers of one run (gate G6 in miniature, blueprint 5.7).

After the mart build, the tracer checks that nothing was lost or doubled on the way:

    rows committed to silver for the unit      (the build's own accounting)
  = rows of the unit in the Iceberg snapshot   (read with PyIceberg)
    rows of the month in the Iceberg snapshot
  = rows of the month in the pinned lake table (read by ClickHouse)
  = events of the month in the mart            (sum of the aggregated measure)

and that the mart partition records the snapshot it was built from.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from gbmi.config import Settings, get_settings
from gbmi.core.gdelt import parse_hour
from gbmi.lake.catalog import load_lake_catalog
from gbmi.lake.commit import Unit
from gbmi.lake.tables import GDELT_EVENT
from gbmi.warehouse.client import connect


class ReconciliationError(Exception):
    """The layers disagree; the run must not be reported as a success."""


def assert_tracer_counts(
    hour: str, committed: int, snapshot_id: int, settings: Settings | None = None
) -> dict[str, Any]:
    """Task entry point: compare silver, the pinned lake table and the mart for one hour."""
    settings = settings or get_settings()
    slot_date, slot_hour = parse_hour(hour)
    month = slot_date.year * 100 + slot_date.month
    first_day = slot_date.replace(day=1).isoformat()
    month_filter = f"slot_date >= '{first_day}' and slot_date < '{_next_month(slot_date)}'"

    table = load_lake_catalog(settings).load_table((settings.silver_namespace, GDELT_EVENT))
    silver_unit = (
        table.scan(
            row_filter=Unit(slot_date, slot_hour).row_filter,
            snapshot_id=snapshot_id,
            selected_fields=("slot_date",),
        )
        .to_arrow()
        .num_rows
    )
    silver_month = (
        table.scan(row_filter=month_filter, snapshot_id=snapshot_id, selected_fields=("slot_date",))
        .to_arrow()
        .num_rows
    )

    client = connect(settings=settings)
    try:
        lake_month = int(
            client.query(
                "SELECT count() FROM lake.gdelt_event WHERE toYYYYMM(slot_date) = %(month)s",
                parameters={"month": month},
            ).result_rows[0][0]
        )
        mart_events, mart_rows, mart_snapshots = client.query(
            "SELECT sum(events), count(), groupUniqArray(_silver_snapshot_id) "
            "FROM marts.fct_gdelt_event_daily WHERE toYYYYMM(date) = %(month)s",
            parameters={"month": month},
        ).result_rows[0]
    finally:
        client.close()

    result = {
        "hour": hour,
        "month": month,
        "committed": committed,
        "silver_unit_rows": silver_unit,
        "silver_month_rows": silver_month,
        "lake_month_rows": lake_month,
        "mart_events": int(mart_events),
        "mart_rows": int(mart_rows),
        "mart_snapshot_ids": [int(value) for value in mart_snapshots],
        "snapshot_id": snapshot_id,
    }
    problems: list[str] = []
    if silver_unit != committed:
        problems.append(
            f"silver holds {silver_unit} rows for the unit, the build committed {committed}"
        )
    if lake_month != silver_month:
        problems.append(
            f"ClickHouse reads {lake_month} rows for the month, the snapshot holds {silver_month}"
        )
    if int(mart_events) != silver_month:
        problems.append(
            f"the mart counts {int(mart_events)} events for the month, silver holds {silver_month}"
        )
    if result["mart_snapshot_ids"] != [snapshot_id]:
        problems.append(
            f"the mart partition was built from snapshots {result['mart_snapshot_ids']}, "
            f"but {snapshot_id} was pinned"
        )
    if problems:
        raise ReconciliationError(f"{hour}: " + "; ".join(problems))
    return result


def _next_month(day: date) -> str:
    year, month = (day.year + 1, 1) if day.month == 12 else (day.year, day.month + 1)
    return f"{year:04d}-{month:02d}-01"
