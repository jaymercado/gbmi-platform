"""The daily reconciliation of the ledger against the master list (blueprint 8.2 and 8.3).

``lastupdate.txt`` shows only the newest slot. The reconciliation reads the master list for the
trailing seven days and settles what the routine run cannot see:

* an expected slot which the list holds is listed;
* an expected slot which the list does not hold is counted absent, and confirmed missing after
  two reconciliations and 48 hours;
* a listed file which the source does not serve for 48 hours is confirmed missing;
* a confirmed slot is reopened when the list brings news about the slot;
* bronze is compared with the ledger, and each difference is reported.

The reconciliation is due once in each UTC day from 08:00. It is due, not scheduled, so a
laptop or a host which was off in the morning still reconciles (week 2 plan, D18).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from gbmi.core.gdelt import Feed
from gbmi.ledger.calendar import days_between, first_slot_of, newest_expected, slot_at
from gbmi.ledger.db import Connection
from gbmi.ledger.operator import AUTO
from gbmi.ledger.repository import TRAILING_DAYS, Ledger, Listing, SlotKey, SlotRecord
from gbmi.ledger.states import State
from gbmi.logging import get_logger
from gbmi.quality.results import FAILED, G1, G6, PASSED, CheckResult, record_results
from gbmi.sources.gdelt.index import latest, read_master_list
from gbmi.sources.gdelt.land import Bronze, Fetcher

log = get_logger(__name__)

#: A slot is confirmed missing when the reason has lasted this long (blueprint 8.2).
MISSING_AFTER = timedelta(hours=48)
#: The reconciliations in a row which have to miss an expected slot before the rule confirms.
ABSENT_RECONCILIATIONS = 2
#: The reconciliation is due once in each UTC day from this hour.
DUE_FROM_HOUR = 8
#: A report lists at most this many slots; the count is always complete.
REPORT_LIMIT = 50


def entry_point(ledger: Ledger) -> str:
    return f"{ledger.source}.reconcile"


def is_due(connection: Connection, name: str, as_of: datetime) -> bool:
    """True from 08:00 UTC on, until a reconciliation of the same UTC day has succeeded."""
    moment = as_of.astimezone(UTC)
    if moment.hour < DUE_FROM_HOUR:
        return False
    midnight = moment.replace(hour=0, minute=0, second=0, microsecond=0)
    done = connection.execute(
        "SELECT 1 FROM ops.pipeline_run WHERE entry_point = %s AND status = 'success' "
        "AND as_of >= %s AND as_of < %s AND detail ->> 'reconciled' = 'true' LIMIT 1",
        (name, midnight + timedelta(hours=DUE_FROM_HOUR), midnight + timedelta(days=1)),
    ).fetchone()
    return done is None


def compare_bronze(
    bronze: Bronze, records: list[SlotRecord], *, first_day: date, last_day: date
) -> list[dict[str, Any]]:
    """Each verified or loaded slot whose bronze object is missing or has another size."""
    present: dict[str, int] = {}
    for feed in Feed:
        for day in days_between(first_day, last_day):
            # The directory of a day is the directory of the bronze key of its first slot.
            present.update(bronze.sizes(first_slot_of(day).bronze_key(feed).rsplit("/", 1)[0]))
    differences: list[dict[str, Any]] = []
    for record in records:
        key = record.key.slot.bronze_key(record.key.feed)
        if record.state not in (State.VERIFIED, State.LOADED):
            continue
        if key not in present:
            differences.append({"slot": record.key.label, "difference": "missing in bronze"})
        elif present[key] != record.expected_bytes:
            differences.append(
                {
                    "slot": record.key.label,
                    "difference": "size differs",
                    "bronze_bytes": present[key],
                    "ledger_bytes": record.expected_bytes,
                }
            )
    return differences


def reconcile(
    *,
    as_of: datetime,
    fetcher: Fetcher,
    ledger: Ledger,
    bronze: Bronze,
    days: int = TRAILING_DAYS,
) -> dict[str, Any]:
    """Reconcile the trailing ``days`` against the master list.

    Returns a summary of built-in types; the key ``reconciled`` marks a real reconciliation.
    """
    first = slot_at(as_of - timedelta(days=days))
    last = newest_expected(as_of)
    in_list = {
        SlotKey(feed, slot): Listing(SlotKey(feed, slot), line.url, line.size, line.md5)
        for (feed, slot), line in latest(
            read_master_list(fetcher, oldest=first, as_of=as_of)
        ).items()
        if slot <= last
    }
    newest_in_list = max((key.slot for key in in_list), default=None)
    records = ledger.find(first=first, last=last)
    by_state: dict[State, list[SlotRecord]] = {state: [] for state in State}
    for record in records:
        by_state[record.state].append(record)

    # Expected slots: listed when the list holds them, counted absent when it does not. A slot
    # newer than the newest slot of the list is not absent; the list has not reached the slot.
    found = [in_list[record.key] for record in by_state[State.EXPECTED] if record.key in in_list]
    listed = ledger.mark_listed(found, as_of=as_of)
    absent = [
        record.key
        for record in by_state[State.EXPECTED]
        if record.key not in in_list
        and newest_in_list is not None
        and record.key.slot <= newest_in_list
    ]
    ledger.count_absent(absent)

    # A listed slot which is not verified yet takes a changed size or MD5 from the list.
    changed = ledger.update_listing(
        [in_list[record.key] for record in by_state[State.LISTED] if record.key in in_list]
    )

    # A confirmed slot is reopened in two cases only: the slot was confirmed without ever being
    # listed and the list holds the slot now; or the list gives another size or MD5 than the
    # ledger. A slot which was confirmed from listed or quarantined stands in the list already.
    news = [
        in_list[record.key]
        for record in by_state[State.CONFIRMED_MISSING]
        if record.key in in_list
        and (
            record.url is None
            or (record.expected_bytes, record.expected_md5)
            != (in_list[record.key].size, in_list[record.key].md5)
        )
    ]
    reopened = ledger.reopen(news, reason="the master list brings news about the slot", as_of=as_of)

    # Automatic confirmation (blueprint 8.2), decided by the rule and written as 'auto'.
    cutoff = as_of - MISSING_AFTER
    never_listed = [
        record.key
        for record in ledger.find(first=first, last=slot_at(cutoff), states=[State.EXPECTED])
        if record.absent_count >= ABSENT_RECONCILIATIONS
    ]
    confirmed_absent = ledger.confirm_missing(
        never_listed,
        from_state=State.EXPECTED,
        decided_by=AUTO,
        note=f"absent from the master list in {ABSENT_RECONCILIATIONS} reconciliations, "
        "and 48 hours old",
        as_of=as_of,
    )
    not_served = [
        record.key
        for record in ledger.find(first=first, last=last, states=[State.LISTED])
        if record.unavailable_since is not None and record.unavailable_since <= cutoff
    ]
    confirmed_unavailable = ledger.confirm_missing(
        not_served,
        from_state=State.LISTED,
        decided_by=AUTO,
        note="the source lists the file and has not served the file for 48 hours",
        as_of=as_of,
    )

    differences = compare_bronze(
        bronze,
        ledger.find(first=first, last=last, states=[State.VERIFIED, State.LOADED]),
        first_day=first.slot_date,
        last_day=last.slot_date,
    )
    record_results(
        ledger.connection,
        [
            CheckResult(
                gate=G6,
                check_name="ledger_against_bronze",
                status=FAILED if differences else PASSED,
                observed_value=len(differences),
                threshold="0 differences",
                unit=f"{first.slot_date.isoformat()}..{last.slot_date.isoformat()}",
                detail={"differences": differences[:REPORT_LIMIT]},
            )
        ],
        source=ledger.source,
        run_id=ledger.run_id,
    )
    summary: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "reconciled": True,
        "from": first.stamp,
        "to": last.stamp,
        "files_in_the_master_list": len(in_list),
        "listed": len(listed),
        "absent": len(absent),
        "listing_changed": len(changed),
        "reopened": len(reopened),
        "confirmed_missing_absent": len(confirmed_absent),
        "confirmed_missing_unavailable": len(confirmed_unavailable),
        "bronze_differences": len(differences),
        "bronze_difference_examples": differences[:REPORT_LIMIT],
    }
    log.info("reconciliation", **{k: v for k, v in summary.items() if not k.endswith("examples")})
    return summary


def report_overdue(*, as_of: datetime, ledger: Ledger) -> dict[str, Any]:
    """Report every slot which is not verified six hours after the slot time (blueprint 8.2)."""
    overdue = ledger.overdue(as_of)
    labels = [record.key.label for record in overdue]
    record_results(
        ledger.connection,
        [
            CheckResult(
                gate=G1,
                check_name="overdue_slots",
                status=FAILED if overdue else PASSED,
                observed_value=len(overdue),
                threshold="0 slots unverified six hours after the slot time",
                unit=slot_at(as_of).stamp,
                detail={"slots": labels[:REPORT_LIMIT]},
            )
        ],
        source=ledger.source,
        run_id=ledger.run_id,
    )
    return {"as_of": as_of.isoformat(), "overdue": len(overdue), "slots": labels[:REPORT_LIMIT]}
