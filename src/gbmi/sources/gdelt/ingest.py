"""Pipeline PL-1: every GDELT file lands unchanged, verified and tracked (blueprint 8.3).

One ingest run has the seven stages of the blueprint:

1. the calendar creates the expected rows up to 15 minutes ahead of the clock;
2. the index marks the slots which the source lists, with URL, size and MD5;
3. the listed slots are selected, oldest first, up to the batch limit;
4. to 6. each file is fetched, checked by gate G1 and written to its bronze key;
7. each outcome becomes a guarded change of state in the ledger.

A run is safe to stop anywhere. A file which is in bronze without its change of state is found
by the next run, which records the change without a second download.
"""

from __future__ import annotations

import threading
import time
from collections import Counter, deque
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from gbmi.core.gdelt import Feed, Slot
from gbmi.ledger.calendar import (
    LEAD,
    SLOT_LENGTH,
    first_slot_of,
    last_slot_of,
    newest_expected,
    slots_between,
)
from gbmi.ledger.repository import Ledger, Listing, SlotKey, SlotRecord, StaleStateError
from gbmi.ledger.states import State
from gbmi.logging import get_logger
from gbmi.quality.quarantine import write_quarantined_file
from gbmi.quality.results import FAILED, G1, PASSED, CheckResult, record_results
from gbmi.sources.gdelt.index import IndexLine, latest, read_last_update, read_master_list
from gbmi.sources.gdelt.land import (
    Bronze,
    Fetcher,
    FileUnavailableError,
    LandedFile,
    RateLimitedError,
    VerificationError,
    land_file,
)

log = get_logger(__name__)

#: A file which fails gate G1 this many times is quarantined (blueprint 8.3).
MAX_ATTEMPTS = 3
#: The pause after an HTTP 429 which names no waiting time (week 2 plan, WP-04).
RATE_LIMIT_PAUSE = 60.0
#: One run waits at most this long for the source, in seconds; the next run goes on.
RATE_LIMIT_BUDGET = 600.0
DEFAULT_BATCH_LIMIT = 200
DEFAULT_CONCURRENCY = 4

Sleep = Callable[[float], None]


@dataclass(frozen=True)
class _Outcome:
    """What one download attempt gave."""

    record: SlotRecord
    kind: str  # landed | rejected | unavailable | limited | deferred | error
    landed: LandedFile | None = None
    error: Exception | None = None


def listings(lines: Iterable[IndexLine]) -> list[Listing]:
    """The lines of a file list as the ledger takes them."""
    return [Listing(SlotKey(line.feed, line.slot), line.url, line.size, line.md5) for line in lines]


def _line(record: SlotRecord) -> IndexLine:
    listing = record.listing
    return IndexLine(
        size=listing.size,
        md5=listing.md5,
        url=listing.url,
        slot=record.key.slot,
        feed=record.key.feed,
    )


def _landing(
    key: SlotKey, status: str, size: int, detail: dict[str, Any] | None = None
) -> CheckResult:
    """The result of gate G1 for one file."""
    return CheckResult(
        gate=G1,
        check_name="landing",
        status=status,
        observed_value=size,
        threshold="size and MD5 of the index; the zip opens",
        feed=key.feed.value,
        unit=key.slot.stamp,
        detail=detail or {},
    )


def download_listed(
    records: Sequence[SlotRecord],
    *,
    ledger: Ledger,
    bronze: Bronze,
    fetcher: Fetcher,
    lake_uri: str,
    as_of: datetime,
    concurrency: int = DEFAULT_CONCURRENCY,
    sleep: Sleep = time.sleep,
) -> dict[str, int]:
    """Stages 4 to 7 for the given listed slots; returns the counts of the outcomes.

    The downloads run in ``concurrency`` threads; every change of the ledger happens in the
    calling thread. After the first HTTP 429 the run goes on with one download at a time and
    honours the header Retry-After (week 2 plan 3.3).
    """
    counts: Counter[str] = Counter()
    results: list[CheckResult] = []
    limited = threading.Event()
    later: list[SlotRecord] = []
    first_wait: list[float] = []

    def attempt(record: SlotRecord) -> _Outcome:
        try:
            return _Outcome(record, "landed", landed=land_file(bronze, fetcher, _line(record)))
        except VerificationError as error:
            return _Outcome(record, "rejected", error=error)
        except FileUnavailableError as error:
            return _Outcome(record, "unavailable", error=error)
        except RateLimitedError as error:
            return _Outcome(record, "limited", error=error)
        except Exception as error:  # one file must not stop the files beside it
            return _Outcome(record, "error", error=error)

    def attempt_unless_limited(record: SlotRecord) -> _Outcome:
        if limited.is_set():
            return _Outcome(record, "deferred")
        outcome = attempt(record)
        if outcome.kind == "limited":
            limited.set()
        return outcome

    def settle(outcome: _Outcome) -> SlotRecord | None:
        """Record one outcome; returns the slot when the file is to be fetched again at once."""
        record, key = outcome.record, outcome.record.key
        try:
            if outcome.landed is not None:
                landed = outcome.landed
                ledger.record_download(
                    key,
                    bronze_uri=bronze.uri(landed.key),
                    published_at=landed.published_at,
                    as_of=as_of,
                )
                counts["verified"] += 1
                counts["written" if landed.written else "already_in_bronze"] += 1
                counts["bytes"] += landed.size if landed.written else 0
                results.append(_landing(key, PASSED, landed.size))
            elif isinstance(outcome.error, VerificationError):
                reason = str(outcome.error)
                attempts = ledger.record_failed_attempt(key, error=reason, as_of=as_of)
                counts["rejected"] += 1
                results.append(
                    _landing(
                        key,
                        FAILED,
                        len(outcome.error.content),
                        {"error": reason, "attempt": attempts},
                    )
                )
                if attempts < MAX_ATTEMPTS:
                    return record
                kept = write_quarantined_file(
                    lake_uri,
                    ledger.source,
                    key.feed.value,
                    key.slot.slot_date,
                    key.slot.file_name(key.feed),
                    outcome.error.content,
                )
                ledger.quarantine(
                    key,
                    from_state=State.LISTED,
                    reason=f"gate G1 failed {attempts} times: {reason}",
                    as_of=as_of,
                )
                counts["quarantined"] += 1
                log.error("slot_quarantined", slot=key.label, reason=reason, kept=kept)
            elif isinstance(outcome.error, FileUnavailableError):
                ledger.record_unavailable(key, as_of=as_of, error=str(outcome.error))
                counts["unavailable"] += 1
            elif outcome.kind in ("limited", "deferred"):
                later.append(record)
                if isinstance(outcome.error, RateLimitedError) and outcome.error.retry_after:
                    first_wait.append(outcome.error.retry_after)
            else:
                counts["errors"] += 1
                log.warning("download_failed", slot=key.label, error=repr(outcome.error))
        except StaleStateError as error:
            # Somebody else moved the slot since the selection, for example an operator.
            counts["stale"] += 1
            log.warning("slot_changed_during_the_run", slot=key.label, error=str(error))
        return None

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        pending: set[Future[_Outcome]] = {
            pool.submit(attempt_unless_limited, record) for record in records
        }
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                again = settle(future.result())
                if again is not None:
                    pending.add(pool.submit(attempt_unless_limited, again))

    if later:
        counts["rate_limited"] = 1
        budget = RATE_LIMIT_BUDGET
        if first_wait:
            pause = min(max(first_wait), budget)
            budget -= pause
            sleep(pause)
        queue = deque(sorted(later, key=lambda record: record.key.slot))
        while queue:
            record = queue.popleft()
            outcome = attempt(record)
            if isinstance(outcome.error, RateLimitedError):
                pause = outcome.error.retry_after or RATE_LIMIT_PAUSE
                if pause > budget:
                    counts["left_for_the_next_run"] = len(queue) + 1
                    break
                budget -= pause
                sleep(pause)
                queue.appendleft(record)
                continue
            again = settle(outcome)
            if again is not None:
                queue.appendleft(again)

    record_results(ledger.connection, results, source=ledger.source, run_id=ledger.run_id)
    return dict(counts)


def run_ingest(
    *,
    as_of: datetime,
    fetcher: Fetcher,
    ledger: Ledger,
    bronze: Bronze,
    lake_uri: str,
    batch_limit: int = DEFAULT_BATCH_LIMIT,
    concurrency: int = DEFAULT_CONCURRENCY,
    sleep: Sleep = time.sleep,
) -> dict[str, Any]:
    """One ingest run; returns a summary of built-in types."""
    newest_before = ledger.newest_slot()
    listed_before = ledger.newest_slot(listed=True)
    target = newest_expected(as_of)
    # The run works as if the clock showed as_of: a line for a later slot waits for a later run.
    update = [line for line in read_last_update(fetcher) if line.slot <= target]
    newest_update = max((line.slot for line in update), default=None)

    # 1. Calendar. An empty ledger starts at the newest slot of lastupdate.txt.
    if newest_before is None:
        first = newest_update or target
    else:
        first = Slot(newest_before.start + SLOT_LENGTH)
    created = ledger.ensure_expected(
        [SlotKey(feed, slot) for slot in slots_between(first, target) for feed in Feed]
    )

    # 2. Index. One request of 320 bytes in a normal run.
    listed = ledger.mark_listed(listings(update), as_of=as_of)
    # The run follows a stop or a missed run when lastupdate.txt is more than one slot ahead of
    # the newest listed slot: the master list then names the files of the gap.
    after_a_gap = newest_before is not None and (
        listed_before is None
        or (newest_update is not None and newest_update.start - listed_before.start > LEAD)
    )
    if after_a_gap:
        if listed_before is not None:
            gap_start = Slot(listed_before.start + SLOT_LENGTH)
        else:
            # Nothing was ever listed: the gap starts at the oldest slot which still waits.
            waiting = ledger.find(states=[State.EXPECTED], limit=1)
            gap_start = waiting[0].key.slot if waiting else first
        lines = read_master_list(fetcher, oldest=gap_start, as_of=as_of)
        wanted = [line for line in latest(lines).values() if line.slot <= target]
        listed |= ledger.mark_listed(listings(wanted), as_of=as_of)

    # 3. Select, oldest first, up to the batch limit. 4 to 7: download and record.
    selected = ledger.pending_downloads(limit=batch_limit)
    counts = download_listed(
        selected,
        ledger=ledger,
        bronze=bronze,
        fetcher=fetcher,
        lake_uri=lake_uri,
        as_of=as_of,
        concurrency=concurrency,
        sleep=sleep,
    )
    summary: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "newest_slot": target.stamp,
        "expected_created": created,
        "listed": len(listed),
        "after_a_gap": after_a_gap,
        "selected": len(selected),
        "still_listed": len(ledger.pending_downloads(limit=batch_limit)),
        **{
            name: counts.get(name, 0)
            for name in (
                "verified",
                "written",
                "already_in_bronze",
                "bytes",
                "rejected",
                "quarantined",
                "unavailable",
                "errors",
                "stale",
                "rate_limited",
                "left_for_the_next_run",
            )
        },
    }
    log.info("ingest_run", **summary)
    return summary


def enqueue(
    first_day: date,
    last_day: date,
    *,
    as_of: datetime,
    fetcher: Fetcher,
    ledger: Ledger,
    reserve: str | None = None,
) -> dict[str, Any]:
    """Create the calendar rows of a range of UTC days and list every file the master list holds.

    The ingest run drains the rows afterwards (blueprint 8.3, backfill). ``reserve`` puts a claim
    on the new rows, so the routine silver jobs leave the range to a bulk job.
    """
    if first_day > last_day:
        raise ValueError(f"the range ends before it starts: {first_day} to {last_day}")
    first = first_slot_of(first_day)
    # The calendar never runs ahead of the clock by more than its lead.
    last = min(last_slot_of(last_day), newest_expected(as_of))
    keys = [SlotKey(feed, slot) for slot in slots_between(first, last) for feed in Feed]
    created = ledger.ensure_expected(keys, reserve=reserve)
    lines = read_master_list(fetcher, oldest=first, as_of=as_of) if keys else []
    wanted = [line for line in latest(lines).values() if first <= line.slot <= last]
    listed = ledger.mark_listed(listings(wanted), as_of=as_of)
    summary = {
        "from": first_day.isoformat(),
        "to": last_day.isoformat(),
        "slots": len(keys),
        "created": created,
        "in_the_master_list": len(wanted),
        "listed": len(listed),
        "reserved_for": reserve,
    }
    log.info("enqueue", **summary)
    return summary
