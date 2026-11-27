# ADR-007 — Ledger state machine and the meaning of completeness

| | |
|---|---|
| **Date** | 2026-11-27 |
| **Status** | **Accepted** |
| **Sources** | Blueprint §5.1 (principle 4), §7.3, §8.2, §8.3; week 2 plan §3.3, WP-03, WP-04, WP-09 |
| **Related** | Gate G1 and gate G6 (blueprint §5.7); risk R22; benchmark report v1 (source profile) |

## Context

GDELT publishes one file for each of three feeds every 15 minutes: 288 files a day. One interval of 15 minutes is called a *slot*. The source has holes. The master file list of GDELT does not hold 1.77% of the slots since February 2015, and the missing slots come in blocks (benchmark report v1). A file can also be listed and not served, arrive late, or arrive damaged.

A pipeline which downloads "what the source lists today" cannot tell three cases apart: a file which is late, a file which will never come, and a file which the pipeline lost. No query can then say whether a day is complete, and a rerun cannot know what an earlier run finished.

Four points need a decision: where the knowledge about each expected file lives; which changes of state are allowed; when the platform stops waiting for a file; and what "complete" means for a day.

## Decision

**1. One row for each expected file.** The PostgreSQL table `ops.source_slot`, called the *ledger*, holds one row for each feed and slot. The rows come from a calendar, 96 slots for each UTC day and feed, and not from the file list of the source. A file which the source never lists is still expected. The calendar expects a slot up to 15 minutes ahead of the clock, because GDELT uploads a file about 10 minutes before the time in the file name (week 2 plan, finding F1).

**2. Seven states.**

| State | Meaning |
|---|---|
| `expected` | The calendar says: the file should exist |
| `listed` | The source lists the file. The address, the size and the MD5 hash are known |
| `downloaded` | The bytes arrived. Gate G1 has not judged the bytes yet |
| `verified` | Gate G1 passed (size, MD5 hash, readable zip), and the file is in bronze |
| `loaded` | The rows of the file are committed to silver |
| `quarantined` | The file failed gate G1 three times, or failed gate G2 as a whole file (more than 1% bad rows) |
| `confirmed_missing` | A rule or an operator decided: no file will come |

**3. Twelve transitions and no other.** A *transition* is one allowed change from one state to another state.

| From | To | When |
|---|---|---|
| `expected` | `listed` | The file list of the source names the file |
| `listed` | `downloaded` | The download ended |
| `downloaded` | `verified` | Size and MD5 hash match, and the zip opens |
| `downloaded` | `listed` | The check failed; the file is fetched again |
| `listed` | `quarantined` | Three attempts failed |
| `verified` | `loaded` | The silver commit is done |
| `verified` | `quarantined` | The file failed gate G2 as a whole |
| `quarantined` | `listed` | An operator sends the file back to the queue |
| `expected` | `confirmed_missing` | Rule 5a below, or an operator |
| `listed` | `confirmed_missing` | Rule 5b below, or an operator |
| `quarantined` | `confirmed_missing` | An operator only |
| `confirmed_missing` | `listed` | The file list brings news about the slot |

Two states end the life of a slot: `loaded` and `confirmed_missing`. No transition leaves `loaded`.

**4. Every transition is guarded and logged.** One SQL statement changes the state: an update which names the state the row must have (`UPDATE … WHERE state = <from>`), together with the insert of one row into the log table `ops.slot_transition`. When two processes try the same change, one process wins and the other process gets an error. The code holds the twelve pairs as one table (`gbmi.ledger.states`), and a check constraint of the log table holds the same twelve pairs. A successful download records two transitions in one transaction (`listed` to `downloaded` to `verified`), so no row rests in `downloaded`.

**5. Two rules stop the wait for a file.** The reconciliation job runs once in each UTC day from 08:00 and reads the master list for the trailing seven days.

- **Rule 5a, never listed.** An `expected` slot becomes `confirmed_missing` when two reconciliations did not find the slot in the list and the slot time is 48 hours old or older. A slot which is newer than the newest slot of the list does not count as absent.
- **Rule 5b, listed and not served.** A `listed` slot becomes `confirmed_missing` when the source has answered with HTTP 404 for 48 hours or longer.

Both rules write `decided_by = 'auto'` and a note. An operator can decide earlier with `gbmi ledger confirm-missing`; the command needs a range of days and a note. Only an operator confirms a quarantined slot. A confirmed slot goes back to `listed` in two cases: the slot was never listed and the list now holds the slot, or the list gives another size or MD5 hash than the ledger.

**6. Completeness.** A day is *complete* when every slot of the day is `loaded` or `confirmed_missing`. The completeness of a day is:

```text
completeness = loaded slots ÷ (expected slots − confirmed-missing slots)
```

The expected slots of one day are 96 for one feed and 288 for the three feeds. Completeness measures what the platform loaded of what the source could give. A slot which the source never published does not lower the number; the report shows the count of confirmed-missing slots beside the number. Two implementations compute the number from the ledger and have to agree: `Ledger.completeness` in the Python package and the dbt model `ops_slot_status_daily` in ClickHouse.

## Alternatives considered

| Alternative | Why the alternative lost |
|---|---|
| **Retries without state.** Each run downloads the newest files and tries a failed download a few times. No table of expected files exists | A file which fails beyond the retries leaves a hole which no later run sees. A run which never starts, for example on a laptop which slept, leaves the same hole. No query can separate "late" from "never coming", and a rerun cannot know what an earlier run finished |
| **Trust in the source list.** The master list defines what should exist | The list has holes and is not sorted: 7,291 of the 412,699 calendar slots of the Events feed are absent, and 1,167 lines stand out of time order (benchmark report v1). A slot which the list never names would be invisible, and completeness would be 100% by definition |
| **State in the orchestrator.** One Airflow task for each file, with the task state as the truth | The state would live in the metadata database of one Airflow installation: 288 task states a day, lost with a reset of the database, and not readable by a dbt model. The ledger is a plain PostgreSQL table which the warehouse reads |
| **A state column without a transition table** | Every code path could write every state. The rule "only the twelve transitions" could not be tested and could not be enforced by the database |

## Consequences

**Easier.**

- A rerun and a restart are safe. Each job asks the ledger what is pending and never guesses from file names.
- A backfill needs no separate code: an operator creates the rows of a range of days (`gbmi gdelt enqueue`), and the routine ingest job drains the rows. The 30-day backfill of WP-09 ran through the same code path as the scheduled job.
- One query shows the state of every file, and one model gives the completeness of every day.

**Harder.**

- Every new source needs a calendar and the same life cycle. Phase P2 adds `ops.source_object` for SEC files (blueprint §7.3).
- No shortcut exists for a wrong state. No transition leaves `loaded`: a loaded file with wrong rows is repaired by a rebuild of the unit (runbook `silver-build-failed-or-wrong-data.md`), not by a change of the state.

**Given up, and known limits.**

- The ledger knows the size and the MD5 hash of a file, not the meaning of the content.
- The rules look at the trailing seven days only (review of the code before the backfill, finding V3). A slot which becomes `verified` after the day build of a day older than seven days is never built by the routine jobs, and a file which the source does not serve is confirmed automatically only while the slot is younger than seven days. Both runbooks name the limit. The repair queue of gate G6 (phase P4) closes the gap.
- The check for overdue slots reports a slot which is not `verified` six hours after the slot time. During the drain of a backfill the check reports the files which wait for the download: six failed results in WP-09, between 20:18 and 20:28 UTC on 2026-11-26, while 13 GB were downloaded. The results are true and say nothing about the source.

## Evidence

| Claim | Test or measurement | Result |
|---|---|---|
| Only the twelve transitions succeed | Property test of the state table (`tests/unit/test_ledger_states.py`). Property test against PostgreSQL: 120 random sequences of up to 30 repository calls on three slots; each call succeeds exactly when the table allows the call, and the state of each slot equals the replay of the logged transitions (`tests/ledger/test_repository_properties.py`) | Pass (WP-03) |
| The code and the database hold the same twelve pairs | `tests/ledger/test_schema.py` | Pass (WP-03) |
| Two processes cannot both make the same change | Twenty slots, two connections, started together by a barrier (`tests/ledger/test_repository_properties.py`) | For each slot exactly one connection wins (WP-03) |
| The calendar gives 96 slots for every UTC day | `tests/unit/test_ledger_calendar.py`: eight chosen days, for example 2027-01-01, the leap day 2028-02-29 and the four days of 2026 and 2027 on which clocks change in the United States; a property test over ranges of days | Pass (WP-03) |
| Rule 5a and rule 5b confirm at the right moment and not before | Tests with a controlled clock (`tests/ledger/test_reconcile.py`): two reconciliations and 48 hours; one reconciliation is not enough; a slot which the list has not reached is not absent; a file not served for 48 hours | Pass (WP-04). The rules never fired on real data in week 2: the list held every slot of the 30 days |
| Coverage of the package `gbmi.ledger` | `make test`, at the end of WP-03 | 99% of 474 statements |
| The state machine carries a real backfill | WP-09, 2026-11-26: 8,640 slots of 30 days. The log table shows exactly four transitions for each slot, in the order `expected`, `listed`, `downloaded`, `verified`, `loaded`, and no other transition | 8,640 of 8,640 slots `loaded`; no slot needed a second attempt |
| Both implementations of completeness agree | The end-to-end test `tests/e2e/test_ops_models.py` on the recorded day; the evidence script and the dbt model on the 30 days (status report of phase P1) | 100% for each of the 30 days in both |

## Status

**Accepted.** The states, the transitions and the two rules are tested (WP-03, WP-04), and 30 real days ran through the ledger (WP-09). The rules of automatic confirmation are proven by tests with a controlled clock only; no real slot has needed a rule yet.
