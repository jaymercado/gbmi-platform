# Runbook: a GDELT slot is quarantined or overdue

| | |
|---|---|
| **Pipeline** | PL-1, the DAG `gdelt_ingest` (blueprint §8.2 and §8.3) |
| **Written** | 2026-11-26, phase P1, work package WP-11 |
| **Proved by** | Drill DR4 (`make drill DR=4`) and the tests of `tests/integration/silver` |

A **slot** is one expected file of GDELT: one feed (`export`, `mentions` or `gkg`) and one 15-minute interval. The **ledger** is the table `ops.source_slot` in the operational database; each slot is one row with a state. Every command of the runbook runs in the repository folder. `make toolbox CMD="…"` runs a `gbmi` command inside the stack, and `make psql SQL="…"` runs one SQL statement on the ledger. Both commands act on the working stack; add `STACK=gbmi-demo` for the demo stack.

## Symptom

One of the following is true.

- A log line of level error names a slot: `slot_quarantined` or `quality_check_failed`.
- The task `report_overdue` of `gdelt_ingest` reports slots: a slot is **overdue** when the slot is not verified six hours after the time of the slot.
- A day stays below 100% in `ops.ops_slot_status_daily`, and the day has slots in the state `quarantined`, `listed` or `expected`.

## Detection

```bash
# The number of slots in each state, for each feed and day of a range.
make toolbox CMD="gbmi ledger status --from 2026-11-20 --to 2026-11-26"

# The slots which are not verified six hours after their time.
make toolbox CMD="gbmi ledger overdue"

# The slots which need a decision, with the reason.
make psql SQL="SELECT feed, slot_ts, state, attempts, absent_count, unavailable_since, last_error FROM ops.source_slot WHERE source = 'gdelt' AND state IN ('quarantined', 'listed', 'expected') AND slot_ts < now() - interval '6 hours' ORDER BY slot_ts, feed"

# The failed checks of the last two days.
make psql SQL="SELECT created_at, gate, check_name, feed, unit, detail FROM ops.dq_result WHERE status = 'failed' AND created_at > now() - interval '2 days' ORDER BY created_at"
```

The history of one slot, oldest change first:

```bash
make psql SQL="SELECT created_at, from_state, to_state, reason, decided_by FROM ops.slot_transition WHERE source = 'gdelt' AND feed = 'export' AND slot_ts = '2026-11-25 08:30+00' ORDER BY transition_id"
```

## Diagnosis

Read the state and the column `last_error` of the slot, then find the row of the table.

| State | What the ledger says | Cause | Way out |
|---|---|---|---|
| `quarantined` | `last_error` starts with "gate G1 failed 3 times" | Three downloads in a row gave bytes whose size or MD5 differs from the file list of GDELT. The bad bytes of the last download lie under `quarantine/gdelt/<feed>/dt=<day>/<file name>` in the lake | Recovery A: requeue |
| `quarantined` | `last_error` says "the file in bronze has the MD5 …" or "the file is not in bronze" | The silver build found the bronze copy of a verified file changed or missing. A copy of a changed file lies in the quarantine area | Recovery A: requeue. The ingest downloads the file again and replaces the bronze copy |
| `quarantined` | `last_error` says "… rows failed gate G2, more than 1% of the file" | The file is damaged at the source: more than 1% of the rows fail the parse gate. The whole file lies in the quarantine area and gave no row to silver | Recovery C: confirm the slot as missing. A requeue does not help: the ingest lands the same bytes again, and the next build quarantines the slot again |
| `listed` | `unavailable_since` holds a time | GDELT lists the file and answers HTTP 404 for the file | Recovery B: wait for the automatic confirmation, or confirm earlier |
| `listed` | `attempts` grows, `unavailable_since` is empty | Downloads fail for another reason; `last_error` names the reason, for example a network error | Read `last_error`. The next ingest run tries again by itself. A source which refuses requests (HTTP 429) slows the run down and needs no action |
| `expected` | `absent_count` is 1 or more | GDELT does not list the file. GDELT has gaps: single slots, and sometimes hours | Recovery B: wait for the automatic confirmation, or confirm earlier |
| `expected` | `absent_count` is 0 and the slot is some hours old | No reconciliation has run since the slot was created. The reconciliation runs once in each UTC day, from 08:00 | Run the reconciliation: `make toolbox CMD="gbmi gdelt reconcile --force"` |

A slot in the state `verified` which is not loaded belongs to the other runbook: [a silver build failed](silver-build-failed-or-wrong-data.md).

## Recovery steps

### A. Requeue: download the file again

`gbmi ledger requeue` moves every quarantined slot of a range of days back to `listed`. The range and a note are required; `--feed` limits the command to one feed.

```bash
make toolbox CMD="gbmi ledger requeue --from 2026-11-25 --to 2026-11-25 --feed export --decided-by jay --note 'G1 failed three times; the file list is unchanged, so the download is repeated'"
```

The next run of `gdelt_ingest` downloads the file. To run the ingest at once:

```bash
make toolbox CMD="gbmi gdelt ingest"
```

When the same day also holds a slot which the 1% rule quarantined, the requeue also moves the slot of the 1% rule. The next silver build quarantines the slot of the 1% rule again; then use recovery C for the slot.

### B. A file which GDELT does not list or does not serve

The reconciliation confirms such a slot as missing by itself, with `decided_by = 'auto'`:

- an `expected` slot which two reconciliations in a row did not find in the file list, when the slot is 48 hours old;
- a `listed` slot whose file GDELT has not served for 48 hours.

So the first way out is to wait. The reconciliation reads the trailing seven days only: for a slot which is older than about four days when the problem starts, for example a slot of a backfill, the 48 hours end outside the seven days, and the automatic confirmation never comes. For such a slot use the command and do not wait.

The second way out is an earlier decision of the operator, with a note. `--state` names the states which the command may confirm, so a slot which only waits for a download is not confirmed by accident.

```bash
make toolbox CMD="gbmi ledger confirm-missing --from 2026-11-25 --to 2026-11-25 --state expected --state listed --decided-by jay --note 'GDELT published no file for the slots; checked on the master list on 2026-11-27'"
```

When GDELT publishes the file later, the reconciliation moves the slot back to `listed`, and the file is processed as usual.

### C. A file which the 1% rule quarantined

First look at the rejected file: the copy lies under `quarantine/gdelt/<feed>/dt=<day>/<file name>` in the lake (`data/lake` for the working stack). The row of `ops.dq_result` with the check `file_bad_row_share` holds the number of failed rows. When the file is damaged at the source, confirm the slot as missing:

```bash
make toolbox CMD="gbmi ledger confirm-missing --from 2026-11-25 --to 2026-11-25 --feed gkg --state quarantined --decided-by jay --note '3% of the rows have 26 columns; the file at the source is damaged'"
```

When the rows are good and the parser is wrong, the parser has to change first. A slot which was confirmed as missing does not come back by itself after such a repair; the repair of the parser then includes a decision about the confirmed slots.

## Verification

```bash
# Every slot of the day is loaded or confirmed missing, and the day is complete.
make toolbox CMD="gbmi ledger status --from 2026-11-25 --to 2026-11-25"
make toolbox CMD="gbmi ledger completeness --from 2026-11-25 --to 2026-11-25"

# No pending unit is left for the day once the silver job has run.
make toolbox CMD="gbmi silver units"

# The model which dashboards read is rebuilt in full from the ledger.
make toolbox CMD="dbt build --project-dir dbt --profiles-dir dbt/profiles --select +ops_slot_status_daily"
```

A requeued slot goes `quarantined → listed → downloaded → verified → loaded`; the query of the section "Detection" shows the history. A file which arrives for a day with a finished day build makes the whole day pending, and the next run of `gdelt_silver_hourly` builds the day again.

## Prevention

- Gate G1 checks the size and the MD5 of every download against the file list and tries three times before a slot is quarantined.
- The daily reconciliation reads the master list for the trailing seven days, so a gap is seen even when no file arrives.
- The two rules of automatic confirmation close a day without a person after 48 hours, and each automatic decision is written to the ledger with a note.
- Drill DR4 runs in the workflow `e2e` on every pull request: a damaged download, the quarantine, the requeue and the good file.
