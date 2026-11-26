# Runbook: a silver build failed, or silver holds wrong data

| | |
|---|---|
| **Pipeline** | PL-2, the DAGs `gdelt_silver_hourly` and `gdelt_silver_daily` (blueprint §8.4) |
| **Written** | 2026-11-26, phase P1, work package WP-11 |
| **Proved by** | Drills DR2 and DR8 (`make drill DR=2`, `make drill DR=8`) and the tests of `tests/integration/silver` |

A **unit** is what one commit replaces in a silver table: one hour or one whole UTC day of one feed. A build reads the verified files of the unit from bronze, checks the rows (gates G2 and G3), commits to the Iceberg tables and then records the build in the ledger: one row in `ops.silver_partition` for each table (a **partition version**) and the state `loaded` for each file. Every command of the runbook runs in the repository folder. `make toolbox CMD="…"` runs a `gbmi` command inside the stack, and `make psql SQL="…"` runs one SQL statement on the ledger. Both commands act on the working stack; add `STACK=gbmi-demo` for the demo stack.

## Symptom

One of the following is true.

- A task of `gdelt_silver_hourly` or `gdelt_silver_daily` failed in Airflow.
- `gbmi silver units` lists the same unit run after run.
- A row count, a duplicate or a value in a silver table is wrong.

## Detection

```bash
# The units which wait for a build.
make toolbox CMD="gbmi silver units"

# The failed runs of the last two days, with the error.
make psql SQL="SELECT started_at, entry_point, run_id, left(error, 300) AS error FROM ops.pipeline_run WHERE status = 'failed' AND entry_point LIKE 'gdelt.silver%' AND started_at > now() - interval '2 days' ORDER BY started_at"

# The failed checks of the last two days. The column detail holds the problem in words.
make psql SQL="SELECT created_at, gate, check_name, table_name, unit, detail FROM ops.dq_result WHERE status = 'failed' AND created_at > now() - interval '2 days' ORDER BY created_at"

# The builds of one day, oldest first: rows, files, checksum and snapshots.
make psql SQL="SELECT partition_version_id, table_name, unit_kind, slot_hour, committed_rows, file_count, content_digest, snapshot_id, previous_snapshot_id, committed_at FROM ops.silver_partition WHERE source = 'gdelt' AND slot_date = '2026-11-25' ORDER BY partition_version_id"
```

## Diagnosis

A build which fails commits nothing: the version of the unit from before the build stays in silver. Find the error text in the table.

| Error text | Cause | Way out |
|---|---|---|
| `AuditError: unit … failed the pre-commit audit`, with "raw lines", "committed", "not unique", "rows have no", "NULL lists" or "organisation rows" | Gate G3 found rows which do not add up: a defect in a parser, or input which the parser does not expect. No slot changed | Read the problem in `ops.dq_result`. Repair the parser, with the failing file as a test. Then recovery A |
| `AuditError … rows are outside 0.5 to 2.0 times the median` | The day has far fewer or far more rows than the same weekday of the four weeks before | Find the cause first: an outage of GDELT, a holiday, a missing feed. A true small or large day: recovery B |
| `BuildError: slot … is loaded, but the file in bronze has the MD5 …` or `… the file is not in bronze` | The bronze copy of a file which is loaded already changed or is gone. No state leads out of `loaded`, so the build stops | Recovery C: restore the bronze file |
| `BuildError: slot … is loaded, but … rows failed gate G2` | A file which was loaded before now fails the 1% rule. The rules of the parser changed since the load | The change of the parser is the cause. Decide with the diff of the parser whether the old rule or the new rule is right; the build of the unit works again when the file passes |
| `BuildError: … units failed: …` | The summary of a run of `gdelt_silver_hourly`: one or more units failed, the other units were built | Look up each named unit in `ops.pipeline_run` and `ops.dq_result`, and follow the row of the diagnosis table for the error of the unit |
| `BuildError: the run built no day-level version of …` | The task `summary` of `gdelt_silver_daily`: the build of one feed failed | Open the failed task `build_<feed>` of the same run, and follow the row of the diagnosis table for the error of the task |
| `CommitFailedException` or `ValidationException` | Another process committed to the same table at the same moment, four times in a row | Recovery A. The pool `silver_writer` of Airflow has one slot, so the usual cause is a build by hand beside a DAG run |
| The task was killed, or the container stopped | A crash. Before the commit nothing is visible. After the commit silver holds the new rows, and the slots are still `verified` | Nothing to repair by hand: the next run builds the same unit again and records the build (drill DR2). Recovery A does the same at once |
| No error: a unit holds wrong rows | A bad publication: a defect which the gates did not catch | Repair the cause, then recovery A. The section "A bad publication" explains the snapshot before the commit |

## Recovery steps

### A. Build the unit again from bronze

A build is safe to repeat: the unit is replaced as a whole, and a second build of the same files gives the same content checksum (drill DR8).

```bash
# One hour of one feed, or of the three feeds without --feed.
make toolbox CMD="gbmi silver build-hour --hour 2026-11-25T08 --feed gkg"

# One whole day. The day build replaces the hourly files of the day with one file.
make toolbox CMD="gbmi silver build-day --day 2026-11-25 --feed gkg"
```

In Airflow the same build is a run of `gdelt_silver_daily` by hand with the parameter `day` set to `2026-11-25`.

For a range of days use the backfill of Airflow. The two dates of the command are the times at which the runs may start, and the run which may start at 08:25 on one day builds the day before. So for the days A to B the command names the days A+1 to B+1. Always start with the dry run and compare the printed logical dates with the days to build:

```bash
# The days 2026-11-20 to 2026-11-22.
docker compose --project-name gbmi --project-directory . --env-file .env -f compose/compose.yaml exec airflow-scheduler \
  airflow backfill create --dag-id gdelt_silver_daily \
  --from-date 2026-11-21T08:25:00+00:00 --to-date 2026-11-23T08:25:00+00:00 \
  --max-active-runs 1 --dry-run
```

The same command without `--dry-run` starts the backfill. The DAG has to be unpaused: Airflow does not start the runs of a paused DAG.

Three rules for a backfill:

- **Enqueue first.** `gbmi gdelt enqueue` creates the ledger rows of the named days only, and the routine ingest continues the calendar after the newest slot of the ledger. A range which is enqueued after an ingest run and does not reach the existing rows leaves the days in between without ledger rows, and no report names the missing slots. So enqueue before the first ingest run, or let the range end on the day before the oldest day of the ledger.
- **Before the builds:** no slot of the range is `listed` or `quarantined`. `gbmi ledger status` shows the states; the [other runbook](gdelt-slot-quarantined-or-overdue.md) closes such a slot.
- **After the builds:** no slot of the range is `verified`. `gdelt_silver_hourly` looks at the trailing seven days only, so a file which is verified after the day build of an older day waits until the day is built again by hand with `gbmi silver build-day`.

Do not start `gdelt_silver_daily` by hand for the current UTC day. The day then counts as finalised, and `gdelt_silver_hourly` builds the whole day again every 15 minutes until the day ends.

### B. Accept a day outside the volume band

When the day is truly small or large, build the day with a note. The note is stored with the check, and the status of the check is `accepted`.

```bash
make toolbox CMD="gbmi silver build-day --day 2026-11-25 --feed export --accept-volume 'GDELT published no export files from 02:00 to 09:00 UTC'"
```

In Airflow: a run of `gdelt_silver_daily` with the parameters `day` and `accept_volume`.

### C. Restore the bronze file of a loaded slot

The ledger holds the address and the MD5 of the file. Read both, download the file to the bronze key, and compare the MD5 before the build. The lake of the working stack is the folder `data/lake`.

```bash
make psql SQL="SELECT url, expected_md5, bronze_uri FROM ops.source_slot WHERE source = 'gdelt' AND feed = 'export' AND slot_ts = '2026-11-25 08:30+00'"

# The address is the value of the column url. The key under data/lake is the part of
# bronze_uri after file:///lake/.
curl --fail --silent --show-error --user-agent "gbmi-platform" \
  --output data/lake/raw/gdelt/v2/export/dt=2026-11-25/20261125083000.export.CSV.zip \
  https://data.gdeltproject.org/gdeltv2/20261125083000.export.CSV.zip
# macOS; on Linux the command is md5sum.
md5 -q data/lake/raw/gdelt/v2/export/dt=2026-11-25/20261125083000.export.CSV.zip
```

The printed MD5 has to equal `expected_md5`. Then use recovery A. When GDELT no longer serves the file with the MD5 of the ledger, stop here: the case needs a decision which phase P1 has no command for, and the unit keeps the last good version in silver.

### A bad publication

A build which passed every gate and still wrote wrong rows is repaired forwards: correct the cause, then build the unit again with recovery A. The new build replaces the unit.

Each partition version names two snapshots of the Iceberg table: `snapshot_id`, the snapshot after the commit, and `previous_snapshot_id`, the snapshot before the commit. The last query of the section "Detection" lists both for every build of a day. `previous_snapshot_id` of the first bad build is the last state of the table without the bad rows, and a reader can still query the snapshot for a comparison.

Phase P1 has no rollback command, by decision (difference D29 of the week 2 plan). After a rollback the files of the removed commits would still be `loaded` in the ledger while silver no longer holds the rows of the files, and no state leads out of `loaded`. A rollback needs the repair queue of gate G6, which is work of phase P4.

## Verification

```bash
# The unit is no longer pending.
make toolbox CMD="gbmi silver units"

# The files of the day are loaded.
make toolbox CMD="gbmi ledger status --from 2026-11-25 --to 2026-11-25"

# The newest partition version of each table: rows, files and checksum.
make psql SQL="SELECT DISTINCT ON (table_name) table_name, unit_kind, committed_rows, file_count, content_digest, committed_at FROM ops.silver_partition WHERE source = 'gdelt' AND slot_date = '2026-11-25' ORDER BY table_name, partition_version_id DESC"

# Accounting: for every loaded file, raw lines = parsed rows + quarantined rows.
make psql SQL="SELECT count(*) AS loaded_files, count(*) FILTER (WHERE raw_lines = parsed_rows + quarantined_rows) AS balanced FROM ops.source_slot WHERE source = 'gdelt' AND state = 'loaded' AND slot_ts >= '2026-11-25' AND slot_ts < '2026-11-26'"
```

A second build of the same day gives the same `content_digest` as the first one. The drill does the comparison for a real day of the stack:

```bash
make drill DR=8 ARGS="--day 2026-11-25"
```

The command builds the day again for the three feeds and adds one partition version for each table.

## Prevention

- Gate G3 runs before every commit, and a failed audit commits nothing.
- A build records the files which went in (`fileset_hash`) and the content of the unit (`content_digest`), so two builds can be compared without reading the tables.
- The pool `silver_writer` lets one silver task run at a time. A commit which still meets another writer is tried again up to three times.
- Drills DR2 and DR8 run in the workflow `e2e` on every pull request: a kill before the commit, a kill after the commit, and a second build of a day.

## Known leftover

A process which dies before the commit of a day build leaves one staged Parquet file in the data folder of the table (a name which ends in `-day.parquet`). No snapshot names the file, so no reader sees the file. Phase P1 does not remove such files; the removal of unreferenced files is maintenance work of a later phase (blueprint §7.9).
