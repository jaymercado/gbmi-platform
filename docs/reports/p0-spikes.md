# P0 spike report

Three spikes settle the technical risks that could change the design (blueprint §13, risks R1, R2 and R6; week 1 plan WP-06 to WP-08). Each section gives, per question: the result, the evidence (a test that stays in the suite), the decision and any follow-up.

**How to read the results.** *Pass* and *fail* are what the named test showed on the stated versions. An option that was rejected is kept in the suite as a *strict expected failure*: if a later release makes it work, that test starts passing, the suite fails, and the decision is revisited.

**Scope of this run.** The spikes were run on the local backend only. No AWS account was connected, so the Glue/S3 halves — SK1 question Q8 and SK2 question Q6 — were **not run**. Decisions that depend on them are marked *provisional*; a failed AWS half reopens the decision (week 1 plan §6).

| Spike | Risk | Decision | Status |
|---|---|---|---|
| SK1 — PyIceberg contract | — | Overwrite filtered on the unit, in one catalog commit; days are streamed into one file and added with `add_files` | Local: passed. Glue/S3: not run |
| SK2 — ClickHouse pinned read | R1 | *(recorded in the SK2 section)* | |
| SK3 A — partition replace | R2 | *(recorded in the SK3 section)* | |
| SK3 B — Cosmos on Airflow 3.4 | R6 | *(recorded in the SK3 section)* | |

---

## SK1 — the PyIceberg contract

**Question.** Does PyIceberg 0.12.0 deliver the idempotency contract of blueprint §5.6 on both lake backends through one catalog factory?

| | |
|---|---|
| Versions | PyIceberg 0.12.0, PyArrow 26.0.1, DuckDB 2.0.1, CPython 3.12.16 |
| Backends run | SQLite catalog with a filesystem warehouse — on the laptop (temporary directory) and in the toolbox container on the shared `/lake` bind mount |
| Backends not run | Glue Data Catalog with S3 (Q8) |
| Code | `src/gbmi/lake/` (`catalog.py`, `tables.py`, `commit.py`, `maintenance.py`), `src/gbmi/core/checksum.py` |
| Evidence | `tests/integration/lake/test_sk1_pyiceberg.py` — run with `make integration` |
| Test table | `slot_date` (date, identity partition), `slot_hour`, an integer key, a `timestamptz`, a double, a `list<string>` and the provenance columns of blueprint §7.12; format version 2; ZSTD |

### Results

| Q | Question | Result | Evidence and observation |
|---|---|---|---|
| Q1 | Does one factory create the same table on both backends? | **Pass** (local) | `test_q1_…`. The table lands at `<namespace location>/<table>`, that is `…/warehouse/silver.db/<table>`; data files are ZSTD Parquet |
| Q2 | Does an overwrite filtered on (`slot_date`, `slot_hour`) replace only that hour? | **Pass** | `test_q2_hour_overwrite_replaces_only_that_hour`. One catalog commit (one new metadata file) made of two snapshots, `delete` then `append`. The hour's file is dropped whole; files of other hours and days keep their paths. About 16 ms on the laptop |
| Q3 | Does an overwrite filtered on `slot_date` replace exactly one day and compact it? | **Pass** | `test_q3_…`. One commit, `delete` then `append`; three hourly files became one; the other day's file kept its path |
| Q4 | Are reruns idempotent, including an empty result? | **Pass** | `test_q4_rerun_…`: same content checksum, new snapshot. `test_q4_empty_input_…`: the unit ends empty. `test_q4_rows_of_another_unit_are_refused` and `test_q4_failed_audit_commits_nothing`: nothing is committed |
| Q5 | How is a whole day written with bounded memory? | **Both patterns pass** | (a) `test_q5a_…`: one transaction, delete plus 24 appends — atomic, but 25 snapshots and 24 files. (b) `test_q5b_…`: the day is streamed into one Parquet file, then delete plus `add_files` — atomic, 2 snapshots, 1 file |
| Q6 | Can a result be audited on a branch and then published? | **Pass** | `test_q6_…`. A write with `branch="audit"` leaves `main` unchanged; `set_current_snapshot(ref_name="audit")` publishes it; the branch is then removed |
| Q7 | What does snapshot expiry do? | **Pass** | `test_q7_…`. Eight commits (15 snapshots): the newest five commits stay whole, a tagged snapshot survives, 4 snapshots are expired. No file is removed: 48 files before, 49 after (the extra one is the new metadata file) |
| Q8 | Do Q1–Q4, Q6 and Q7 hold on Glue/S3? | **Not run** | The same tests are parametrised for AWS and skipped without `GBMI_TEST_AWS=1` |

Rejected options, kept as strict expected failures:

| Option | Why it is rejected | Test |
|---|---|---|
| Streaming a `RecordBatchReader` into the table (blueprint §8.4) | PyIceberg 0.12.0 raises `NotImplementedError` for partitioned tables | `test_rejected_streaming_reader_into_a_partitioned_table` |
| `dynamic_partition_overwrite` | It takes the partitions from the rows and does nothing on empty input, so it cannot empty a unit | `test_rejected_dynamic_partition_overwrite_cannot_empty_a_unit` |

### Decisions

1. **Commit primitive: `overwrite` with an explicit unit filter, inside one transaction** (`gbmi.lake.commit.overwrite_unit`). It is one catalog commit and up to two snapshots, as the week 1 plan expected (difference D8). The helper refuses rows that do not belong to the unit and runs the audit before the commit, so a failed audit leaves the previous version live.
2. **Every snapshot of a commit carries the same `gbmi.commit-id`.** This is what lets maintenance count commits instead of snapshots, and it identifies the rollback target: the snapshot before the commit. A mart records the commit's *last* snapshot as `_silver_snapshot_id`.
3. **Daily pattern: (b), stream then `add_files`** (`gbmi.lake.commit.overwrite_day_streaming`). Memory is bounded by one batch, the day ends as one file (compaction), and the commit has two snapshots, not twenty-five. The file is written with the table's field ids and codec, so it is indistinguishable from a file PyIceberg wrote. Pattern (a) also works and remains the fallback. Whether ClickHouse reads a file added this way is checked in SK2.
4. **Namespace location is set explicitly on both backends** (`<warehouse>/silver.db`). Without it the two catalogs disagree: PyIceberg's SQL catalog defaults to `<warehouse>/<namespace>/<table>` and its Glue catalog to `<warehouse>/<database>.db/<table>`.
5. **Snapshot expiry is by snapshot id** (`gbmi.lake.maintenance.expire_snapshots`): keep the newest five commits whole, expire older snapshots after seven days, never a branch head or a tag.

### What the spike changes or adds to the design

| Finding | Consequence |
|---|---|
| An hourly overwrite inside a day that the daily job has already compacted rewrites that day's file (snapshots `overwrite` then `append`, not `delete` then `append`) | Correct, but it costs a rewrite of the day's file. Late slots for a finalised day are rare; phase P1 measures the cost on real GKG days |
| An empty input still creates an (empty) `append` snapshot | A unit that is empty on every run adds one snapshot per run. Expiry removes them; P1 may skip the commit when the unit is already empty |
| Writing to a branch that does not exist starts from an empty table | The audit branch is always created from `main`'s current snapshot first |
| Expiry removes metadata entries only | Unreferenced files accumulate. On AWS, Athena `VACUUM` removes them (blueprint §7.9); for the local backend this is an open item for phase P4 |
| DuckDB returns `TIMESTAMPTZ` columns to Arrow in the session time zone — `Asia/Manila` on this laptop, UTC in the containers | Every DuckDB connection of the package sets `TimeZone = 'UTC'` (blueprint §7.11); the checksum renders timestamps as microseconds since the epoch, so it is independent of the time zone |
| In PyIceberg 0.12.0 `add_files` accepts Parquet files that carry field ids | This is what makes pattern (b) produce first-class data files |

### Follow-up

- **Q8 on Glue/S3** when an AWS account is connected: `GBMI_TEST_AWS=1 GBMI_TEST_AWS_LAKE_BUCKET=<lake bucket> make integration`, with short-lived credentials in the environment. It also records commit latency.
- Phase P1 reads Q2 and Q5 first: they decide how `gdelt_silver_hourly` and `gdelt_silver_daily` write.
