# P0 spike report

Three spikes settle the technical risks that could change the design (blueprint §13, risks R1, R2 and R6; week 1 plan WP-06 to WP-08). Each section gives, per question: the result, the evidence (a test that stays in the suite), the decision and any follow-up.

**How to read the results.** *Pass* and *fail* are what the named test showed on the stated versions. An option that was rejected is kept in the suite as a *strict expected failure*: if a later release makes it work, that test starts passing, the suite fails, and the decision is revisited.

**Scope of this run.** The spikes were run on the local backend only. No AWS account was connected, so the Glue/S3 halves — SK1 question Q8 and SK2 question Q6 — were **not run**. Decisions that depend on them are marked *provisional*; a failed AWS half reopens the decision (week 1 plan §6).

| Spike | Risk | Decision | Status |
|---|---|---|---|
| SK1 — PyIceberg contract | — | Overwrite filtered on the unit, in one catalog commit; days are streamed into one file and added with `add_files` | Local: passed. Glue/S3: not run |
| SK2 — ClickHouse pinned read | R1 | Engine-level pinning with `iceberg_metadata_file_path`; no fallback needed | Local: passed. S3: not run |
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

---

## SK2 — ClickHouse reads a pinned table

**Question.** Can a mart build read exactly one Iceberg metadata version (blueprint §5.12, risk R1)?

| | |
|---|---|
| Versions | ClickHouse 26.8.46.6, clickhouse-connect 1.13.0, PyIceberg 0.12.0 |
| Backends run | Local: tables written by PyIceberg under the shared `/lake` mount, read by the Compose ClickHouse |
| Backends not run | S3 with short-lived credentials (Q6) |
| Code | `src/gbmi/warehouse/pin.py`, `src/gbmi/warehouse/client.py` |
| Evidence | `tests/integration/warehouse/test_sk2_pinned_read.py`, `tests/integration/warehouse/test_dialect_checks.py`, `tests/unit/test_pin.py` |

The pin step issues one statement per silver table:

```sql
CREATE OR REPLACE TABLE lake.<table>
ENGINE = IcebergLocal('/lake/warehouse/silver.db/<table>/')
SETTINGS iceberg_metadata_file_path = 'metadata/<file>.metadata.json'
```

### Results

Every comparison is exact row equality between what ClickHouse returns and a PyIceberg scan of the same snapshot.

| Q | Question | Result | Evidence and observation |
|---|---|---|---|
| Q1 | Does an engine-level pin read exactly the named metadata file? | **Pass** | `test_q1_…`. The pinned table returns the rows of the catalog's current snapshot; pinning an older metadata file returns the older rows. Pin plus first read: 12 ms |
| Q2 | Does the pin hold, and does re-pinning take effect? | **Pass** | `test_q2_…`. Two later commits (a new hour, a replaced hour) do not change what the pinned table returns. Re-creating it shows the new rows at once, although the metadata cache is on by default |
| Q3 | Are the types mapped as documented? | **Pass** | `test_q3_…`. `date` → `Date32`, `int` → `Int32`, `long` → `Int64`, `timestamptz` → `DateTime64(6, 'UTC')`, `double` → `Float64`, `string` → `String`, `list<string>` → `Array(Nullable(String))`. Required fields are plain types, optional fields are `Nullable` |
| Q4 | Which local paths may `IcebergLocal` read? | **Pass, after three settings** | `test_q4_…`. See "What ClickHouse needs" below |
| Q5 | Does a plain view over `lake.t` survive re-pinning? | **Pass** | `test_q5_…`. The view returns the new rows without being re-created |
| Q6 | Do Q1 and Q2 hold on S3 with short-lived credentials? | **Not run** | The same tests are parametrised for AWS and skipped without `GBMI_TEST_AWS=1` |
| Q7 | Do the query-level alternatives work? | **Not run** | Required only if Q1 or Q2 fails; otherwise a stretch item |
| Q8 | Is a `slot_date` filter pruned to its partition? | **Not run** | Stretch item |
| — | Does ClickHouse read a day that SK1's pattern (b) added with `add_files`? | **Pass** | `test_day_written_with_add_files_reads_like_any_other`, including the file rewritten by a later hourly overwrite |

### What ClickHouse needs

The documentation says nothing about which local paths `IcebergLocal` may read (finding F7). The spike found three requirements, each now in the repository:

| Requirement | Without it | Where it is set |
|---|---|---|
| `user_files_path` must contain the table: it is set to `/lake/` | `PATH_ACCESS_DENIED: File path … is not inside /var/lib/clickhouse/user_files/` | `docker/clickhouse/config.d/gbmi.xml` |
| `CLICKHOUSE_DO_NOT_CHOWN=1` | The image's entrypoint tries to `chown` `user_files_path`, fails on the read-only lake mount, and the server does not start | `compose/compose.yaml` |
| The pinning role needs `TABLE ENGINE ON IcebergLocal` (and `IcebergS3`) and `INSERT` on `lake.*`, besides `CREATE TABLE` and `DROP TABLE` | `ACCESS_DENIED`. ClickHouse 26.8 requires a grant per engine that reaches outside the server, and `INSERT` to create a table whose engine can write | `db/clickhouse/init/02_roles.sql`, role `pipeline_rw` |

Only the pipeline user holds these grants. The dbt user reads `lake.*` and cannot drop or re-create a pinned table (asserted in `test_q4_…`).

### Decision

**Engine-level pinning is the working mechanism** (`gbmi.warehouse.pin.pin_table`), as blueprint §7.7 preferred. Q1, Q2 and Q5 pass locally, so none of the fallback rungs is taken and no draft of ADR-006 is needed for this spike. The decision is **provisional for S3** until Q6 has run.

### What the spike changes or adds to the design

| Finding | Consequence |
|---|---|
| An Iceberg list that is NULL reads as an empty array, because a ClickHouse `Array` cannot be NULL | A mart cannot tell "no themes" from "themes missing" through the array alone. Where the difference matters (GKG themes and countries, phase P1), silver carries a count or a flag column |
| `pipeline_rw` has to hold `INSERT` on `lake.*` | The pipeline never inserts through ClickHouse. Locally the lake mount is read-only for ClickHouse; on AWS the equivalent protection is the hardening step of blueprint §5.9 (a role for ClickHouse that cannot write to the lake), to be decided in phase P2 |
| `user_files_path` is the lake mount | Any user with the `FILE` source grant could read lake files with `file()`. No role has that grant |
| ClickHouse's documentation recommends the `iceberg` table function over the table engine, because the engine was not designed for schemas that change underneath it | The pinned table is re-created before every build and bound to one metadata file, so its schema cannot change underneath it. A schema change in silver reaches ClickHouse at the next pin |

### Dialect checks

Three of the blueprint's illustrative statements, run on ClickHouse 26.8 with inline data (blueprint §17.3, assumption 16).

| Statement | Result | Detail |
|---|---|---|
| Type-2 intervals with `lagInFrame` and `leadInFrame` (blueprint §7.10) | **Works, with one correction** | A, B, A gives three intervals. The blueprint's warning is right: with the default frame `leadInFrame` sees no later row. But `toDate('9999-12-31')` silently becomes `2149-06-06`, the end of the `Date` range. The open end has to be `toDate32('9999-12-31')`: `Date32`, the type Iceberg dates arrive as, holds that value on 26.8 |
| As-of query with `argMax(value, (known_at, accession_number))` (blueprint §7.10) | **Works as written** | Returns the first-reported value before a restatement and the restated one after it; when two filings share `known_at`, the higher accession number wins |
| Array expressions of the data mining layer (blueprint §8.12) | **Work as written** | `arrayReduce('medianExact', …)`, `arrayMap` over an alias of the same `SELECT` and `length()` in `WHERE` behave as intended. One edge: `arraySort(groupArray(distance))[2]` is 0 when there is only one centroid, so the margin is negative — a one-cluster model must not use it |

### Follow-up

- **Q6 on S3** when an AWS account is connected. `pin.py` already builds the `IcebergS3` statement, with or without short-lived credentials.
- Q7 and Q8 remain stretch items.
