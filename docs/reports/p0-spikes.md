# P0 spike report

Three spikes settle the technical risks that could change the design (blueprint §13, risks R1, R2, R3 and R6; week 1 plan WP-06 to WP-08). They were run on 2026-11-25 on the local backend; the Glue/S3 halves of SK1 and SK2 were run on 2026-11-26. Each section gives, per question: the result, the evidence (a test that stays in the suite), the decision and any follow-up.

**How to read the results.** *Pass* and *fail* are what the named test showed on the stated versions. An option that was rejected is kept in the suite as a *strict expected failure*: if a later release makes it work, that test starts passing, the suite fails, and the decision is revisited.

**Scope.** Every spike was run on the local backend on 2026-11-25. The Glue/S3 halves — SK1 question Q8 and SK2 question Q6 — waited for an AWS account. They were run on 2026-11-26 against Glue and S3 in `us-east-1` and passed, so the decisions that depended on them are no longer provisional.

| Spike | Risk | Decision | Status |
|---|---|---|---|
| SK1 — PyIceberg contract | R3 | Overwrite filtered on the unit, in one catalog commit; days are streamed into one file and added with `add_files` | Passed on both backends |
| SK2 — ClickHouse pinned read | R1 | Engine-level pinning with `iceberg_metadata_file_path`; no fallback needed | Passed on the local backend and on S3 |
| SK3 A — partition replace | R2 | Fallback taken: the custom `partition_replace` materialisation. The adapter's `insert_overwrite` fails test T4 | Passed (T1–T6, by day and by month) |
| SK3 B — Cosmos on Airflow 3.4 | R6 | Cosmos as designed; no fallback needed | Passed (B1–B5) |

---

## SK1 — the PyIceberg contract

**Question.** Does PyIceberg 0.12.0 deliver the idempotency contract of blueprint §5.6 on both lake backends through one catalog factory?

| | |
|---|---|
| Versions | PyIceberg 0.12.0, PyArrow 26.0.1, DuckDB 2.0.1, CPython 3.12.16 |
| Backends run | SQLite catalog with a filesystem warehouse — on the laptop (temporary directory) and in the toolbox container on the shared `/lake` bind mount. Glue Data Catalog with S3 in `us-east-1` (Q8), from the toolbox container on the laptop |
| Code | `src/gbmi/lake/` (`catalog.py`, `tables.py`, `commit.py`, `maintenance.py`), `src/gbmi/core/checksum.py` |
| Evidence | `tests/integration/lake/test_sk1_pyiceberg.py` — run with `make integration`, and on AWS with `make integration-aws` |
| Test table | `slot_date` (date, identity partition), `slot_hour`, an integer key, a `timestamptz`, a double, a `list<string>` and the provenance columns of blueprint §7.12; format version 2; ZSTD |

### Results

| Q | Question | Result | Evidence and observation |
|---|---|---|---|
| Q1 | Does one factory create the same table on both backends? | **Pass** | `test_q1_…`. The table lands at `<namespace location>/<table>`, that is `…/warehouse/silver.db/<table>`; data files are ZSTD Parquet |
| Q2 | Does an overwrite filtered on (`slot_date`, `slot_hour`) replace only that hour? | **Pass** | `test_q2_hour_overwrite_replaces_only_that_hour`. One catalog commit (one new metadata file) made of two snapshots, `delete` then `append`. The hour's file is dropped whole; files of other hours and days keep their paths. About 16 ms on the laptop |
| Q3 | Does an overwrite filtered on `slot_date` replace exactly one day and compact it? | **Pass** | `test_q3_…`. One commit, `delete` then `append`; three hourly files became one; the other day's file kept its path |
| Q4 | Are reruns idempotent, including an empty result? | **Pass** | `test_q4_rerun_…`: same content checksum, new snapshot. `test_q4_empty_input_…`: the unit ends empty. `test_q4_rows_of_another_unit_are_refused` and `test_q4_failed_audit_commits_nothing`: nothing is committed |
| Q5 | How is a whole day written with bounded memory? | **Both patterns pass** | (a) `test_q5a_…`: one transaction, delete plus 24 appends — atomic, but 25 snapshots and 24 files. (b) `test_q5b_…`: the day is streamed into one Parquet file, then delete plus `add_files` — atomic, 2 snapshots, 1 file |
| Q6 | Can a result be audited on a branch and then published? | **Pass** | `test_q6_…`. A write with `branch="audit"` leaves `main` unchanged; `set_current_snapshot(ref_name="audit")` publishes it; the branch is then removed |
| Q7 | What does snapshot expiry do? | **Pass** | `test_q7_…`. Eight commits (15 snapshots): the newest five commits stay whole, a tagged snapshot survives, 4 snapshots are expired. No file is removed: 48 files before, 49 after (the extra one is the new metadata file) |
| Q8 | Do Q1–Q4, Q6 and Q7 hold on Glue/S3? | **Pass** | All 13 tests of the file passed on Glue and S3 on 2026-11-26 (`make integration-aws`), Q5 included. Tables land at `s3://<lake bucket>/ci/warehouse/silver.db/<table>`, in the Glue database `gbmi_ci_silver`. A whole test took 13 to 194 seconds from the laptop, and pattern (a) of Q5 took 471 seconds: round trips to `us-east-1`, not a measure of commit latency |

Rejected options, kept as strict expected failures:

| Option | Why it is rejected | Test |
|---|---|---|
| Streaming a `RecordBatchReader` into the table (blueprint §8.4) | PyIceberg 0.12.0 raises `NotImplementedError` for partitioned tables | `test_rejected_streaming_reader_into_a_partitioned_table` |
| `dynamic_partition_overwrite` | It takes the partitions from the rows and does nothing on empty input, so it cannot empty a unit | `test_rejected_dynamic_partition_overwrite_cannot_empty_a_unit` |

### Decisions

1. **Commit primitive: `overwrite` with an explicit unit filter, inside one transaction** (`gbmi.lake.commit.overwrite_unit`). It is one catalog commit and up to two snapshots, as the week 1 plan expected (difference D8). The helper refuses rows that do not belong to the unit and runs the audit before the commit, so a failed audit leaves the previous version live.
2. **Every snapshot of a commit carries the same `gbmi.commit-id`.** This is what lets maintenance count commits instead of snapshots, and it identifies the rollback target: the snapshot before the commit. A mart records the commit's *last* snapshot as `_silver_snapshot_id`.
3. **Daily pattern: (b), stream then `add_files`** (`gbmi.lake.commit.overwrite_day_streaming`). Memory is bounded by one batch, the day ends as one file (compaction), and the commit has two snapshots, not twenty-five. The file is written with the table's field ids and codec, so it is indistinguishable from a file PyIceberg wrote. Pattern (a) also works and remains the fallback. Whether ClickHouse reads a file added this way is checked in SK2. The memory probe later ran this pattern on a real day: 96 GKG files (1,455 MB raw, 111,522 rows) became one 103 MB file in 6.9 seconds with a 535 MiB peak, and a rebuild of the day was one commit of `delete` and `append` (`docs/reports/p0-memory.md`).
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
| On S3, from the laptop, the test of pattern (a) took 471 seconds and the test of pattern (b) 194 seconds | One more reason for decision 3: 24 appends are 24 rounds of metadata writes |
| `purge_table` on S3 removes what the current metadata refers to and nothing else. After the AWS run, 43 zero-byte directory markers (keys that end in `/`, created by PyArrow's S3 file system) and the 9 files of the snapshots that Q7 had expired were still current objects | The Q7 finding holds on S3: expiry removes no file. The file clean-up of phase P4 has to cover unreferenced files and directory markers; the P0 close-out deleted them by hand |
| Credentials from `aws login` last 15 minutes and cannot be renewed inside a container, and the AWS halves take longer | They run one test at a time, each with fresh credentials (`tests/integration/run_aws.sh`; week 1 plan, D13). On the host the instance role makes that unnecessary |

### Follow-up

- Q8 ran on 2026-11-26 (see Results). Commit latency on S3 was not measured on its own; phase P1 measures it on the host, beside the hourly jobs.
- Phase P1 reads Q2 and Q5 first: they decide how `gdelt_silver_hourly` and `gdelt_silver_daily` write.

---

## SK2 — ClickHouse reads a pinned table

**Question.** Can a mart build read exactly one Iceberg metadata version (blueprint §5.12, risk R1)?

| | |
|---|---|
| Versions | ClickHouse 26.8.46.6, clickhouse-connect 1.13.0, PyIceberg 0.12.0 |
| Backends run | Local: tables written by PyIceberg under the shared `/lake` mount, read by the Compose ClickHouse. S3 (Q6): tables written to S3 through Glue, read by the same ClickHouse with short-lived credentials |
| Code | `src/gbmi/warehouse/pin.py`, `src/gbmi/warehouse/client.py` |
| Evidence | `tests/integration/warehouse/test_sk2_pinned_read.py`, `tests/integration/warehouse/test_dialect_checks.py`, `tests/unit/test_pin.py` |

The pin step issues one statement per silver table:

```sql
CREATE OR REPLACE TABLE lake.<table>
ENGINE = IcebergLocal('/lake/warehouse/silver.db/<table>/')
SETTINGS iceberg_metadata_file_path = 'metadata/<file>.metadata.json'
```

On S3 the engine is `IcebergS3('s3://<bucket>/…/<table>/')`. On a laptop the access key, the secret and the session token follow the location as arguments; on the host ClickHouse uses the instance role and the statement carries no credentials.

### Results

Every comparison is exact row equality between what ClickHouse returns and a PyIceberg scan of the same snapshot.

| Q | Question | Result | Evidence and observation |
|---|---|---|---|
| Q1 | Does an engine-level pin read exactly the named metadata file? | **Pass** | `test_q1_…`. The pinned table returns the rows of the catalog's current snapshot; pinning an older metadata file returns the older rows. Pin plus first read: 12 ms |
| Q2 | Does the pin hold, and does re-pinning take effect? | **Pass** | `test_q2_…`. Two later commits (a new hour, a replaced hour) do not change what the pinned table returns. Re-creating it shows the new rows at once, although the metadata cache is on by default |
| Q3 | Are the types mapped as documented? | **Pass** | `test_q3_…`. `date` → `Date32`, `int` → `Int32`, `long` → `Int64`, `timestamptz` → `DateTime64(6, 'UTC')`, `double` → `Float64`, `string` → `String`, `list<string>` → `Array(Nullable(String))`. Required fields are plain types, optional fields are `Nullable` |
| Q4 | Which local paths may `IcebergLocal` read? | **Pass, after three settings** | `test_q4_…`. See "What ClickHouse needs" below |
| Q5 | Does a plain view over `lake.t` survive re-pinning? | **Pass** | `test_q5_…`. The view returns the new rows without being re-created |
| Q6 | Do Q1 and Q2 hold on S3 with short-lived credentials? | **Pass** | On 2026-11-26 `test_q1_…` to `test_q5_…` and `test_day_written_with_add_files_…` passed against S3 (`make integration-aws`): six tests, 77 to 125 seconds each from the laptop. The credentials of an `aws login` session — access key, secret and session token — are arguments of the `IcebergS3` engine. The stretch questions Q7 and Q8 skip themselves on AWS |
| Q7 | Do the query-level alternatives work? | **Recorded** (stretch, local) | `test_q7_table_function_…`: the `icebergLocal` table function reads a named metadata file when the setting is written inside its parentheses; as a query-level setting the same name is rejected (`UNKNOWN_SETTING`), and the dbt role is refused because the function needs `READ ON FILE`. `test_q7_snapshot_id_setting_…`: `SETTINGS iceberg_snapshot_id` on an unpinned table returns that snapshot's rows, for the dbt role too, and still does after a later commit; the unpinned table itself follows each commit at once |
| Q8 | Is a `slot_date` filter pruned to its partition? | **Pass** (stretch, local) | `test_q8_…`: four files, 36 rows. `WHERE slot_date = …` reads 11 rows, the day's file, and the query log shows `IcebergPartitionPrunedFiles = 3`. The same holds through `toDate(slot_date)`, the form the staging model uses; a month filter reads only that month's files. With `use_iceberg_partition_pruning = 0` the Parquet statistics still skip the other three files |
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

**Engine-level pinning is the working mechanism** (`gbmi.warehouse.pin.pin_table`), as blueprint §7.7 preferred. Q1, Q2 and Q5 pass locally, so none of the fallback rungs is taken and no draft of ADR-006 is needed for this spike. Q6 passed on S3 on 2026-11-26, so the decision holds on both backends.

Q7 changes the order of the fallback ladder, should it ever be needed. The plan's first rung, a query-level `iceberg_metadata_file_path`, does not exist: the setting belongs to the table function, and using the function would give the dbt role `READ ON FILE`, which on the local backend opens every file under the lake mount. The second rung, `iceberg_snapshot_id` on an unpinned table, needs no new grant and would be taken first.

### What the spike changes or adds to the design

| Finding | Consequence |
|---|---|
| An Iceberg list that is NULL reads as an empty array, because a ClickHouse `Array` cannot be NULL | A mart cannot tell "no themes" from "themes missing" through the array alone. Where the difference matters (GKG themes and countries, phase P1), silver carries a count or a flag column |
| `pipeline_rw` has to hold `INSERT` on `lake.*` | The pipeline never inserts through ClickHouse. Locally the lake mount is read-only for ClickHouse; on AWS the equivalent protection is the hardening step of blueprint §5.9 (a role for ClickHouse that cannot write to the lake), to be decided in phase P2 |
| `user_files_path` is the lake mount | Any user with the `FILE` source grant could read lake files with `file()`. No role has that grant |
| On a laptop a pinned S3 table carries credentials that expire after 15 minutes (`aws login`) | Enough for a test. A mart build on a laptop against S3 would have to re-pin within that time; on the host the instance role removes the limit |
| A filter on `slot_date` reaches Iceberg's partition values, also through `toDate(slot_date)` and `toYYYYMM(slot_date)` | A mart build that asks for one month reads only that month's files. The staging model needs no change for the volumes of phase P1 |
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
- Q7 and Q8 were recorded on the local backend only; they are not repeated on S3.

---

## SK3 — partition replace and Cosmos

### Part A — the incremental contract

**Question.** Can a partition-replaced mart be rebuilt idempotently, one partition at a time (blueprint §7.7, risk R2)?

| | |
|---|---|
| Versions | dbt-core 1.12.7, dbt-clickhouse 1.10.4, ClickHouse 26.8.46.6 |
| Code | `dbt/macros/materializations/partition_replace.sql`, `dbt/macros/partitions.sql` |
| Evidence | `tests/integration/dbt/test_sk3_partition_replace.py` with the test-only dbt project in `tests/integration/dbt/project/`. Run by `make integration`. The `dbt` workflow is written to run it on every pull request; no workflow has run yet |
| Set-up | A source of three days in two months. Every test runs for a model partitioned by day (`slot_date`) and one partitioned by month (`toYYYYMM(slot_date)`). dbt connects as a user holding exactly the privileges of the `dbt_rw` role |

**What the adapter's own strategy does.** `insert_overwrite` in dbt-clickhouse 1.10.4 builds a staging table, reads the partitions *present in it* and replaces exactly those. A partition whose rebuild returns no rows is not in the staging table, so it is never replaced and keeps its old rows (read in the installed adapter source, `incremental.sql`, macro `clickhouse__incremental_insert_overwrite`).

| T | Test | `insert_overwrite` (stock) | `partition_replace` (custom) |
|---|---|---|---|
| T1 | First build creates every partition | Pass | **Pass** |
| T2 | Identical rebuild: same count and checksum per partition, no duplicates | Pass | **Pass** |
| T3 | One partition's input changes; build with `partitions=[p]`: only `p` changes | Pass | **Pass** — a second partition whose input also changed, but was not requested, keeps its old rows |
| T4 | One partition's input becomes empty: the partition ends empty | **Fail** — kept as a strict expected failure | **Pass** |
| T5 | The replace step fails (user without `ALTER`) | Not run | **Pass** — target unchanged; the staging table of the failed run is left behind; the next run gives the correct result |
| T6 | `--full-refresh` equals the incremental result | Not run | **Pass** |

Three further tests guard the materialisation itself: a model that ignores the requested partitions is refused before anything is swapped; a partition run without a partition list is an error; an empty list is a no-op.

**Decision: the fallback of blueprint §7.7 step 3 is adopted.** Partition-replaced marts use the `partition_replace` materialisation. It replaces every *requested* partition — those the new data contains, and those the request selects in the target — with `ALTER TABLE … REPLACE PARTITION ID … FROM <staging>` and `allow_replace_partition_from_empty_source = 1`. The requested partitions come from the dbt variable `partitions`. The decision is recorded in the draft of ADR-006.

| Finding | Consequence |
|---|---|
| A failed run leaves its staging table behind, named `<model>__dbt_new_data_<invocation id>` | The next run uses a new name and is not disturbed. Dropping orphaned staging tables is an open item for phase P4, as the week 1 plan already lists |
| Several `REPLACE PARTITION` commands in one `ALTER` are each atomic, not atomic together | A crash between two partitions leaves some replaced and some not. The rerun repairs it, which is the behaviour blueprint §7.7 item 5 expects |
| The partition id of a `Date32` key is its day number (20756 for 2026-10-30), not `YYYYMMDD` as for a `Date` key | The materialisation reads ids from `_partition_id` and never builds them, so it is unaffected. Anything that names partitions by id must not assume `YYYYMMDD` |
| dbt-clickhouse creates the profile's default database on connect | The dbt user needs `CREATE DATABASE` on it; `dbt_rw` has it for the five databases it builds into |

### Part B — Cosmos on Airflow 3.4

**Question.** Does Cosmos 1.16.0 run dbt from its own environment on Airflow 3.4.0, with nothing parsed when DAG files load (blueprint §5.5, risk R6)?

| | |
|---|---|
| Versions | Apache Airflow 3.4.0 (LocalExecutor), astronomer-cosmos 1.16.0, dbt-core 1.12.7 in `/opt/venvs/dbt`, `gbmi` in `/opt/venvs/gbmi` |
| Evidence | A spike DAG, `spike_sk3_cosmos`, with a one-model dbt project, run by hand twice on 2026-11-25. Both files were removed when the tracer DAG superseded them; the branch `wp-08-sk3-partition-replace-cosmos` still has them. What guards each answer now: the image build fails if `pip check` does (B1), and every `make demo` runs the tracer DAG twice, which exercises B2, B4 and B5 — the second run fails if the partition list does not reach dbt. B3 was observed by hand and has no automated guard until the DAG test suite of phase P1 |

| Q | Question | Result | Observation |
|---|---|---|---|
| B1 | Does the image build with Cosmos 1.16.0 under the Airflow 3.4.0 constraints? | **Pass** | `pip check`: "No broken requirements found" (image build, WP-05) |
| B2 | Does a `DbtTaskGroup` render from a prebuilt manifest and run dbt from `/opt/venvs/dbt`? | **Pass** | `LoadMode.DBT_MANIFEST`, `ExecutionConfig(dbt_executable_path=…, invocation_mode=SUBPROCESS)` and `ProfileConfig(profiles_yml_filepath=…)`: one task, `dbt.spike_sk3_cosmos_run`, for the one model; the run is green |
| B3 | Is DAG parsing free of dbt calls? | **Pass** | Parsing the file takes 0.51 s; dbt's log file is untouched by it |
| B4 | Do runtime variables reach dbt? | **Pass** | The second run was triggered with `{"partitions": [202611]}`. Afterwards partition 202610 still carried the first run's dbt invocation id and partition 202611 the second run's |
| B5 | Does `@task.external_python` run a `gbmi` function from `/opt/venvs/gbmi`? | **Pass** | The task reported its interpreter as `/opt/venvs/gbmi/…` and returned a dictionary through XCom to a task in the Airflow environment |

**Decision: Cosmos as designed.** No fallback is taken and no draft of ADR-010 is needed.

| Finding | Consequence |
|---|---|
| A templated list arrives in dbt as the string `"[202611]"` unless the DAG sets `render_template_as_native_obj=True` | The tracer DAG sets it, and the macro `requested_partitions()` also parses a JSON string, so neither mistake can make a model iterate over characters |
| Airflow 3 looks for new DAG files every 300 seconds by default, and DAGs are paused at creation | The local stack sets the interval to 30 seconds; `make demo` unpauses the tracer and bounds its wait |
| An `external_python` task's result is unpickled in the Airflow environment, where `gbmi` is not installed | Tasks return built-in types only, and import `gbmi` inside the function |

---

## Open items from the spikes

| Item | Closes when |
|---|---|
| SK1 Q8: the lake contract on Glue and S3, with commit latency | An AWS account is connected and the Terraform baseline is applied. Until then ADR-004 is provisional |
| SK2 Q6: ClickHouse reads a pinned table on S3 with short-lived credentials | The same run, after Q8 |
| SK3 B3 has no automated guard | The DAG test suite of phase P1 |
| The cost of an hourly overwrite inside an already compacted day; skipping the commit for a unit that is empty and stays empty | Phase P1, on real GKG days |
| A flag or count column wherever "no values" and "values missing" must differ in a mart | Phase P1, with the GKG parser |
| ClickHouse's role on AWS should not be able to write to the lake, although the pinning role needs `INSERT` on `lake.*` | Phase P2, with the host's IAM roles |
| Unreferenced files on the local backend; orphaned dbt staging tables | Phase P4 |
