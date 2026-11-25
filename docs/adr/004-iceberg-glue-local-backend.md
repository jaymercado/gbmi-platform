# ADR-004 — Iceberg with Glue; a local filesystem backend instead of an S3 emulator

| | |
|---|---|
| **Date** | 2026-11-25 |
| **Status** | **Provisional** — the local backend is proven; the Glue/S3 backend has not been run (see Evidence) |
| **Sources** | Blueprint §5.4, §5.6, §5.12, §7.1, §7.4, §7.9; week 1 plan WP-06 and WP-07 |
| **Related** | TD-05, TD-06 (blueprint §6.2); risks R1 and R3; ADR-006 |

## Context

Silver tables have to be replaced one unit at a time, atomically, with a history that allows a bad publication to be rolled back — and they have to be readable by every engine in the design: PyIceberg and DuckDB for routine writes, Spark for bulk backfills, ClickHouse for the marts and Athena for ad hoc queries and maintenance (blueprint §5.4).

Two further constraints shape the choice. The platform must run **offline** — on a laptop, in CI and for a reviewer who runs `make demo` — with no cloud account and at no cost. And it is operated by one person on about USD 30 a month, so every extra service has to earn its place.

## Decision

1. **Table format.** Silver tables are Apache Iceberg, format version 2, with ZSTD-compressed Parquet. GDELT tables are partitioned by `slot_date` with the identity transform, and the partition spec never evolves.
2. **Catalog.** The AWS Glue Data Catalog in production; PyIceberg's SQL catalog on SQLite locally and in CI. One factory (`gbmi.lake.catalog`) returns the right one from configuration; application code never branches on the environment.
3. **Storage.** Amazon S3 in production; the local filesystem otherwise, mounted at the same path (`/lake`) in every container, because Iceberg metadata holds absolute URIs. **No S3 emulator is run.**
4. **Location.** The silver namespace has an explicit location, `<warehouse>/silver.db`, on both backends, so a table is always at `<warehouse>/silver.db/<table>`.
5. **Commit contract.** A unit — one slot-hour or one slot-date — is replaced with an `overwrite` filtered on the unit, inside one transaction, after an in-memory audit (`gbmi.lake.commit.overwrite_unit`). That is one catalog commit and up to two snapshots, a `delete` and an `append`.
6. **Daily builds** stream the day into one Parquet file that carries the table's field ids, then delete the day and add the file in one commit (`overwrite_day_streaming`).
7. **Reading from ClickHouse.** Each mart build reads a table pinned to one exact metadata file through the engine-level setting `iceberg_metadata_file_path` (`gbmi.warehouse.pin`).
8. **Maintenance.** Snapshots are expired by id: the newest five commits stay whole, older snapshots go after seven days, and a branch head or tag is never expired.

## Alternatives considered

| Alternative | Why it was not chosen |
|---|---|
| Plain partitioned Parquet | No atomic replacement and no history: a reader can see half a partition, and a bad load cannot be rolled back |
| Delta Lake, Hudi, DuckLake | Delta is strongest inside Databricks; neither it nor Hudi is read by every engine here as Iceberg is; DuckLake is new in 2026 |
| A self-hosted REST catalog (Polaris, Lakekeeper) | One more service to run locally and in production. A managed catalog plus a file-based one is less work |
| Amazon S3 Tables | It hides the maintenance — expiry, compaction — that this project sets out to learn |
| An S3 emulator for local work (MinIO, LocalStack, RustFS, SeaweedFS) | MinIO's community edition is unmaintained, LocalStack needs an account token, the newer ones are unproven with this stack. An emulator would be a service to operate and debug that is not the real thing either |
| `dynamic_partition_overwrite` | It infers the partitions from the rows and does nothing on empty input, so it cannot empty a unit. Kept as a strict expected failure in the suite |
| Streaming a `RecordBatchReader` into the table (blueprint §8.4) | PyIceberg 0.12.0 raises `NotImplementedError` for partitioned tables. Kept as a strict expected failure |
| One transaction that deletes the day and appends it hour by hour | It works and is atomic, but creates 25 snapshots and 24 files per day and compacts nothing. It remains the fallback for decision 6 |
| Pinning in each query: the table function with the metadata file as its setting, or `iceberg_snapshot_id` on an unpinned table | Not needed: engine-level pinning passed. Both work (SK2 Q7). The first needs a file-read grant for the dbt role; the second needs none and would be the first fallback |

## Consequences

**Follow from one commit being up to two snapshots** (week 1 plan, difference D8):

- A **rollback** goes to the snapshot *before the commit*, not to the previous snapshot. `overwrite_unit` returns it as `previous_snapshot_id`.
- **Expiry counts commits, not snapshots.** Every snapshot of a commit carries the same `gbmi.commit-id` in its summary; "keep the last five" means five commits, kept whole.
- A mart records the commit's **last** snapshot as `_silver_snapshot_id`.

**Easier**

- Every hop into silver is idempotent: a rerun gives the same rows and a new snapshot, and an empty result empties the unit.
- The same tests run on both backends, and the whole platform runs with no cloud account.
- ClickHouse never reads a table that is being written.

**Harder, or to be aware of**

- Maintenance is the operator's job. Expiry removes metadata entries only; unreferenced files stay until Athena `VACUUM` removes them on AWS. For the local backend there is no clean-up yet (open item for phase P4).
- An hourly overwrite inside a day that has already been compacted rewrites that day's file. It is correct; the cost on real GKG days is measured in phase P1.
- The local backend does not exercise S3 semantics (consistency, permissions, request cost). The blueprint answers this with integration tests against real S3 on a schedule; until an AWS account is connected those tests are skipped.
- ClickHouse needs three things to read the local lake that its documentation does not mention: `user_files_path` set to the lake mount, `CLICKHOUSE_DO_NOT_CHOWN=1` because that mount is read-only, and for the pinning role `TABLE ENGINE` grants plus `INSERT` on `lake.*`.
- An Iceberg list that is NULL reads as an empty array in ClickHouse.

## Evidence

Local backend, run on 2026-11-25 with PyIceberg 0.12.0, PyArrow 26.0.1 and ClickHouse 26.8.46.6, on the laptop and in the toolbox container on the shared lake mount. Details are in `docs/reports/p0-spikes.md`.

| Claim | Test | Result |
|---|---|---|
| One factory, same table layout | `test_sk1_pyiceberg.py::test_q1_…` | Pass |
| An hour is replaced alone, in one commit, by whole-file delete | `test_q2_hour_overwrite_replaces_only_that_hour` | Pass: snapshots `delete`, `append` |
| A day is replaced alone and compacted | `test_q3_…` | Pass: three files became one |
| Reruns are idempotent; empty input empties the unit; a failed audit commits nothing | `test_q4_…` (four tests) | Pass |
| A day can be written with bounded memory, atomically | `test_q5b_…` | Pass: one commit, two snapshots, one file |
| Audit on a branch, then publish | `test_q6_…` | Pass |
| Expiry keeps the newest five commits whole and removes no file | `test_q7_…` | Pass |
| ClickHouse reads exactly the pinned metadata file, and the pin holds | `test_sk2_pinned_read.py::test_q1_…`, `test_q2_…` | Pass |
| A view over a pinned table survives re-pinning | `test_q5_…` | Pass |
| ClickHouse reads a day written with `add_files` | `test_day_written_with_add_files_reads_like_any_other` | Pass |
| A filter on `slot_date` reads only that partition's files | `test_q8_slot_date_filter_reads_only_that_partition` | Pass: 3 of 4 files pruned by partition |
| The whole path works end to end | `make demo`, `tests/e2e/test_demo.py` | Pass: 4,023 rows in silver, in the pinned table, in the mart and in the chart |

**Not run:** SK1 question Q8 (the same tests on Glue and S3) and SK2 question Q6 (ClickHouse reading S3 with short-lived credentials). No AWS account was connected during week 1's first run. Both are parametrised in the suite and run with `GBMI_TEST_AWS=1`.

## Status

**Provisional.** Decisions 1 to 8 are proven on the local backend. The record becomes *accepted* when Q8 and Q6 pass on AWS; if either fails, the affected decision is reopened (week 1 plan §6).
