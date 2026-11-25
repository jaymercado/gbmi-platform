# ADR-006 — Incremental contract: partition replacement

| | |
|---|---|
| **Date** | 2026-11-25 |
| **Status** | **Provisional** (draft) — the materialisation is decided and tested; watermarks and rebuilt aggregates are added in phase P3 |
| **Sources** | Blueprint §5.6, §7.7; week 1 plan WP-08 |
| **Related** | TD-08 (blueprint §6.2); risk R2 |

## Context

Large facts cannot be rebuilt in full every hour. The blueprint's contract for them is: the orchestrating task decides which partitions are stale, dbt builds exactly those partitions, and each one is replaced as a whole, atomically (blueprint §7.7). `delete+insert` is excluded because it is two non-atomic steps, and `append` because it duplicates on rerun.

The blueprint preferred the dbt-clickhouse adapter's `insert_overwrite` strategy, which is marked experimental, and named a fallback in case it failed the build-twice test of phase P0: a small custom materialisation that performs the same steps explicitly.

## Decision

Partition-replaced models use the custom materialisation **`partition_replace`** (`dbt/macros/materializations/partition_replace.sql`).

1. The requested partitions reach dbt as the variable `partitions`, a list of partition values such as `[202611]` or `['2026-11-25']`.
2. A model restricts its input with `requested_partitions_filter(<partition expression>)`.
3. The materialisation builds the model into a staging table with the target's structure, refuses to continue if that table holds rows outside the requested partitions, and then issues `ALTER TABLE target REPLACE PARTITION ID … FROM staging` for every partition that the staging table holds **and** every requested partition that exists in the target.
4. The first build and `--full-refresh` build the whole table; a full refresh is swapped in with an atomic table exchange.

## Alternatives considered

| Alternative | Why it was not chosen |
|---|---|
| The adapter's `insert_overwrite` strategy (dbt-clickhouse 1.10.4) | It replaces only the partitions present in the new data. A partition whose rebuild returns no rows keeps its old rows — test T4. In this platform that case is real: a slot that is re-parsed and quarantined, or a correction that removes rows, must empty its partition |
| `delete+insert` | Two non-atomic steps; readers can see a partition half-deleted |
| `append` | Duplicates rows on every rerun |
| Full rebuild with an atomic table exchange for every model | Correct and simple, and still the default for small tables (blueprint §7.6). Too slow for facts that grow by millions of rows a month |

## Consequences

- **Easier:** a rerun of any partition is idempotent and an emptied input empties its partition. The same materialisation serves hourly and daily builds.
- **Harder:** the project owns about 150 lines of Jinja that depend on the adapter's internal macros (`get_create_table_as_sql`, `exchange_tables_atomic`). An adapter upgrade has to be checked against the SK3 suite, which is why the `dbt` workflow is set to run that suite on every pull request.
- **Accepted:** a run that fails between building the staging table and replacing the partitions leaves that staging table behind. It does no harm — the next run uses a new name — and removing such tables is planned for phase P4. Replacing several partitions is not atomic as a whole; a rerun repairs a partial replacement.
- **Watch:** the stock strategy stays in the suite as a strict expected failure. If a later adapter release makes T4 pass, the suite fails and this decision is revisited.

## Evidence

`tests/integration/dbt/test_sk3_partition_replace.py`, run against ClickHouse 26.8.46.6 with dbt-core 1.12.7 and dbt-clickhouse 1.10.4 on 2026-11-25, for a model partitioned by day and one partitioned by month:

| Test | `insert_overwrite` | `partition_replace` |
|---|---|---|
| T1 first build | Pass | Pass |
| T2 identical rebuild gives the same count and checksum per partition | Pass | Pass |
| T3 only the requested partition changes | Pass | Pass |
| T4 a partition whose input became empty ends empty | **Fail** | Pass |
| T5 a failed replace leaves the target unchanged | Not run | Pass |
| T6 a full refresh equals the incremental result | Not run | Pass |

Details are in `docs/reports/p0-spikes.md`, section SK3.

## Status

**Provisional.** This is the draft that blueprint §16.4 asks for when a spike takes its fallback. The materialisation is decided on the evidence above. The record becomes *accepted* in phase P3, when the watermark table and the rule for aggregates that are rebuilt in full (blueprint §7.7, items 2, 5 and 7) are implemented and tested.
