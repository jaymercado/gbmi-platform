# P0 memory report

The blueprint's memory budget (§9.1) is a table of estimates, each marked "to be measured in P0". This report puts measured values beside them, for the laptop only. Its conclusion is limited to what was measured (week 1 plan, WP-10).

| | |
|---|---|
| Measured on | 2026-11-25, MacBook Pro M2 Pro, 16 GB; Docker Desktop 4.42.0, engine 28.2.2 |
| Docker VM | **7.65 GiB** — the 9 GB of blueprint §9.1 had not been set yet. Nothing below came near that ceiling |
| Also in the VM | Eight containers of an unrelated local development stack ran throughout. They are in none of the figures; they used 0.3 GB when checked after the measurements |
| Stack | Airflow 3.4.0 (LocalExecutor, `parallelism=4`, API server with two workers), PostgreSQL 17.12, ClickHouse 26.8.46.6, Metabase 0.65.7.1 |
| Tool | `benchmarks/p0_memory_probe/docker_stats.py`; raw results in `benchmarks/p0_memory_probe/results/` |

**How to read the numbers.** Sizes are binary: 1 GB is 1,024 MiB. *Working set* is what `docker stats` reports: memory in use without the inactive page cache. *Container peak* is the cgroup's `memory.peak`, the high-water mark since the container started; it includes page cache, so it reaches the limit of any container that reads many files and says little by itself. *Peak RSS* is the largest resident size of one process.

## Budget against measurement

All values for the laptop. The estimate column is blueprint §9.1.

| Component | Estimate | Idle | Peak during `make demo` | GKG day probe |
|---|---|---|---|---|
| Airflow, four processes | 2.5 GB | 1.4 GB | 1.8 GB, with the tracer's task processes | — |
| PostgreSQL | 0.5 GB | 0.05 GB; 0.2 GB with Metabase's application data | 0.2 GB | — |
| ClickHouse | 2.5 GB | 0.4–0.5 GB | 0.4–0.6 GB | 0.5 GB while aggregating; largest query 0.13 GB |
| Task processes | 2.0 GB | — | Inside the scheduler container, whose peak was 0.7 GB | **0.5 GB (535 MiB) peak RSS** for a whole GKG day |
| Metabase, on demand | 1.2 GB | 1.1 GB | 1.1 GB | — |
| **Total** | **7.5 GB core; 8.7 GB with BI** | **1.9 GB core; 3.0 GB with BI** | **3.8 GB with BI** | 2.3 GB core |

The stack uses about a quarter of its core budget when idle and about half of the whole budget during the demo. The estimates were ceilings for a loaded system; the loaded system — thirty days of three feeds, the large marts — does not exist yet.

## Per container

Working set in MiB; the container peak follows in brackets.

| Container | Limit | Idle, core only | Idle, with BI | `make demo` (maximum) | GKG build alone | GKG build with aggregation |
|---|---|---|---|---|---|---|
| `airflow-apiserver` | 1,000 | 635 (689) | 634 (687) | 752 (832) | 634 | 634 |
| `airflow-scheduler` | 2,600 | 352 (393) | 316 (354) | 661 (747) | 579 (1,174) | 596 (1,174) |
| `airflow-dag-processor` | 500 | 209 (273) | 224 (396) | 224 (396) | 224 | 238 |
| `airflow-triggerer` | 400 | 247 (289) | 244 (282) | 244 (282) | 244 | 244 |
| `postgres` | 512 | 50 (62) | 186 (289) | 220 (324) | 52 | 50 |
| `clickhouse` | 2,500 | 414 (435) | 368 (373) | 400 (455) | 452 | 518 (682) |
| `metabase` | 1,536 | — | 1,103 | 1,151 | — | — |
| **Sum** | | **1,908** | **3,074** | **3,927** | 2,196 | 2,353 |

- *Idle* is the median of five samples taken after the stack had been idle, ten minutes for the core and three with BI. The core idle column was measured with the earlier limits of 800 MiB for the API server and 2,800 MiB for the scheduler; idle use does not depend on them.
- *`make demo`* is the maximum of samples taken every two seconds across a whole run from a fresh start: two runs of the tracer DAG, the Metabase provisioning and the end-to-end tests. Its sum includes two short-lived containers (the test toolbox, 187 MiB, and `airflow-init`, 88 MiB).
- The two probe columns were measured before the limits of the API server and the scheduler were rebalanced (they were 800 and 2,800 MiB then).
- The unconstrained idle measurement, with every limit removed, gave the same picture: 1,933 MiB in total. Only ClickHouse used more (479 MiB instead of 414), because it sizes its caches from the memory it sees.

## What the measurements changed

| Finding | Evidence | Change |
|---|---|---|
| The reference health check costs more than the service it checks. `airflow jobs check` starts a Python process that adds 115–150 MiB for about 1.4 seconds, every 30 seconds | With it, the triggerer peaked at 397 MiB against a 400 MiB limit and the DAG processor at 365. Sampled every second, both jumped by that amount exactly when the check ran (`idle-*-jobs-check.json`) | The DAG processor and the triggerer read their heartbeat from the API server's `/api/v2/monitor/health`. Peaks fell to 289 and 273 MiB |
| The API server needs more than its share | With two workers it idles at 635 MiB and reached a container peak of 778 MiB of 800 during the demo | Limit raised from 0.8 to 1.0 GB and the scheduler's lowered from 2.8 to 2.6 GB. The core total stays 7.5 GB. The peak is now 832 of 1,000 MiB |
| Metabase does not fit 1.2 GB | With an 800 MiB heap it sat at the limit. With a 640 MiB heap it still held 1,108 MiB of anonymous memory, and the kernel had to reclaim at the limit 3,100 times in a few minutes. The JVM needs about 470 MiB beyond its heap | Heap 640 MiB, limit 1.5 GB. Anonymous memory is then 1,139 MiB, which leaves about 400 MiB. The ceilings now add up to 9.0 GB with BI |
| The scheduler's allocation is mostly unused so far | Its container peak was 747 MiB in the demo and 1,174 MiB in the GKG probe, most of that page cache | None. It also hosts the task processes, and their real load arrives in phase P1 |

## GKG day probe

The largest job the routine path will have is the daily finalisation of the Global Knowledge Graph (blueprint §8.4). The probe builds one closed day — 2026-11-24 — inside the scheduler container, where a task process would run, with the daily pattern chosen in spike SK1: each file is split by DuckDB (memory limit 1 GB, spill directory set), written as one row group, and the day is committed with `add_files`. Every column stays text and the GCAM column is dropped. It is a probe, not a parser: nothing is typed, derived or audited.

| | |
|---|---|
| Input | 96 files, all 96 slots listed; 469 MB compressed, 1,455 MB raw; 111,522 rows, every row with 27 columns |
| Landing | 24.7 seconds through the pipeline's land step, each file verified against the master list |
| Build alone | **6.9 seconds**; peak RSS of the process **535 MiB**; 7.3 CPU-seconds; one commit (`append`) |
| Build while ClickHouse aggregates | 7.4 seconds; peak RSS 523 MiB; one commit (`delete`, `append`) |
| Output | One Parquet file of 103 MB — 14 times smaller than the raw text, which is the effect of dropping GCAM and of ZSTD |
| ClickHouse, at the same time | 117 aggregations over the pinned table in 14.9 seconds (a count, the top organisations, the top themes); slowest 0.26 seconds; largest query 128 MiB; working set 518 MiB |

The blueprint estimated silver GKG at 2.5–4 GB a month. One day at 103 MB extrapolates to about 3.1 GB, inside that range. This is one day and untyped data, so it is an indication, not a measurement of the month.

## Conclusions

**1. Does a daily silver build fit the 2 GB task budget?** For what the probe does, yes, with a wide margin: a 535 MiB peak for a whole GKG day, about a quarter of the budget. The real GKG parser of phase P1 does more per row — it extracts titles, splits tone, explodes organisations and deduplicates — and has to be measured when it exists. The streaming pattern keeps memory proportional to one file, not to the day, so the margin should hold.

**2. Does anything measured contradict the 8 GB host (risk R4)?** One line does, and the total does not.

- The host budget gives Metabase 1.0 GB. It needs about 1.1 GB with a 640 MiB heap, so on the host it needs at least 1.3 GB or a smaller heap.
- Every other line is far below its estimate: Airflow, PostgreSQL and ClickHouse together peaked at about 2.6 GB against the 4.4 GB the host budget gives them.

Nothing measured so far argues for a larger host. That is a statement about memory under a very small load, and nothing more.

**3. Not measured.**

| Open | Phase |
|---|---|
| The AWS host column of blueprint §9.1 — memory and, more important, CPU. An M2 Pro is several times faster than a t4g.large, and the host's burstable CPU credits were not exercised at all | P2 |
| The real GKG parser, and a silver build running beside the hourly jobs | P1 |
| The large marts and their partition replacement under realistic volume | P3 |
| The same measurements with Docker Desktop at the intended 9 GB | Next run |

## Reproducing the measurements

```bash
# Idle: ten idle minutes, then five samples 30 seconds apart, median per container
make up            # or: make up LIMITS=off
uv run python benchmarks/p0_memory_probe/docker_stats.py sample --label idle-limits \
    --out benchmarks/p0_memory_probe/results/idle-limits.json

# Under load: sample every two seconds while a command runs
uv run python benchmarks/p0_memory_probe/docker_stats.py watch --label tracer-demo \
    --out benchmarks/p0_memory_probe/results/tracer-demo.json -- make demo

# The GKG probe (downloads one day of GKG, about 0.47 GB)
make up && bash benchmarks/p0_memory_probe/run_probe.sh 2026-11-24
```
