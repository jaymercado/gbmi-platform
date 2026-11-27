# P1 status report — GDELT at production quality

| | |
|---|---|
| Phase | P1 — GDELT at production quality (blueprint §11.2), as specified by the week 2 plan |
| State | **In progress.** The report grows with each work package. The verdict, the exit gate with evidence, the numbers of the backfill, the open items and the hand-off follow in WP-12 |
| Run | Started on 2026-11-26 at 16:09 UTC, on one laptop: MacBook Pro M2 Pro, 16 GB, Docker Desktop with a virtual machine of 7.65 GiB and 12 CPUs. Claude, the AI model which works in the repository, carries out the work packages. Jay gave the go-ahead for the Git work of the week (week 2 plan, §3.5) |

## Work packages

The order follows change C1 of the week 2 plan (§3.5). Every time is UTC. The working time of a work package excludes the waits for the GitHub checks (change C9).

| WP | Subject | Start | End | Pull request | State |
|---|---|---|---|---|---|
| 01 | Start: checks, pins, plan in the repository | 2026-11-26 16:09 | 2026-11-26 16:28 | [#4](https://github.com/jaymercado/gbmi-platform/pull/4) | Done. Working time about 10 minutes; the GitHub checks took about 10 minutes |
| 02 | Operational database: migrations, the schema `ops`, two stacks | 2026-11-26 16:29 | 2026-11-26 16:58 | [#5](https://github.com/jaymercado/gbmi-platform/pull/5) | Done. Working time about 20 minutes; the GitHub checks took about 10 minutes |
| 03 | Ledger: state machine, calendar, operator commands | 2026-11-26 16:48 | 2026-11-26 17:12 | [#6](https://github.com/jaymercado/gbmi-platform/pull/6) | Done. Working time about 12 minutes; the GitHub checks took about 11 minutes |
| 04 | Ingestion: fixtures, index, verified download, gate G1, reconciliation | 2026-11-26 16:59 | 2026-11-26 17:32 | [#7](https://github.com/jaymercado/gbmi-platform/pull/7) | Done. Working time about 21 minutes; the GitHub checks took about 12 minutes |
| 05 | Parsers: Mentions and GKG, text functions, gate G2, contracts | 2026-11-26 17:21 | 2026-11-26 17:50 | [#8](https://github.com/jaymercado/gbmi-platform/pull/8) | Done. Working time about 18 minutes; the GitHub checks took about 11 minutes |
| 06 | Silver build: units, gate G3, partition versions | 2026-11-26 17:38 | 2026-11-26 18:32 | [#9](https://github.com/jaymercado/gbmi-platform/pull/9) | Done. Working time about 43 minutes; the GitHub checks took about 11 minutes |
| 07 | DAGs, DAG tests, tracer on the new path | 2026-11-26 18:21 | 2026-11-26 18:59 | [#10](https://github.com/jaymercado/gbmi-platform/pull/10) | Done. Working time about 27 minutes; the GitHub checks took about 11 minutes |
| 08 | Operational models in dbt | 2026-11-26 18:48 | 2026-11-26 19:12 | [#11](https://github.com/jaymercado/gbmi-platform/pull/11) | Done. Working time about 15 minutes of the time box of 90 minutes; cut 1 is not applied. The GitHub checks took about 9 minutes |
| 11 | Drills DR2, DR4, DR8 and two runbooks (before WP-09: change C1) | 2026-11-26 19:02 | 2026-11-26 19:25 | [#12](https://github.com/jaymercado/gbmi-platform/pull/12) | Done. Working time about 11 minutes; the GitHub checks took about 12 minutes |
| — | Review of the code before the backfill, and the repairs (change C6) | 2026-11-26 19:10 | 2026-11-26 20:12 | [#13](https://github.com/jaymercado/gbmi-platform/pull/13) | Done. The review agent worked from 19:10 to 19:46. The repairs and the local checks ended at 20:00; the GitHub checks took about 12 minutes |
| 09 | Thirty-day backfill and live run | 2026-11-26 20:14 | 2026-11-27 17:57 | [#14](https://github.com/jaymercado/gbmi-platform/pull/14) | Done. The drain, the backfill and the evidence took 33 minutes (20:14 to 20:47). The live run took 20 hours and 55 minutes of elapsed time, with four sleeps of the laptop. Working time about 90 minutes, an estimate: the work was mixed with the preparation of WP-10 during the live run. The GitHub checks took about 10 minutes |
| 10 | Source profile and benchmarks B1, B2, B4 | 2026-11-26 20:18 | | [#15](https://github.com/jaymercado/gbmi-platform/pull/15) | The pull request is open. The scripts, the source profile and benchmark B4 were done during the live run of WP-09, because the three only read files and tables. Benchmarks B1 and B2 and the comparison for D17 ran after the live run, on 2026-11-27 from 17:46 to 18:21, with the three DAGs paused. Working time about 2.5 hours, an estimate; 3.5 hours were planned. No part of the work package was cut. The stretch item of change C8 was taken |

## Version pins

Checked on 2026-11-26 between 16:10 and 16:13 UTC, the start date, against PyPI, Docker Hub, the GitHub container registry, the GitHub releases and the Terraform registry. Rule of the week 2 plan (WP-01): a newer patch release of the same minor version is taken; any other change stays on the pin.

| Component | Pin of phase P0 | Found on 2026-11-26 | Pin of phase P1 |
|---|---|---|---|
| Apache Airflow; task SDK | 3.4.0; 1.4.0 | Newest releases; the digest of the image is unchanged | Unchanged |
| astronomer-cosmos | 1.16.0 | Newest release | Unchanged |
| DuckDB | 2.0.1 | Newest release | Unchanged |
| PyIceberg; PyArrow; SQLAlchemy | 0.12.0; 26.0.1; 2.1.9 | Newest releases | Unchanged |
| dbt Core; dbt-clickhouse; sqlfluff | 1.12.7; 1.10.4; 4.5.0 | Newest releases | Unchanged |
| ClickHouse | 26.8.46.6 (image tag `26.8`, by digest) | **26.8.47.5** (2026-11-25): the tag `26.8` moved to a newer patch release | **26.8.47.5**: the tag `26.8` by the new digest, in `compose/compose.yaml` and in the workflow `dbt` |
| PostgreSQL | 17.12 (image tag `17`, by digest) | 17.12 is the newest release of version 17; the digest of the tag is unchanged | Unchanged |
| Metabase | 0.65.7.1 | **0.65.7.2** (2026-11-25): a patch release | **0.65.7.2**, by digest |
| Terraform; AWS provider | 1.17.2; 6.74.0 | Newest releases | Unchanged |
| tflint; AWS rules of tflint; checkov | 0.66.0; 0.50.0; 3.4.11 | Newest releases | Unchanged |
| Python | CPython 3.12.16, managed by uv | The newest 3.12 release which uv 0.13.10 offers | Unchanged |
| uv; ruff; mypy; pytest; pytest-cov; pre-commit | 0.13.10; 0.17.6; 2.4.1; 9.2.0; 7.1.0; 4.7.0 | Newest releases; the digest of the uv image is unchanged | Unchanged |
| typer | 0.29.2 | **0.29.3** (2026-11-26): a patch release | **0.29.3** |
| httpx | 0.28.1 | **0.29.0** (2026-11-26): a new minor version, not a patch release | **0.28.1.** The lock file holds the pin. An upgrade needs a decision record (blueprint §6.1, risk R7) |
| Other libraries | clickhouse-connect 1.13.0, psycopg 3.3.9, pydantic 2.14.4, pydantic-settings 2.16.3, tenacity 9.2.1, structlog 26.1.0, tzdata 2026.6 | Newest releases | Unchanged |
| hypothesis | Not installed | 6.170.17 (2026-11-20) | **6.170.17**, new in the development group (`hypothesis>=6.170,<7`), with sortedcontainers 2.4.0 |
| GitHub Actions | checkout 7.0.1, setup-uv 10.4.0, setup-buildx 4.4.1, build-push 7.4.0, setup-terraform 4.0.1, setup-tflint 6.3.2; gitleaks 8.30.1 | Newest releases | Unchanged |

## Review of the plan before the start

On 2026-11-26, before the first work package, one independent review agent (a second instance of the AI model, with read-only access) compared the week 2 plan with the repository and with the installed tools. The review reported sixteen points. The table lists each point and the work package which handles the point. A point marked "open" belongs to a later work package.

| # | Point | Handled |
|---|---|---|
| R1 | `make demo` could run on the working stack, because the Makefile fixed the Compose command when the file was read. The shortened GKG fixtures would then become loaded slots of the real ledger | WP-02: `make demo` and `make e2e` call make again with `STACK=gbmi-demo`; a target which starts containers stops while the other stack runs; `tests/unit/test_make_stacks.py` holds the rule |
| R2 | `compose/smoke.sh` built a Compose command without a project, and the Makefile created only `data/lake` | WP-02: the Makefile exports `STACK` and the lake directory of the stack, and creates the directory before a container starts |
| R3 | The migration runner refuses a changed file, so an edit of an applied file fails only where the file is applied: on the laptop | WP-02: columns which a unit can leave empty are nullable from the first file on; every later change is a new file; the working stack received the migrations as the last step |
| R4 | `--cov-fail-under` would measure the whole package; pytest fails for a test path which does not exist | WP-02: the gate is a second coverage report over three packages; `tests/ledger` joined `make test`. WP-05: `tests/contract` joined |
| R5 | `HttpFetcher.fetch` retries HTTP 429 five times, so an ingest run never sees the first 429 | WP-04: a file request is not repeated after HTTP 429; the run goes on with one download at a time and honours `Retry-After`. The two list requests still wait out HTTP 429 |
| R6 | DuckDB 2.0.1 returns a list with one empty string for an empty field | WP-05: the parsers filter empty strings; an empty GKG field gives an empty list. 2,100 of 2,349 real records end `V1THEMES` with a semicolon |
| R7 | PyIceberg 0.12.0 already retries a failed commit four times and raises a second exception type for a conflict on the same unit | WP-06: confirmed in the source code of PyIceberg 0.12.0 and measured with two writers (`tests/integration/lake/test_commit_conflicts.py`). `commit_with_retry` loads the table again for each attempt and catches both exception types (choice 35). The commit result reads the snapshot before the commit from the commit (choice 36) |
| R8 | A day build leaves the quarantine files of the hour units of the same day | WP-06: a day build removes the quarantine files of the hour units of the day. Open for WP-09: the quarantine report takes the counts from the accounting columns of the ledger |
| R9 | The DAG tests need the Airflow interpreter in the toolbox, a bundle-aware loader for the import of `common`, and a subprocess assertion which allows `file` and `uname` | WP-07: `make test-dags` runs `tests/dags` with `/home/airflow/.local/bin/python -m unittest` in the toolbox. The tests load the DAG files with `BundleDagBag`, require an empty list of import errors, and require: no command which the import starts names dbt |
| R10 | A DAG run triggered through the REST API without a logical date has no logical date in the templates | WP-07: the templates read `dag_run.run_after` and `dag_run.logical_date`, which every run has as a field (choice 48). The end-to-end test runs `gdelt_silver_hourly` without a logical date and `gdelt_silver_daily` with one |
| R11 | The second run of the tracer has to rebuild the hour, because the end-to-end test expects a new snapshot | WP-07: the tracer builds the hour of the parameter with `build_unit` for the three feeds. A second run gives a new snapshot (`delete`, `append`) and the same content checksum for each of the four tables |
| R12 | ClickHouse 26.8 rejects a query parameter in `CREATE NAMED COLLECTION` | WP-08: `clickhouse-init` writes the statements in a here-document, `CREATE NAMED COLLECTION IF NOT EXISTS` and then `ALTER NAMED COLLECTION`. The service stops when the password of the reader is not hexadecimal (choice 60) |
| R13 | The entrypoint of the ClickHouse image writes a file in `users.d` which replaces the user `default` | WP-08: the mounted file is `users.d/gbmi.xml`, which sorts after `default-user.xml`. Checked in the running container: `CHECK GRANT CREATE NAMED COLLECTION ON *` returns 1 |
| R14 | A read-only PostgreSQL role needs `CONNECT` on the database `gbmi`; a one-shot service on the PostgreSQL image needs a `tmpfs` | WP-08: the role file grants `CONNECT`; the service `postgres-roles` has a `tmpfs` for the data directory, and the smoke check "no service uses an anonymous volume" passes |
| R15 | No required workflow builds the operational dbt models | WP-08: `make demo` runs `dbt build --select +ops_slot_status_daily`, so the workflow `e2e` builds and tests the two models. The password test requires the compiled model before the search |
| R16 | `uv lock` alone keeps typer 0.29.2; hypothesis writes `.hypothesis/`; the aggregation script of phase P0 reads a table which the probe removed | WP-01 (typer, `.hypothesis/`); WP-10: a new script, `benchmarks/b2_transform/clickhouse_load.py`, runs the three aggregations on the real GKG silver tables. The script of phase P0 stays as the record of the probe of phase P0 |

## Review of the code before the backfill

Change C6 of the week 2 plan. On 2026-11-26, from 19:10 to 19:46 UTC, one independent review agent (a second instance of the AI model, with read-only duty and no network access) read the code of the ledger, the ingestion, the parsers, the build and the three DAGs on `main` against the week 2 plan and the blueprint. The agent ran fourteen separate experiments in a scratch folder. One experiment built a synthetic GKG day of real size from three real GKG files.

**Result.** The review found no defect which puts wrong, doubled or lost rows into silver. The table lists every finding and what was done.

| # | Finding | Verdict of the review | Handling |
|---|---|---|---|
| V1 | A start of WP-09 after 10:49 UTC leaves the day before the start day without a day build: the first scheduled run of `gdelt_silver_daily` builds the start day | Suspected, about 90% sure | Known before the review as change C4 of the execution plan: one more backfill for the single day S−1 follows the 30-day backfill |
| V2 | The working stack runs the code of the working tree (`src` and `dags` are bind mounts), and the column `_pipeline_version` holds the package version whatever commit built the row | Facts read from two files | Rule for WP-09: no file under `src` and `dags` changes between the start of the backfill and the end of the live run. The commit of the working tree is recorded at the start and at the end. The content checksum includes `_pipeline_version`, so the value has to stay the same for the rerun proof. The Git commit as the version is work of the runtime image (phase P2) |
| V3 | The rules of the trailing seven days do not reach an older day. (a) A slot which is verified after the day build of a day older than seven days is never built. (b) A listed file which GDELT does not serve is confirmed automatically only while the slot is younger than seven days | Confirmed by two experiments | No change of code before the backfill. WP-09 checks: no slot of the window is `listed` or `quarantined` before the builds, and no slot is `verified` after the builds. Both runbooks name the limit and the command to use. The repair queue of gate G6 (phase P4) closes the gap for good |
| V4 | An ingest run before `gbmi gdelt enqueue` leaves days without ledger rows between the enqueued range and the first older row, and no report names the days | Confirmed by an experiment; the order of the plan (enqueue first) is safe | WP-09 checks before the enqueue: the table `ops.source_slot` of the working stack holds no row. The runbook for silver builds has the rule |
| V5 | Gate G1 did not recognise a zip whose compressed stream is broken while size and MD5 equal the file list: the slot stayed `listed` for ever, with no failed attempt on record | Confirmed by an experiment; the input is rare | Repaired: every way in which a zip cannot be read is a failed verification, so the slot is fetched again and quarantined after three attempts. One new test |
| V6 | A GKG word count above 2,147,483,647 stopped the build of the unit with a conversion error | Confirmed; not reachable with real articles | Repaired: gate G2 rejects the row with a reason. While writing the test, one more case showed: DuckDB reads a text such as `1e999` as infinity. Gate G2 now rejects a number which is not finite, for every number column of the three feeds. Four new test cases |
| V7 | A run of `gdelt_silver_daily` which is started by hand for the current day makes `gdelt_silver_hourly` build the whole day again every 15 minutes | Suspected; only a run from the Airflow screen can do so | The runbook for silver builds has the warning |

Three facts of the review for the run, which are no defects:

- The local stack ran with 0 task retries, so one transient error would fail a day of the backfill. The Makefile now sets 3 retries for the working stack (the value of the blueprint, §8.1) and 0 for the demo stack and CI.
- Twenty containers of two other projects on the laptop used about 2.1 GiB of the 7.65 GiB of the Docker machine at 19:40 UTC. The memory report of phase P0 assumed 0.3 GB. Week 2 does not touch the containers. WP-09 starts the working stack without Metabase and records the memory during the backfill.
- A synthetic GKG day of 341 MB (zipped) built in 8.9 seconds with a peak of 835 MB of resident memory, and a day of 682 MB in 16.2 seconds with about 1.29 GB, on macOS outside a container. The largest real day of the window has 541 MB (finding F4 of the week 2 plan). The limit of the scheduler container is 2.6 GB.

The review did not reach: Airflow at run time (the scheduler, `airflow backfill create`, the order in the pool), the live GDELT servers, real Events and Mentions files beyond the fixture hour, dbt and ClickHouse.

## The 30-day backfill (WP-09)

Every time is UTC. S, the UTC date of the start of WP-09, is 2026-11-26. The window is the 30 days from 2026-10-26 (S−31) to 2026-11-24 (S−2): 8,640 expected files, 96 slots of 15 minutes for each of three feeds and each day.

### Start

| Step | Time | Result |
|---|---|---|
| Start of WP-09 | 20:14 | The demo stack is stopped. The working stack runs with six containers, without Metabase, and `make smoke` passes with 15 checks. The command `caffeinate -is` keeps the laptop awake. The working folder is at commit `6ee85dd`: `main` at `1a26fa8` plus four scripts under `benchmarks/`. No file under `src/` and `dags/` changes until the live run ends (finding V2) |
| Check before the enqueue (finding V4) | 20:15 | The table `ops.source_slot` holds 0 rows |
| `gbmi gdelt enqueue --from 2026-10-26 --to 2026-11-24` | 20:15 | 8,640 rows created; the master list names all 8,640 files: 12.90 GB |

### The drain of bronze

The script `benchmarks/b1_ingest/drain_bronze.py` started one run of the DAG `gdelt_ingest` after another through the REST API of Airflow, each with a batch limit of 1,500 files and 4 parallel downloads, until no slot of the window was `listed`. Raw result: `benchmarks/b1_ingest/results/drain-2026-10-26-to-2026-11-24.jsonl`.

| Run | Start | Seconds of the task | Files | MB | MB for each second | Files for each minute |
|---|---|---:|---:|---:|---:|---:|
| 1 | 20:16:19 | 151.9 | 1,500 | 2,587.3 | 17.03 | 592.5 |
| 2 | 20:18:57 | 140.3 | 1,500 | 2,158.9 | 15.39 | 641.4 |
| 3 | 20:21:21 | 135.0 | 1,500 | 2,082.9 | 15.43 | 666.7 |
| 4 | 20:24:11 | 139.4 | 1,500 | 1,959.9 | 14.06 | 645.8 |
| 5 | 20:26:35 | 135.0 | 1,500 | 2,530.4 | 18.74 | 666.7 |
| 6 | 20:28:57 | 112.3 | 1,477 | 2,053.1 | 18.29 | 789.4 |
| All six | 20:16:19 to 20:30:53 | 813.9 | 8,977 | 13,372.6 | 16.43 | 661.8 |

- The drain took 14 minutes and 34 seconds from the first start to the last end. The week 2 plan expected about 12 minutes at 19 MB for each second.
- An ingest run takes the oldest listed files first, and the ledger also held the files of the two days after the window. The six runs landed 8,440 files of the window and 537 newer files. The scheduled run of `gdelt_ingest` at 20:23 landed the other 200 files of the window, between run 3 and run 4.
- Gate G1 rejected no file. No download needed a second attempt, and the servers of GDELT answered no request with HTTP 429.
- Check after the drain (finding V3): each of the 8,640 slots of the window was `verified`. No slot was `listed` or `quarantined`.
- The check for overdue slots wrote six failed results between 20:18 and 20:28, with 7,602 slots in the first result and 1,402 slots in the last one. A slot is overdue when the file is not verified six hours after the slot time, so every file which waited for the download counted. Since 20:30 each result of the check has passed with 0 slots.

### The backfill

1. `gdelt_silver_daily` was unpaused at 20:31. `gdelt_silver_hourly` stayed paused.
2. The dry run printed 30 logical dates, from 2026-10-26 08:25 to 2026-11-24 08:25: the 30 days of the window (change C2).

   ```bash
   airflow backfill create --dag-id gdelt_silver_daily \
     --from-date 2026-10-27T08:25:00+00:00 --to-date 2026-11-25T08:25:00+00:00 \
     --max-active-runs 1 --dry-run
   ```

3. The same command without `--dry-run` created the backfill with the identifier 1 at 20:31:37. Airflow records the backfill as completed at 20:45:53.

| Fact | Value |
|---|---|
| Backfill identifier | 1 (table `backfill` of Airflow: from 2026-10-27 08:25, to 2026-11-25 08:25, one active run at a time, reprocess behaviour `none`) |
| Runs | 30, from `backfill__2026-10-27T08:25:00+00:00` to `backfill__2026-11-25T08:25:00+00:00`. Each run has the state `success`; each task ran once, without a retry |
| Duration | 13 minutes and 54 seconds, from 20:31:37 to 20:45:31. One run took 27.4 seconds in the median (17.4 to 47.4) |
| Build of one day, from `ops.silver_partition` | Events: median 2.6 seconds, highest 2.8. Mentions: median 3.0, highest 3.4. GKG: median 16.9, highest 19.8 |
| Run list | `benchmarks/p1_backfill/results/gdelt_silver_daily-runs.json` |
| Gate G3 | Every check of every build passed. The volume check compares a day with four earlier days of the same weekday in silver. The backfill built the days in date order, so the check ran for two days only (2026-11-23 and 2026-11-24, 8 results, each passed) and was recorded as skipped for the 28 days before (112 results) |
| Memory of the scheduler container (limit 2,600 MiB) | Two samples of `docker stats` during the backfill: 828 MiB and 987 MiB. The highest value of the control group since the start of the container: 2,480 MiB, page cache included. The counters `max`, `oom` and `oom_kill` of the control group stayed at 0 |

**The day before the start day (change C4).** The second backfill for the single day 2026-11-25 was not needed. Eleven seconds after the backfill command, the scheduler created the run `scheduled__2026-11-26T08:25:00+00:00`, and the run built the day 2026-11-25 between 20:31:49 and 20:33:02, beside the first runs of the backfill. The pool `silver_writer` let one build write at a time. The code of Airflow 3.4.0 explains the run: the newest run of a backfill counts as the last automated run of a DAG (`airflow/dag_processing/collection.py`), and the next scheduled run is the later one of two times, the newest trigger time before now and the trigger time after the last automated run (`airflow/timetables/trigger.py`). Both times were 2026-11-26 08:25, which had passed.

### Evidence for the exit gate, rows 1 to 4

**Row 1: every slot of the window is `loaded`, and each silver table holds a day-level version for each day.** The command `gbmi ledger status --from 2026-10-26 --to 2026-11-24` (run at 21:01) returns 90 entries, one for each feed and day, each with 96 slots in the state `loaded`. The script `benchmarks/p1_backfill/evidence.py` (run at 20:46:20; raw result `benchmarks/p1_backfill/results/evidence-2026-10-26-to-2026-11-24.json`) confirms 8,640 of 8,640 calendar slots as `loaded` and finds a day-level partition version for each of the 30 days in each table:

| Silver table | Days with a day-level version | Files | Rows |
|---|---:|---:|---:|
| `gdelt_event` | 30 | 2,880 | 3,102,544 |
| `gdelt_mention` | 30 | 2,880 | 8,235,535 |
| `gdelt_gkg_doc` | 30 | 2,880 | 2,905,938 |
| `gdelt_gkg_org` | 30 | 2,880 | 6,608,297 |

**Row 2: completeness.** The dbt model `ops.ops_slot_status_daily` was built at 20:46:45 in the toolbox (`dbt build --select +ops_slot_status_daily`: 16 of 16 steps passed). The feed `all` counts the three feeds together.

```sql
SELECT feed, count() AS days, min(completeness) AS lowest_completeness,
       countIf(is_complete) AS complete_days, sum(loaded_slots) AS loaded_slots,
       sum(confirmed_missing_slots) AS confirmed_missing_slots, max(_built_at) AS built_at
FROM ops.ops_slot_status_daily
WHERE source = 'gdelt' AND slot_date BETWEEN '2026-10-26' AND '2026-11-24'
GROUP BY feed ORDER BY feed
```

```text
┌─feed─────┬─days─┬─lowest_completeness─┬─complete_days─┬─loaded_slots─┬─confirmed_missing_slots─┬────────────────built_at─┐
│ all      │   30 │                   1 │            30 │         8640 │                       0 │ 2026-11-26 20:46:45.480 │
│ export   │   30 │                   1 │            30 │         2880 │                       0 │ 2026-11-26 20:46:45.480 │
│ gkg      │   30 │                   1 │            30 │         2880 │                       0 │ 2026-11-26 20:46:45.480 │
│ mentions │   30 │                   1 │            30 │         2880 │                       0 │ 2026-11-26 20:46:45.480 │
└──────────┴──────┴─────────────────────┴───────────────┴──────────────┴─────────────────────────┴─────────────────────────┘
```

The completeness is 100% for each of the 30 days; the gate asks for 99.5%. The report of the ledger code (`Ledger.completeness`, in the evidence script) gives the same result.

**Row 3: accounting.** One query over the ledger, run at 21:33 with `make psql`:

```sql
SELECT count(*) AS loaded_files,
       count(*) FILTER (WHERE raw_lines = parsed_rows + quarantined_rows) AS balanced_files,
       sum(raw_lines) AS raw_lines, sum(parsed_rows) AS parsed_rows,
       sum(quarantined_rows) AS quarantined_rows
FROM ops.source_slot
WHERE source = 'gdelt' AND state = 'loaded'
  AND slot_ts >= '2026-10-26 00:00+00' AND slot_ts < '2026-11-25 00:00+00'
```

```text
 loaded_files | balanced_files | raw_lines | parsed_rows | quarantined_rows
--------------+----------------+-----------+-------------+------------------
         8640 |           8640 |  14395666 |    14395666 |                0
```

For each of the 8,640 loaded files, the raw lines equal the parsed rows plus the quarantined rows: the accounting is 100%. The report `docs/reports/p1-quarantine-reconciliation.md` shows the numbers by feed and by day.

**Row 4, the part on real days: a rerun gives equal checksums.** Drill DR8 in the form `make drill DR=8 ARGS="--day <day>"` built the three feeds of a real day again with `gbmi silver build-day` and compared the content checksum of each table with the stored checksum. Both drills passed with 12 checks each, between 20:46 and 20:47. Raw results: `benchmarks/p1_backfill/results/dr8-2026-11-18.json` and `dr8-2026-11-22.json`.

| Table | 2026-11-18 (Wednesday): rows | Checksum, stored and after the rerun | 2026-11-22 (Sunday): rows | Checksum, stored and after the rerun |
|---|---:|---|---:|---|
| `gdelt_event` | 128,905 | `339f708996dde8b1`, equal | 59,805 | `d836ca7b3701b694`, equal |
| `gdelt_mention` | 348,367 | `d4a1733fd2477d9b`, equal | 146,721 | `c1307e258a76af59`, equal |
| `gdelt_gkg_doc` | 118,241 | `6a95d590c580bcf3`, equal | 52,109 | `0affe2685bbcbd07`, equal |
| `gdelt_gkg_org` | 272,214 | `ad08d58c29f05763`, equal | 107,978 | `2db1159631eae87b`, equal |

## The live run (WP-09)

Every time is UTC. The laptop clock is 8 hours ahead.

| Fact | Value |
|---|---|
| Start | 2026-11-26 20:47:52. `gdelt_silver_hourly` was unpaused; `gdelt_ingest` and `gdelt_silver_daily` were unpaused already |
| End | 2026-11-27 17:43:03. The three DAGs were paused (change C7) |
| Length | 20 hours and 55 minutes. The laptop slept four times in between |
| Manual action on the pipeline | None. A watcher script read the state every five minutes and changed nothing |
| Code of the stack | The working folder was at commit `6ee85dd` at the start and at `82d067d` at the end. No file under `src/` and `dags/` differs between the two commits, and no such file was changed in between (finding V2) |
| DAG runs | 54 scheduled runs of `gdelt_ingest`, 62 of `gdelt_silver_hourly` and 1 of `gdelt_silver_daily`. Each DAG run ended with the state `success` |
| Work | 252 files landed (298 MB); 237 units built by the hourly job; 1 day built by the daily job. No file was rejected by gate G1, no unit failed, no slot was quarantined |
| State at the end | 288 of 288 files of 2026-11-26 and 216 of 216 files of 2026-11-27 up to the slot 17:45 are `loaded` |

Raw results, all under `benchmarks/p1_backfill/results/`: `awake-periods.json` (the power log of macOS, read by `awake_periods.py`), `live-run-2026-11-26-to-2026-11-27.json` (the tries of each job from `ops.pipeline_run`, read by `live_run.py`), two latency files (read by `latency.py`) and `gdelt_silver_daily-runs.json` (the 32 runs of the daily DAG).

### The laptop slept four times

The power log gives the cause "Clamshell Sleep" for each of the four sleeps: the lid was closed. In three of the four cases the laptop ran on battery. The first sleep came five seconds before the log shows battery power. The table shows each period in which the laptop was fully awake, and the short wakes of the sleeping laptop in one row. A short wake lasts some seconds, most of the time without a network; the containers run during a short wake.

| Period of the live run | Length | Runs of `gdelt_ingest` | Runs of `gdelt_silver_hourly` | Failed tries | Files landed | Units built |
|---|---|---:|---:|---:|---:|---:|
| 2026-11-26 20:47:52 to 23:14:06, awake | 2 h 26 min | 10 | 10 | 0 | 29 | 91 |
| 2026-11-27 01:34:20 to 02:24:00, awake | 50 min | 6 | 5 | 0 | 38 | 15 |
| 2026-11-27 04:43:09 to 05:38:58, awake | 56 min | 6 | 4 | 1 | 39 | 18 |
| 2026-11-27 08:07:09 to 08:08:25, awake | 1 min | 3 | 2 | 0 | 30 | 0 |
| 2026-11-27 11:26:18 to 17:43:03, awake | **6 h 17 min** | 25 | 25 | 0 | 75 | 75 |
| Short wakes during the four sleeps | | 4 | 16 | 8 | 41 | 38 |
| The whole live run | 20 h 55 min | 54 | 62 | 9 | 252 | 237 |

What the pipeline did without help:

- **The gap after a sleep.** The first ingest run after a wake found the newest file of GDELT more than one slot ahead of the newest listed slot, read the end of the master list, listed the missed files and landed the files: 27 files at 01:38, 30 files at 04:53 and 36 files at 11:18. At 08:08 the daily reconciliation listed 27 missed files, and the ingest task of the same run landed 30 files. The hourly silver job built the waiting hours in the next run. No slot of the two days is missing.
- **Failed tries.** Nine tries of the task `ingest` failed with a connection error ("Name or service not known", and once a cut TLS connection). Eight tries ran in a short wake of the sleeping laptop, and one try ran nine seconds after a full wake. A later try of the same run passed each time: the working stack runs with three retries (review before the backfill).
- **In the first and in the last period every run started on time.** Outside the two periods, 18 tries of the ingest task and 20 tries of the hourly silver job started more than one minute after the scheduled time, which is the mark of a sleep.
- **A file which GDELT listed and did not serve.** The servers of GDELT answered HTTP 404 for the GKG file of the slot 2026-11-26 23:15 at 23:08 and at 23:27, and for the GKG file of the slot 23:30 at 23:27. Both files landed at 01:34, after the wake. How long the servers did not serve the two files is not known, because the laptop slept. The ledger kept both slots as `listed` with the time of the first 404, as the runbook describes.

One observation corrects the execution plan. The plan said: `caffeinate` does not keep a laptop with a closed lid awake. The statement holds on battery. On mains power the laptop stayed awake with a closed lid from about 11:56 to 17:43 under `caffeinate -is`.

### The scheduled daily run

The live run held one scheduled run of `gdelt_silver_daily`: `scheduled__2026-11-27T08:25:00+00:00`. The laptop slept at 08:25, so the run started at 09:08:22, in a short wake, and ended at 09:08:49 with the state `success`. The run built the day 2026-11-26 from the 288 files in bronze:

| Table | Rows of 2026-11-26 | Build |
|---|---:|---|
| `gdelt_event` | 95,311 | 2.5 seconds |
| `gdelt_mention` | 226,211 (230,139 parsed rows, 3,928 duplicates) | 2.9 seconds |
| `gdelt_gkg_doc` | 88,072 | 15.9 seconds, together with the organisations |
| `gdelt_gkg_org` | 195,786 | |

Each table holds a day-level version for the day, and every check of gate G3 passed. The volume check ran, because silver held four earlier Thursdays: the day lies inside the band from 0.5 to 2.0 times the median.

The daily reconciliation was due from 08:00 and ran at 08:08:00, in the one minute in which the laptop was awake: 2,022 files in the master list for the trailing seven days, no absent slot, no difference between bronze and the ledger (`docs/reports/p1-quarantine-reconciliation.md`).

### Latency

The script `benchmarks/p1_backfill/latency.py` reads three times for each file from the ledger. A sample starts with the first slot which GDELT uploaded after the backlog was empty. Each cell gives the median and the 95th percentile.

| Sample | Files | From the upload by GDELT to `listed` | From `listed` to `verified` | From `listed` to the silver commit |
|---|---:|---|---|---|
| First period: the 7 slots from 2026-11-26 21:30 to 23:00 | 21 | 242.6 s / 262.0 s | 0.2 s / 1.5 s | 180.5 s / 182.5 s |
| Last period: the 25 slots from 2026-11-27 11:45 to 17:45 | 75 | 200.1 s / 219.5 s | 0.3 s / 1.8 s | 180.9 s / 186.8 s |

The last period by feed, from `listed` to the silver commit: Events 180.2 s / 183.6 s, Mentions 181.4 s / 186.8 s, GKG 181.2 s / 186.3 s. The longest time of the 75 files was 192.2 seconds.

- **Against the targets of the blueprint.** From `listed` to `verified`: 1.8 seconds at the 95th percentile; the target is 10 minutes (§8.3). From `listed` to the silver commit: 187 seconds at the 95th percentile; the target is 45 minutes (§3.4).
- **The three minutes are the schedule.** The ingest job runs at minute 8 and the silver job at minute 11 (difference D15 and D16 of the week 2 plan). A file is in silver about 6.4 minutes after the upload by GDELT, in the median (200 seconds plus 181 seconds).
- **Limit.** The two samples cover 2.4 hours and 6.3 hours on one laptop. The samples say nothing about a day with a slow source.

### Exit-gate row 7, as measured

Row 7 asks for two facts.

1. *`gdelt_ingest` and `gdelt_silver_hourly` ran on the live source for at least 6 hours in a row without manual action.* The fact holds for the last period: 6 hours and 17 minutes, from 11:26:18 to 17:43:03 on 2026-11-27, with 25 of 25 runs of each of the two DAGs on time and no failed try.
2. *The live run held one scheduled run of `gdelt_silver_daily` (08:25 UTC).* The fact holds: the run of 2026-11-27 started at 09:08 and built 2026-11-26.

The two facts lie in different periods of the live run. The daily run did not happen inside the 6 unbroken hours, and the daily run started 43 minutes late, because the laptop slept at 08:25. The week 2 plan gives the rule for a sleep: the count of 6 hours starts again (§10). A run of 6 unbroken hours which holds 08:25 UTC was not done on the laptop. The host of phase P2 runs the schedules without a lid, and the soak of phase P6 measures the same latency over 14 days.

## Choices made during the run

| # | Work package | Choice | Reason |
|---|---|---|---|
| 1 | WP-01 | The pin check also took ClickHouse 26.8.47.5 and Metabase 0.65.7.2 | The week 2 plan (§4.2) had not compared the image tags with Docker Hub and had not checked Metabase again. Both changes are patch releases, so the rule of WP-01 applies |
| 2 | WP-01 | The status report starts in WP-01 and grows with each pull request. The week 2 plan lists the report under WP-12 | The pin table and the times of each work package are evidence which exists from the first day. WP-12 completes the report |
| 3 | WP-01 | `.gitignore` names the directory `.hypothesis/` | hypothesis stores examples in the directory during a test run |
| 4 | WP-02 | `make reset` for the working stack stops and asks for `CONFIRM=yes`. `make reset STACK=gbmi-demo` asks nothing | The working stack will hold 30 days of real data. The week 2 plan (§9) says: a reset of the working stack needs the agreement of Jay |
| 5 | WP-02 | The migrations hold more rules than the illustrative DDL of the blueprint: the twelve transitions as a check constraint, an append-only trigger on `ops.slot_transition`, the accounting identities on `ops.source_slot` and on `ops.silver_partition`, and the facts which each state needs | The database refuses a state, a transition or an accounting which the rules do not allow, also when the code has a defect. `tests/ledger/test_schema.py` tests each rule |
| 6 | WP-02 | Stored times such as `listed_at`, `verified_at` and `created_at` come from the clock of the database. Rules read the time from the argument `as_of`, and the tables `ops.slot_transition` and `ops.pipeline_run` store `as_of` in a column of its own | The latency numbers of WP-09 need real times. The tests of the rules need a clock which a test controls (week 2 plan, §3.3) |
| 7 | WP-02 | Two columns join the tables of the plan: `entry_point` in `ops.pipeline_run` (the function of the package, also for a run without Airflow) and `detail` (`jsonb`) in `ops.silver_partition` | "The reconciliation is due" is read from the run records (week 2 plan, §3.3). Free detail stays out of new columns |
| 8 | WP-02 | The coverage gate is a second report (`coverage report --include … --fail-under=80`) after the test run | `make test` keeps the coverage table of every module, as in phase P0 |
| 9 | WP-02 | `make reset` removes `dbt/target` only while the other stack is down | Both stacks read the dbt manifest from the same directory |
| 10 | WP-02 | A test found a defect before the first migration was applied: a check constraint let an hour unit without an hour pass, because a comparison with `NULL` is `NULL` | The constraint now names `IS NOT NULL`. Every other constraint was read again for the same trap |
| 11 | WP-03 | Each change of state is one SQL statement: the guarded update and the insert of the log rows | The slot and the log cannot differ, also when a process stops between two statements |
| 12 | WP-03 | A function for one slot raises `StaleStateError` when the slot is not in the assumed state. A function for many slots returns the slots which moved | The ingest job and the reconciliation work on sets; a set call which moves a part of the set is normal there |
| 13 | WP-03 | `gbmi ledger confirm-missing` asks for the option `--state` (`expected`, `listed` or `quarantined`) beside the range and the note | A slot which only waits for its download is not confirmed by accident |
| 14 | WP-03 | `gbmi.core.clock` is the only module which reads the real clock | Week 2 plan, §3.3: every function takes the time as the argument `as_of` |
| 15 | WP-04 | The first line of a cut tail of the master list is dropped. The reader of phase P0 trusted the line | A range request can cut a line inside its size. The rest still looks like a line, with a wrong size (`tests/unit/test_gdelt_index.py`) |
| 16 | WP-04 | A file which fails gate G1 is fetched again in the same run. The third failure quarantines the file and keeps the bytes under `quarantine/<source>/<feed>/dt=…/` | Blueprint §8.3: "fetch again up to three times, then quarantined". A cut transfer is repaired within one run |
| 17 | WP-04 | An ingest run works as if the clock showed `as_of`: a line of `lastupdate.txt` for a later slot waits for a later run | A run with a controlled clock gives the same result on every day (week 2 plan, §3.3) |
| 18 | WP-04 | The reconciliation counts an expected slot as absent only when the master list holds a newer slot | The list has not reached a slot which is newer than the newest slot of the list |
| 19 | WP-04 | After HTTP 429 a run waits at most 600 seconds in total (60 seconds for an answer without `Retry-After`). Slots which are left stay `listed` for the next run | A run every 15 minutes has to end; the ledger keeps the state (week 2 plan, §10) |
| 20 | WP-04 | `ops.dq_result` gets one row for each file of gate G1, one row for each reconciliation (ledger against bronze, recorded under gate G6) and one row for each overdue report | Blueprint §7.3: one row for each execution of a check. An overdue report with one row for each slot and run would write 96 rows a day for one missing file |
| 21 | WP-04 | `gbmi.core.failpoint` and `gbmi.quality.results` arrive in WP-04. The week 2 plan lists both under WP-06 | The failpoint before the bronze move and the results of gate G1 belong to the ingest code |
| 22 | WP-04 | `benchmarks/source_profile/live_ingest_once.py` is the live check of WP-04 as a script: two ingest runs against the GDELT servers in a throwaway database and a scratch lake | The check can be repeated, and benchmark B1 of WP-10 measures the same code path |
| 23 | WP-05 | Gate G2 for GKG is as strict as the week 2 plan says: the record identifier has the form of the codebook, the date is a valid time of 14 digits, the tone holds exactly seven numbers. A profile of three real GKG files (2,349 records of 2026-11-22, 2026-11-25 and 2026-11-26) found no record which breaks a rule | The GKG codebook says: for some special collections the date can be 0. Such a record would go to quarantine with a reason. The backfill of WP-09 shows whether the case occurs |
| 24 | WP-05 | The normalisation of text repeats its one pass until the text stops changing | For a few rare characters one pass is not stable. The rule "a second normalisation changes nothing" has to hold for every text, and a property test with 500 random texts checks the rule |
| 25 | WP-05 | "Letter" and "digit" in the normalisation are the Unicode categories L and Nd | The categories are the same in every engine which uses the Python function |
| 26 | WP-05 | A parser applies the 1% rule for a whole file only when the caller passes `file_threshold`. The build of WP-06 passes 0.01 | The tests of single rows keep their meaning: a test file with one good row and one bad row is not rejected as a whole |
| 27 | WP-05 | The list columns `themes` and `countries` are declared as required in the Iceberg schema | PyIceberg 0.12.0 writes them and ClickHouse 26.8 reads them as `Array(String)` (`tests/integration/lake/test_silver_tables.py`). The fallback of the week 2 plan is not needed |
| 28 | WP-05 | The column meanings of the four contracts were checked against the GDELT Event Codebook V2.0 and the GKG Codebook V2.1 on 2026-11-26 | A contract states facts about the source; five meanings were corrected after the check |
| 29 | WP-05 | PyYAML is a declared dependency of the development group | The contract tests read the YAML files of `docs/contracts` |
| 30 | WP-06 | A failed audit changes no slot. A file which gate G2 rejects as a whole moves to `quarantined` only after the audit of the other files of the unit has passed | The week 2 plan says: a failed audit leaves the ledger unchanged. The next run finds the rejected file again |
| 31 | WP-06 | The parsers count the duplicate rows in the typed table. Before WP-06 the number was the difference between the parsed rows and the output rows | The identity "committed rows = parsed rows − duplicate rows" of gate G3 now compares two measurements. A parser which loses a row or repeats a row fails the audit |
| 32 | WP-06 | Gate G3 has one more check, `quarantine_rows`: the quarantine file of the unit holds one row for each line which the accounting counts as quarantined | Blueprint §3.4: every raw line is accounted for, also the rejected lines |
| 33 | WP-06 | The audit of a day unit reads the key columns back from the staged Parquet file and checks: no key is NULL, the key is unique over the whole day, every row has the date of the unit, and the row total equals the accounting | The audit checks the file which the commit adds, not the rows in memory. The key columns of one day are small, so memory stays near one hour of data |
| 34 | WP-06 | The content checksum of a unit is computed from the rows which go into the commit. For a day unit the checksum is the combination of the checksums of the hours (a sum modulo 2^64) | A day is never held in memory as a whole. The integration tests compare the stored checksum with a scan of the table |
| 35 | WP-06 | `commit_with_retry` tries a commit again up to three times, each time on a freshly loaded table, after `CommitFailedException` and after `ValidationException`. Measured with two writers on PyIceberg 0.12.0 and the SQLite catalog: when the other commit changed another day or another hour, PyIceberg repeats the commit by itself after about 100 ms. When the other commit changed the same unit, PyIceberg raises `ValidationException`, and the second attempt of `commit_with_retry` lands the commit | Review point R7. The last writer wins for a unit: the contract of an overwrite which is filtered on the unit |
| 36 | WP-06 | The commit result takes "the snapshot before the commit" from the parent of the first snapshot of the commit | After a repeat inside PyIceberg, the table object which started the commit is out of date. The rollback target has to be the real parent |
| 37 | WP-06 | A finalised day is pending again in two cases. Case 1, the rule of the week 2 plan: the loaded files of the day are not the files of the newest day build. Case 2, added: an hour unit of the day was built after the newest day build | An operator can build an hour inside a finalised day with `gbmi silver build-hour`. The next hourly run then builds the day again, so a finalised day is always the output of a day build |
| 38 | WP-06 | The first batch with rows creates the staged file of a day. A day without rows leaves no file | An empty unit writes nothing to the lake |
| 39 | WP-06 | A unit without any file is built as an empty unit: no error, and a partition version with 0 rows and 0 files | The ledger says which files exist. Phase P0 raised an error here, because phase P0 had no ledger |
| 40 | WP-06 | A `verified` slot whose file is missing in bronze moves to `quarantined`, like a `verified` slot whose file has another MD5. A copy of a changed file is kept in the quarantine area | An operator can send the slot back to the download with `gbmi ledger requeue`. For a `loaded` slot the build stops with an error, as the week 2 plan says |
| 41 | WP-06 | Gate G2 writes one row to `ops.dq_result` for each build (the largest share of failed rows among the accepted files) and one failed row for each rejected file | One row for each file would mean 288 rows for the day builds of one day |
| 42 | WP-06 | A process which dies before the commit of a day build leaves the staged file in the data directory of the table. No snapshot names the file | No reader sees the file. The removal of unreferenced files is maintenance work of a later phase (blueprint §7.9) |
| 43 | WP-06 | Each line of the file-set hash ends with a line feed | `shasum -a 256` over a text file with the same lines gives the same value |
| 44 | WP-06 | One silver run builds the pending units one after the other. A unit which fails does not stop the later units. The run fails at the end and names each failed unit | One bad file must not block the other feeds and the other hours |
| 45 | WP-06 | No lock keeps two builds of the same unit apart. Two builds at the same time both commit, one after the other, and the newest partition version counts. The rule of choice 37 finds a day which no longer equals a day build | The blueprint gives the serial order to the Airflow pool `silver_writer` (WP-07). The data stays correct without the pool |
| 46 | WP-06 | `gbmi silver build-hour` and `gbmi silver build-day` build the three feeds one after the other, or the feeds named with `--feed`. A feed which fails does not stop the other feeds, and the exit code is 1 | The same rule as choice 44, for the operator |
| 47 | WP-07 | The demo stack gives the three GDELT DAGs no schedule (`GBMI_DAG_SCHEDULES=off`, set by the Makefile for `STACK=gbmi-demo`). The working stack keeps the schedules of the week 2 plan | A run by hand needs an unpaused DAG. An unpaused DAG with a schedule starts runs by the clock, and the default source of `gdelt_ingest` is the live source. The end-to-end test checks the missing schedule before the first unpause. A DAG test checks the schedules of the working stack |
| 48 | WP-07 | The clock of a DAG run is the parameter `as_of`, or `run_after` of the run when the parameter is empty | Every run has `run_after`. A run by hand without a logical date has no logical date (review point R10). For a scheduled run of the two 15-minute DAGs `run_after` equals the logical date |
| 49 | WP-07 | `gdelt_silver_daily` builds the day of the parameter `day`, or the UTC date of the logical date when the parameter is empty. A run with neither fails with one clear message | A scheduled run and a backfill run have a logical date. A run by hand names the day |
| 50 | WP-07 | In `gdelt_ingest` the tasks `ingest` and `report_overdue` also run when `reconcile` failed | The files have to keep landing while the master list is unavailable. The failed task stays visible in Airflow and in `ops.pipeline_run` |
| 51 | WP-07 | In `gdelt_silver_daily` the three builds run one after the other, each one also after a failed build. The task `summary` reads the partition versions of the run from `ops.silver_partition` and fails when a table has no day-level version from the run | One feed which fails must not stop the other feeds, and the run must not count as a success. The summary does not depend on the result which a failed task hands on |
| 52 | WP-07 | Execution timeouts: 60 minutes for `ingest` and for `build_units`, 30 minutes for the build of one feed of one day, 10 minutes for every other task | Estimates **[E]** for the largest planned run: a backfill run with 1,500 files, a silver run with 48 units, one day of the GKG feed. The backfill of WP-09 and benchmark B2 measure the durations |
| 53 | WP-07 | The two silver DAGs have two retries at most (blueprint §8.4), `gdelt_ingest` has three (blueprint §8.1). The local stack sets 0 | A failure shows at once on the laptop and in CI |
| 54 | WP-07 | The tracer creates ledger rows for the twelve slots of the named hour only | A tracer run for one hour must not create the calendar of a whole day in the ledger |
| 55 | WP-07 | `make demo` also runs each of the three GDELT DAGs once on the demo stack: `gdelt_ingest` with a fixed clock, `gdelt_silver_hourly` with the clock of the run, `gdelt_silver_daily` with a logical date. The week 2 plan asks only for the tracer | The DAG tests check the wiring. Templates and the hand-over between the Python environments show only in a real run, and a real run before the 30-day backfill is cheap: about 40 seconds |
| 56 | WP-07 | The demo stack cannot run a backfill: Airflow 3.4.0 refuses a backfill for a DAG without a schedule (`DagNonPeriodicScheduleException`, checked with a dry run) | The dry run of WP-09 on the working stack stays the check of the backfill arguments (change C2) |
| 57 | WP-07 | `tests/e2e/fixture_counts.py` counts the rows of the four silver tables from the fixture zips with the standard library only, and states each rule a second time. The counts: 4,023 events, 7,762 mentions, 240 documents, 541 organisation rows | The week 2 plan asks for counts which share no code with the pipeline |
| 58 | WP-07 | Removed with the entry points of phase P0: `land_export_hour`, the function `land_hour` of `gbmi.sources.gdelt.land`, `build_event_hour`, and the commands `gbmi gdelt land` and `gbmi gdelt build-silver` | The tracer uses the ledger path, and one path is easier to keep correct than two |
| 59 | WP-08 | `ops_slot_status_daily` has the grain source, slot date and feed. The row over all feeds has `feed = 'all'` and 288 calendar slots. The model also sums the three accounting columns of the loaded files | The row over all feeds is the completeness of the blueprint (§8.2). With the accounting sums, exit-gate row 3 can be read from the same model |
| 60 | WP-08 | `clickhouse-init` stops with a message when the password of the role `gbmi_ro` is not hexadecimal | ClickHouse 26.8 takes no query parameter in `CREATE NAMED COLLECTION`, so the password is written into the statement. `make env` generates hexadecimal passwords; a hand-made password with a quote would break the statement |
| 61 | WP-08 | Checked in the running container: ClickHouse 26.8 writes the statements about the named collection to the query log with every value hidden (`password = '[HIDDEN]'`). None of the 42 logged statements held the password | The password of the reader must not appear in a log |
| 62 | WP-08 | The staging model reads PostgreSQL through the macro `ops_table('source_slot')`, which gives `postgresql(pg_ops, schema = 'ops', table = 'source_slot')` | A table function is no relation, so a dbt source cannot describe the table. The macro is the one place which names the collection |
| 63 | WP-08 | The role file sets default privileges: a table which a later migration creates in the schema `ops` as the role `gbmi` is readable for `gbmi_ro` at once | No migration file names the read-only role (week 2 plan §3.3), and a new table must not wait for the next start of the stack |
| 64 | WP-08 | An end-to-end test compares the model with the completeness report of the ledger code (`Ledger.completeness`) for the fixture day, and one test logs in as `gbmi_ro`: reading works, every write and every other database is refused | Two implementations of one definition have to agree. "Reads the schema `ops` and nothing else" is a claim which a test has to hold |
| 65 | WP-08 | The toolbox container receives the password of the role `gbmi_ro` | The end-to-end tests log in as the role and search the dbt artefacts for the password |
| 66 | WP-11 | Each drill works in a throwaway database and in a separate lake directory under `_test/`. Drill DR2 builds the fixture hour a second time in a second such world, undisturbed, as the reference | The week 2 plan (§3.3): no drill writes to the database or the lake of a stack. "A checksum equal to an undisturbed build" needs an undisturbed build |
| 67 | WP-11 | Drill DR2 has a fourth step beside the three of the week 2 plan: a rebuild of a unit which silver holds already is killed before the commit, and the table keeps the snapshot and the content checksum | The first kill of the plan happens on an empty table, where "unchanged" says little |
| 68 | WP-11 | Drill DR8 has a second form, `make drill DR=8 ARGS="--day <day>"`: the drill builds a real day of the stack again and compares the new partition versions with the stored ones. The form adds one partition version for each table | Exit-gate row 4 asks for the rerun proof on two real days of the window. The week 2 plan allows the rebuild of real days on the working stack for the same purpose (§3.3, isolation of tests) |
| 69 | WP-11 | `make psql SQL="…"` runs one SQL statement on the operational database of the stack | The two runbooks read the ledger with SQL, and each command of a runbook has to work as written. The commands of both runbooks were run on the demo stack on 2026-11-26 |
| 70 | WP-11 | `gbmi silver build-day` takes the day as `--day` or as `--date` | The week 2 plan writes `--date` in the verification table (§9) |
| 71 | WP-11 | The runbook for silver builds describes the restore of a bronze file of a loaded slot by hand: download from the address in the ledger, compare the MD5, build again | Phase P1 has no command for the case, and no state leads out of `loaded`. The build refuses a loaded file whose bytes changed |
| 72 | Review | Gate G2 rejects a number which is not finite. Before, a text such as `1e999` passed as infinity | "Nothing is coerced silently" (blueprint §5.7). An infinity in a tone column is no value of the source |
| 73 | Review | The working stack runs with 3 task retries, the demo stack and CI with 0 | Phase P0 set 0 for the local stack: a failure then shows at once. The working stack now runs the schedules on live data, where a transient error has to be tried again. The silver DAGs use 2 of the 3 (blueprint §8.4) |
| 74 | WP-09 | The drain of bronze starts one run of `gdelt_ingest` after another through the REST API of Airflow, each with a batch limit of 1,500 files. The script stops when a run verifies no file | The backfill uses the code path of the routine job (week 2 plan, WP-09, step 3). A run which lands nothing would otherwise repeat without end |
| 75 | WP-09 | No second backfill for the day before the start day (change C4). A scheduled run, which the scheduler created right after the backfill command, built the day 2026-11-25 | The condition of change C4, "when no scheduled run builds day S−1", did not hold. The section on the backfill names the two places in the code of Airflow |
| 76 | WP-09 | The rerun proof on two real days uses drill DR8 in the form `--day` | The drill runs `gbmi silver build-day` for the three feeds and compares the stored checksums, as the verification table of the week 2 plan asks (§9) |
| 77 | WP-09 | A watcher script reads the state of the live run every five minutes: failed runs, failed checks, quarantined slots, the memory events of the scheduler container, the running containers. The script changes nothing and lies outside the repository | "Without manual action" (exit-gate row 7) needs a witness which only reads. A problem during the night has to end the wait at once |
| 78 | WP-09 | The source profile and benchmark B4 ran during the live run, in a separate container, and only read files and tables | The laptop is busy with the live run for about 12 hours. Reading bronze and silver does not change the ledger. Benchmarks B1 and B2 download and rebuild, so both wait for the end of the live run (change C7) |
| 79 | WP-09 | After each sleep of the laptop the count of 6 hours started again. The live run ended after the first period of 6 unbroken hours | The week 2 plan gives the rule (§10). The scheduled daily run lies in the live run and outside the 6 unbroken hours; the section on the live run says so |
| 80 | WP-09 | Two read-only scripts describe the live run: `awake_periods.py` reads the power log of macOS, and `live_run.py` counts the tries of each job from `ops.pipeline_run` | "6 hours in a row" needs a source which is independent of the pipeline: the power log says when the laptop was awake |
| 81 | WP-10 | The benchmark day is 2026-11-04, the largest GKG day of the window (541 MB as zip files). 2026-11-22, a Sunday, is the small day of benchmark B4 | A memory number has to come from the largest day. A median day would flatter the result |
| 82 | WP-10 | The memory fields of the benchmark results are in MiB and carry the unit in the name. Three series were run a second time after the names changed | The control group, `docker stats` and the limits of Compose count in MiB. A field named "megabytes" with a value in MiB was found before the report was written |
| 83 | WP-10 | Each step of the comparison for D17 runs under a small wrapper process | Linux counts the memory which a child holds between fork and exec, a copy of the memory of the parent, in the peak of the child. The first run of the comparison showed the memory of the measuring script, which holds the rows of a day for the checksums. The measuring script of benchmark B2 holds 106 MiB, below every measured peak |
| 84 | WP-10 | The load for "a GKG day build under load" is a new script on the real GKG silver tables (review finding R16) | The script of phase P0 reads a scratch table of text columns, which the probe of phase P0 creates and removes. The silver tables of phase P1 have typed columns and lists, so the three aggregations need other SQL |
| 85 | WP-10 | The stretch item of change C8 was taken: the events mart over the month partitions 202610 and 202611 | The recorded working time of the week is about 8 hours, far below the 45 planned hours |
| 86 | WP-10 | `make bench B=1`, `B=2` and `B=4` run one script each for the benchmark day. The other scripts run through `make toolbox` | The blueprint names the target (§9.1). The other scripts take a range of days or restart the stack, and the report lists each command |
