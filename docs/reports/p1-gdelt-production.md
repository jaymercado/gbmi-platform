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
| 05 | Parsers: Mentions and GKG, text functions, gate G2, contracts | 2026-11-26 17:21 | | | In progress |

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
| R7 | PyIceberg 0.12.0 already retries a failed commit four times and raises a second exception type for a conflict on the same unit | Open: WP-06 |
| R8 | A day build leaves the quarantine files of the hour units of the same day | Open: WP-06 |
| R9 | The DAG tests need the Airflow interpreter in the toolbox, a bundle-aware loader for the import of `common`, and a subprocess assertion which allows `file` and `uname` | Open: WP-07 |
| R10 | A DAG run triggered through the REST API without a logical date has no logical date in the templates | Open: WP-07 |
| R11 | The second run of the tracer has to rebuild the hour, because the end-to-end test expects a new snapshot | Open: WP-07 |
| R12 | ClickHouse 26.8 rejects a query parameter in `CREATE NAMED COLLECTION` | Open: WP-08 |
| R13 | The entrypoint of the ClickHouse image writes a file in `users.d` which replaces the user `default` | Open: WP-08 |
| R14 | A read-only PostgreSQL role needs `CONNECT` on the database `gbmi`; a one-shot service on the PostgreSQL image needs a `tmpfs` | Open: WP-08 |
| R15 | No required workflow builds the operational dbt models | Open: WP-08 |
| R16 | `uv lock` alone keeps typer 0.29.2; hypothesis writes `.hypothesis/`; the aggregation script of phase P0 reads a table which the probe removed | WP-01 (typer, `.hypothesis/`); open: WP-10 |

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
