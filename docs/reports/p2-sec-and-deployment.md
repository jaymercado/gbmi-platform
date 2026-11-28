# P2 status report — SEC ingestion and first deployment

| | |
|---|---|
| Phase | P2 — SEC ingestion and first deployment (blueprint §11.2), as specified by the week 3 plan (`WEEK_3_IMPLEMENTATION_PLAN.md`) |
| State | **Running.** Started on 2026-11-28 at 03:10 UTC. The report grows with each pull request of the week; the last work package (WP-15) completes the report |
| Run | One laptop for the first ten work packages: MacBook Pro M2 Pro, 16 GB, Docker Desktop with a virtual machine of 7.65 GiB and 12 CPUs. One AWS host from the first deployment on. Claude, the AI model which works in the repository, carries out the work packages. Jay gave the go-ahead for the Git work of the week, for three repository settings and for requests to the SEC from the laptop and from the AWS host (week 3 plan, §3.5) |

## Exit gate

The exit gate of the week 3 plan (§2). A row changes from "Open" to "Holds" with the pull request which brings the evidence.

| # | Criterion | State | Evidence |
|---|---|---|---|
| 1 | No second holds more than 8 requests to the SEC: (a) in the tests, (b) in the request log of the AWS host | Open | (a) holds: the real-clock tests of the client (WP-03), the integration test of the three task entry points and drill DR5 show at most 8, and the request log of the working stack after the live load of WP-06 holds 99 requests with at most 4 in any second. (b) is open until WP-13 |
| 2 | The company dimension reproduces the name change from "Facebook Inc" to "Meta Platforms, Inc." from former-name data | Open | The half on the recorded files holds: `dim_company` holds for CIK 1326801 the row "Facebook Inc" with the source `former_name`, valid from 2005-05-06 to 2021-10-27, and the name "Meta Platforms, Inc." from 2021-10-27 on. The end-to-end test `test_the_company_dimension_shows_the_name_change_of_meta` asserts the three rows in `make demo`, on the laptop and in the workflow `e2e` (pull request [#25](https://github.com/jaymercado/gbmi-platform/pull/25)). The live data of the laptop gives the same rows with the first snapshot date 2026-11-28 (section "The SEC models and the mart build on the live data"). The half on the host is open until WP-13 |
| 3 | The dbt unit tests for validity intervals and for as-of logic pass | Holds | Eight dbt unit tests pass on ClickHouse 26.8 with dbt-clickhouse 1.10.4. `make demo` runs the eight tests after the DAG `marts_build` (test `test_the_dbt_unit_tests_pass`, which fails when no unit test ran), on the laptop and in the workflow `e2e` (pull request [#25](https://github.com/jaymercado/gbmi-platform/pull/25)). The section "The SEC models and the mart build on the live data" lists the eight tests |
| 4 | The host is reachable only through Session Manager | Open | |
| 5 | The platform runs on AWS: the stack on the host uses S3 and Glue, and two GDELT DAGs ran for at least 6 hours in a row, in a period which holds one scheduled daily run | Open | |
| 6 | The three SEC DAGs each completed one scheduled run on the host; silver on the host holds the data of the criterion; every SEC object is in a final state | Open | |
| 7 | Drill DR5 passes as a script, and the runbook for a block by the SEC exists | Holds | `make drill DR=5`: 26 checks, none failed, on the laptop on 2026-11-28; the workflow `e2e` runs the drill on every pull request. The runbook is `docs/runbooks/sec-circuit-open.md` (pull request #23) |
| 8 | The delivery path works from end to end: image, deployment through the environment `prod`, `terraform plan` in a pull request, smoke test | Open | |
| 9 | Remote logging works: task logs on S3, container logs in CloudWatch with a retention of 30 days | Open | |
| 10 | The four new Terraform modules are applied; `make plan` reports no changes; `make tf-check` passes; no secret in the state; no account number and no address in the repository | Open | |
| 11 | The first dbt documentation is published | Open | |
| 12 | The host is measured: memory, the build of one GKG day, CPU credits, commit latency on S3 | Open | |
| 13 | The Git commit is the pipeline version on the host | Open | |
| 14 | `make demo` passes on the laptop and in CI with the recorded SEC files; the four workflows of phase P1 are green on `main`; ADR-010, ADR-011 and ADR-015 exist; five contract files; the gold-sheet comparison; the version pins | Open | `make demo` passes on the laptop with the recorded SEC files of five companies: the three SEC DAGs (WP-07) and `marts_build` with the five marts (WP-08); the last five runs took 251, 208, 215, 211 and 212 seconds, the third of them on a reset demo stack. The version pins: checked on 2026-11-28 (section "Version pins"). The five contract files: `docs/contracts/sec_*.yaml` (WP-05). The gold-sheet comparison: 26 of 26 CIKs are in the universe (section "The live load of WP-06") |

## Work packages

The order follows change C10 of the week 3 plan (§3.5). Every time is UTC. The working time of a work package excludes the waits: GitHub checks, downloads, reviews, answers of Jay and the run on the host (change C21).

| WP | Subject | Start | End | Pull request | State |
|---|---|---|---|---|---|
| 01 | Start: checks, pins, plan in the repository | 2026-11-28 03:10 | 2026-11-28 03:30 | [#17](https://github.com/jaymercado/gbmi-platform/pull/17) | Done. Working time about 9 minutes; the GitHub checks took about 12 minutes |
| 02 | Operational tables for SEC objects, circuit and requests | 2026-11-28 03:19 | 2026-11-28 03:44 | [#18](https://github.com/jaymercado/gbmi-platform/pull/18) | Done. Working time about 12 minutes, during the GitHub checks of WP-01; the GitHub checks took about 12 minutes |
| 03 | Fair-access client; review 1 | 2026-11-28 03:32 | 2026-11-28 04:36 | [#19](https://github.com/jaymercado/gbmi-platform/pull/19) | Done. Review 1 ran from 03:49 to 04:05 and reported two blocking defects and eleven smaller ones; the repairs took from 04:06 to 04:25, in the same pull request (change C11). Working time about 15 minutes before the review and about 20 minutes for the repairs; the GitHub checks of the repaired pull request took about 10 minutes |
| 05 | Lake: general units and five SEC tables | 2026-11-28 03:49 | 2026-11-28 04:51 | [#20](https://github.com/jaymercado/gbmi-platform/pull/20) | Done. Written while review 1 read WP-03 (change C10). Working time about 12 minutes for the code and about 5 minutes for the merge of the repairs and the pull request; the GitHub checks took about 11 minutes |
| 04 | Recorded files and payload models | 2026-11-28 04:03 | 2026-11-28 05:12 | [#21](https://github.com/jaymercado/gbmi-platform/pull/21) | Done. The recorder and the metric seed were drafted from 04:03 to 04:08, during review 1. The work went on at 04:26, and the 18 requests left at 04:37, one minute after the merge of WP-03. Working time about 35 minutes; the GitHub checks took about 12 minutes |
| 06 | Pipelines PL-3, PL-4 and PL-5; the watchlist; review 2; one live load | 2026-11-28 04:55 | 2026-11-28 09:52 | [#22](https://github.com/jaymercado/gbmi-platform/pull/22) | Done. The code of the three pipelines, the commands and the tests were written from 04:55 to 05:24 and from 08:36 to 08:42; the session stood still in between. Review 2 ran from 08:42 to 09:18 and found no blocking defect, four defects to repair before the merge and ten notes; the repairs took from 09:19 to 09:27, in the same pull request (change C11). The live load ran from 09:28 to 09:36 on the working stack: 81 requests, no block. Working time about 1 hour 25 minutes |
| 09 | Drill DR5 and one runbook | 2026-11-28 08:43 | 2026-11-28 09:53 | [#23](https://github.com/jaymercado/gbmi-platform/pull/23) | Done. Written while review 2 read WP-06 (change C10). Working time about 10 minutes. The pull request was merged at 09:53, one minute after the pull request of WP-06 |
| 07 | DAGs, DAG tests and the demo | 2026-11-28 08:47 | 2026-11-28 10:15 | [#24](https://github.com/jaymercado/gbmi-platform/pull/24) | Done. The three DAG files and the DAG tests were written from 08:47 to 08:51, while review 2 read WP-06; the summary task and the demo followed from 09:41 to 09:50. `make demo` passed twice in a row on the laptop (202 and 136 seconds). Working time about 15 minutes. The workflow `e2e` of the pull request took 12 minutes 6 seconds with the three SEC DAGs |
| 08 | dbt models, unit tests and `marts_build` | 2026-11-28 09:53 | | [#25](https://github.com/jaymercado/gbmi-platform/pull/25) | The first dbt unit test passed at 10:00, seven minutes after the start (time box: 30 minutes). The models, the tests and the DAG were written until 10:35; `make demo` passed twice in a row and once more on a reset demo stack until 10:50. One run of `marts_build` on the working stack built the five marts from the live data at 10:51; the run showed the defect of change C23 (the knowledge time of a filing which arrives after 17:30 New York time), which was repaired and rebuilt until 10:57. The pull request was opened at 11:11. Working time about 1 hour 20 minutes, of which the five runs of `make demo` took 18 minutes. The end is the merge of the pull request |
| 10 | Runtime for two backends; image; workflow `images` | | | | |
| 12 | The workflow `deploy`, the host targets of the Makefile, the runbook | | | | |
| 11 | Terraform: CI roles, network, roles, host, monitoring; rehearsal; review 3; two applies | | | | |
| 12 | First deployment | | | | |
| 13 | Run and measure on the host | | | | |
| 14 | dbt documentation and three decision records | | | | |
| 15 | Status report and close-out | | | | |

## Version pins

Checked on 2026-11-28 between 03:11 and 03:13 UTC, the start date, against PyPI, Docker Hub, the GitHub container registry, the GitHub releases and the Terraform registry. Rule of the week 3 plan (§4.2): a newer patch release of the same minor version is taken; any other change stays on the pin.

| Component | Pin of phase P1 | Found on 2026-11-28 | Pin of phase P2 |
|---|---|---|---|
| Apache Airflow; task SDK | 3.4.0; 1.4.0 | Newest releases; the digest of the image is unchanged | Unchanged |
| astronomer-cosmos | 1.16.0 | Newest release | Unchanged |
| DuckDB | 2.0.1 | Newest release | Unchanged |
| PyIceberg; PyArrow; SQLAlchemy | 0.12.0; 26.0.1; 2.1.9 | Newest releases | Unchanged |
| dbt Core; dbt-clickhouse; sqlfluff | 1.12.7; 1.10.4; 4.5.0 | Newest releases | Unchanged |
| ClickHouse | 26.8.47.5 (image tag `26.8`, by digest) | **26.8.49.6** (2026-11-27): the tag `26.8` moved to a newer patch release | **26.8.49.6**: the tag `26.8` by the new digest, in `compose/compose.yaml` and in the workflow `dbt` |
| PostgreSQL | 17.12 (image tag `17`, by digest) | 17.12 is the newest release of version 17. The tag `17` points at a new build of 2026-11-26. Both builds hold the same PostgreSQL package (17.12-1.pgdg13+1), the same Debian release (13) and the same C library (2.41-12+deb13u5), read inside both images | Unchanged. A new build is not a release, so the digest of phase P1 stays |
| Metabase | 0.65.7.2 | **0.65.7.4** (2026-11-27): a patch release | **0.65.7.4**, by digest |
| Terraform; AWS provider | 1.17.2; 6.74.0 | Newest releases | Unchanged |
| tflint; AWS rules of tflint; checkov | 0.66.0; 0.50.0; 3.4.11 | Newest releases | Unchanged |
| Python | CPython 3.12.16, managed by uv | uv is unchanged | Unchanged |
| uv; ruff; mypy; pytest; pytest-cov; pre-commit | 0.13.10; 0.17.6; 2.4.1; 9.2.0; 7.1.0; 4.7.0 | Newest releases; the digest of the uv image is unchanged | Unchanged |
| httpx | 0.28.1 | 0.29.0 (2026-11-26) is the newest release: a new minor version, not a patch release | **0.28.1.** The lock file holds the pin. An upgrade needs a decision record (blueprint §6.1, risk R7) |
| hypothesis | 6.170.17 | **6.170.23** (2026-11-28): a patch release | **6.170.23** |
| Other libraries | typer 0.29.3, clickhouse-connect 1.13.0, psycopg 3.3.9, pydantic 2.14.4, pydantic-settings 2.16.3, tenacity 9.2.1, structlog 26.1.0, tzdata 2026.6, pyyaml 6.0.4 | Newest releases | Unchanged |
| boto3 | 1.44.11 in the lock file, as a dependency of PyIceberg | 1.44.13 (2026-11-27) | Unchanged in WP-01. boto3 has no own pin (week 3 plan, §4.2) |
| GitHub Actions of phase P1 | checkout 7.0.1, setup-uv 10.4.0, setup-buildx 4.4.1, build-push 7.4.0, setup-terraform 4.0.1, setup-tflint 6.3.2; gitleaks 8.30.1 | Newest releases | Unchanged |
| New in week 3 | — | `aws-actions/configure-aws-credentials` 6.3.1; `docker/login-action` 4.8.0; `actions/configure-pages` 6.0.0; `actions/upload-pages-artifact` 5.0.0; `actions/deploy-pages` 5.0.1; Trivy 0.76.2; Docker Compose 5.8.1 for the host. The newest `apache-airflow-providers-amazon` at PyPI is 9.41.0; WP-10 reads the version inside the image | The work package which first uses a tool pins the tool: an action by commit hash, a release file by checksum |

## Review of the plan before the start

On 2026-11-28, before the first work package, four read-only agents (further instances of the AI model) compared the week 3 plan with the repository at `2f000c4` and with the installed libraries. Three agents studied one area each: the package and the tests; the DAGs, dbt, Compose and the workflows; Terraform and the policy checks. One agent read the installed libraries and searched the draft of the execution approach for defects. The table lists the 34 points and the planned handling. The tag says how sure a point is: **[V]** verified by Claude with a read-only command; **[RV]** reported by a study agent with a file and a line, not yet repeated by Claude; **[S]** suspected, not checked. The column "State" changes with the pull request of the named work package.

| # | WP | Point | Planned handling | Tag | State |
|---|---|---|---|---|---|
| RF1 | 02 | `tests/ledger/test_schema.py:73-87` expects exactly six tables in the schema `ops`; lines 260-267 expect the constraint name `silver_partition_unit` | The table list of the test grows by four names. `0005` drops the check and adds the wider check under the same name | [RV] | Handled in WP-02: the test names ten tables; `0005` drops the check and adds the wider check under the name `silver_partition_unit` |
| RF2 | 02, 05 | `ops.silver_partition`: `feed`, `file_count`, `fileset_hash` and `content_digest` are NOT NULL (`0002_silver_partition.sql:8,25-27`), and the one index starts with (`table_name`, `slot_date`). `PartitionVersion` requires `slot_date` and derives the unit kind from `slot_hour` (`src/gbmi/ops/partitions.py:29,49-51`) | An SEC unit writes `feed` = the dataset, `file_count` = the objects of the unit, `fileset_hash` = a hash over the names and the MD5 hashes of the objects, `content_digest` = the checksum of the committed rows. `0005` adds an index on (`table_name`, `unit_label`). `PartitionVersion` gains optional fields with defaults, so each call of phase P1 stays unchanged | [RV] | Handled. WP-02: `0005` adds `unit_label` and the index `silver_partition_by_label`. WP-05: `PartitionVersion` has the optional fields `kind` and `unit_label`, and each call of phase P1 is unchanged. The values of `feed`, `file_count`, `fileset_hash` and `content_digest` of an SEC unit are written by the pipelines of WP-06 |
| RF3 | 02 | `StaleStateError` takes a GDELT key (`src/gbmi/ledger/repository.py:135-145`). The trigger function `ops.refuse_change` refuses every UPDATE of a table (`0001_ledger.sql:18-22,113-114`) | One base class for the stale error with a subclass for objects. `ops.source_request` gets an own row trigger: no DELETE, no change of the fixed columns, one update of the outcome | [RV] | Handled in WP-02: `gbmi.ledger.errors.StaleStateError` with the subclass `StaleObjectStateError`; the trigger function `ops.guard_source_request` |
| RF4 | 02 | The migration runner needs the numbers 1 to n without a gap (`src/gbmi/ledger/migrate.py:84-118`). The role `gbmi_ro` reads a new table through default privileges, and no migration file may name the role (`docker/postgres/roles/01_readonly.sql:7-8,20-22`) | The test "`0005` on a database with rows" applies a temporary folder with `0001` to `0004`, inserts rows of phase P1 and then applies the full folder | [RV] | Handled in WP-02: `tests/ledger/test_object_schema.py` applies the first four files, inserts two partition versions of phase P1 and then applies `0005`. On the demo stack the role `gbmi_ro` reads the four new tables and cannot write |
| RF5 | 03 | `sec_user_agent` is plain text, and `gbmi config show` prints the settings as JSON (`src/gbmi/config.py:63`, `src/gbmi/cli.py:97-101`). `dict_tracebacks` of structlog 26.1.0 renders local variables (`structlog/tracebacks.py:49`). A run record stores the text of an exception (`src/gbmi/ops/runs.py:96`) | The type `SecretStr`. The renderer `ExceptionRenderer(ExceptionDictTransformer(show_locals=False))`. The settings hide the input of a validation error; the format check of the value lives in the client and raises a fixed text | [RV]; the echo of pydantic [S] | Handled in WP-03: `SecretStr`, `hide_input_in_errors`, the renderer without local variables, and three tests which search the log lines, the request log and the error text of a run record. The echo of pydantic is confirmed: without the option a validation error shows the value |
| RF6 | 03 | `HttpFetcher` belongs to GDELT: index lines, redirects followed, no header (`src/gbmi/sources/gdelt/land.py:142-182`). The fake GDELT server records file names only (`tests/ledger/gdelt_server.py:81`). `Settings` is frozen and cached, and `fixtures_dir` is the GDELT folder | `SecClient` is new code with an own `httpx.Client`. `tests/ledger/sec_server.py` records time, host, path and headers. A new field `sec_fixtures_dir`; a test which changes the environment clears the cache of the settings | [RV] | Handled in WP-03: `gbmi.sources.sec.client.SecClient`, `tests/ledger/sec_server.py`, the setting `sec_fixtures_dir`, and `tests/conftest.py`, which removes an inherited SEC source from every test |
| RF7 | 03, 06 | The coverage gate is the list `COVERAGE_GATE` of the Makefile (`Makefile:117-124`). `make test` runs `tests/unit`, `tests/ledger`, `tests/transforms` and `tests/contract` only | WP-03 adds `src/gbmi/sources/sec/*` to the list. The pipeline tests which count for the gate live in `tests/ledger` | [RV] | Half handled in WP-03: `src/gbmi/sources/sec/*` is part of the coverage gate (client 96%, the four other modules 100%). The pipeline tests follow in WP-06 |
| RF8 | 03, 12 | The value of `GBMI_SEC_USER_AGENT` stands in double quotes in `.env` | `make host-secrets` and `make redact-check` remove the quotes. One test of WP-03 proves which value a container receives through Compose | [V]; Compose [S] | Half handled in WP-03: `tests/unit/test_compose_env.py` renders the Compose file and proves that a quoted value arrives without the quotes; `tools/redact_check.py` removes the quotes. `make host-secrets` follows in WP-12 |
| RF9 | 04, 07 | The fixtures hold one index file, and the demo clock is fixed. A weekday without a file stays `expected` until the listing names a newer file, and the volume band needs four earlier files | The recorded listing names the one recorded index file, so each earlier weekday of the window becomes `not_published`. The volume rule records "skipped" below four files. The demo clock lies after the publication of the file | [S] | Handled. WP-04: the recorded listing names one form index file, and the fixture transport answers 403 with the recorded body for every other index file. WP-06: the volume rule records "skipped" below four earlier files. WP-07: the clock of the demo is 2026-11-26 12:00 UTC, after the publication of the file; the four earlier weekdays of the seven days become `not_published` |
| RF10 | 05 | `Unit` is imported in about 25 places and used by the quarantine helper, the audit functions and `DayFile` (`src/gbmi/lake/commit.py:51-159`, `src/gbmi/quality/audit.py:65,181`) | `Scope` is a protocol with `label`, `row_filter` and `check`. `Unit` keeps the constructor and fulfils the protocol. `overwrite_scope` is the general function, and `overwrite_unit` calls `overwrite_scope` | [RV] | Handled in WP-05, with other names: the protocol is `UnitOfWork` (label, kind, row filter, check); `Scope` is the class with the three builders; `Unit` is unchanged and fulfils the protocol; `overwrite_unit` calls `overwrite_scope` |
| RF11 | 05 | `create_table_if_missing` always partitions by `slot_date`, and `_schema` always adds `_slot_ts` (`src/gbmi/lake/tables.py:51-83`). `tests/integration/lake/test_silver_tables.py:57` compares the fixture hour with `SILVER_SCHEMAS`. The contract test fixes the partition `slot_date` and six provenance columns (`tests/contract/test_silver_contracts.py:52-74`) | A parameter for the partition column and a switch for `_slot_ts`. The SEC schemas live in an own dictionary. The contract test reads the partition and the provenance columns from the definition of the table | [RV] | Handled in WP-05: `create_table_if_missing(..., partition_column=...)`, `_schema(..., slot_ts=False)`, the dictionary `SEC_SCHEMAS` beside `SILVER_SCHEMAS`, and a contract test which reads the key, the partition and the provenance columns from the definition of each of the nine tables |
| RF12 | 05, 06 | `check_volume` has a fixed band of 0.5 to 2.0 and a fixed history of four values (`src/gbmi/quality/audit.py:36-38,257-298`). One commit of PyIceberg can hold three snapshots; the comment in `src/gbmi/lake/commit.py:8` says two | Parameters with the values of phase P1 as defaults; the 5% rule and the 2% rule are new functions. WP-05 tests the maintenance code with a commit of three snapshots | [RV] | Half handled in WP-05: `check_volume` takes a band; `check_change` (5% rule) and `check_fall` (2% rule) are new. An integration test builds a commit of three snapshots (delete, overwrite, append): the maintenance code counts the commit as one commit and keeps the commit whole. The pipelines of WP-06 call the rules |
| RF13 | 06 | `Bronze.read` loads a whole object, and `Bronze` has no method `exists` (`src/gbmi/sources/gdelt/land.py:258-301`). One test of phase P1 holds the behaviour of the `.part` file on the local file system | New methods `exists` and `size`. The archive never passes through `Bronze`. The local write path stays | [RV] | Open |
| RF14 | 07, 08 | `make demo` has no step for the GDELT DAGs: the DAGs run inside the one pytest call (`Makefile:236-250`, `tests/e2e/test_gdelt_dags.py`) | `tests/e2e/test_sec_dags.py` runs the three SEC DAGs and `marts_build` inside the pytest call, after the GDELT file, with the assertions of the week 3 plan. The dbt unit tests run after `marts_build` and build no model again; the step fails when no unit test ran | [RV] | Handled. WP-07: `tests/e2e/test_sec_dags.py` runs the three SEC DAGs inside the one pytest call of `make demo`, after the GDELT file. WP-08: `tests/e2e/test_sec_marts.py` runs `marts_build` once, checks the five marts against separate code, and runs the dbt unit tests with `dbt test --select test_type:unit`; the unit tests build no model, and the test fails when no unit test ran |
| RF15 | 07 | `DEFAULT_ARGS` uses a growing delay, and the demo stack has 0 retries (`dags/common/__init__.py:30-42`). The lists and the loops of `tests/dags/test_dags.py` name the GDELT DAGs only (lines 23-47, 166, 209-225); each task needs a time limit (lines 109-114) | `SEC_ARGS`: `retries = min(TASK_RETRIES, 1)`, a delay of 20 minutes, no growth. The tests loop over every DAG | [RV] | Handled in WP-07: `SEC_ARGS` in `dags/common` (one retry, 20 minutes, no growing delay), and the DAG tests name the three SEC DAGs in every list and loop |
| RF16 | 03, 07 | `GBMI_SEC_SOURCE`, `GBMI_SEC_FIXTURES_DIR` and `GBMI_WATCHLIST_SIZE` stand neither in the Compose environment nor in the stack block of the Makefile (`compose/compose.yaml:21-54`, `Makefile:18-45`) | WP-03 adds the first two variables, WP-07 the third | [RV] | Handled: WP-03 added `GBMI_SEC_SOURCE` and `GBMI_SEC_FIXTURES_DIR`; WP-06 added `GBMI_WATCHLIST_SIZE` (50 for the working stack, 5 for the demo stack) and `GBMI_WATCHLIST_FILE` |
| RF17 | 08 | A unit test of dbt-clickhouse 1.10.4 creates one temporary table named after the test and drops another name (`dbt/include/clickhouse/macros/materializations/unit.sql:9-13,37`). An input in the format `dict` or `csv` needs the input relation in the warehouse (`dbt/parser/unit_tests.py:140,195-203`). NULL for a column which is not Nullable becomes the default value of the type (`macros/utils/safe_cast.sql:6-12`). The role `dbt_rw` has no right to create a temporary table (`db/clickhouse/init/02_roles.sql`) | Each unit test has a name which is unique in the project. A fixture quotes dates and decimals and gives no NULL for a column which is not Nullable. The grant is added. Claude runs the first proof (week 3 plan, WP-08 step 1) early, on the demo stack, during the wait for the checks of WP-02. WP-08 commits the proof in the project of `tests/integration/dbt`, so the workflow `dbt` holds the proof of finding F18 of the week 3 plan in CI | [RV]; the name collision [S] | Handled in WP-08, with one correction. The first proof ran at the start of WP-08, not during the checks of WP-02. Each of the eight unit tests has a name of its own; a fixture quotes dates and decimals and gives NULL only for a column which is Nullable. **The grant is not needed and was not added** (change C24): the unit test passed with the right and after `REVOKE`, and the query log shows the statement `create temporary table` of the user `dbt` without the right. `tests/integration/dbt/test_f18_unit_tests.py` gives the workflow `dbt` the proof: a unit test passes for a user with exactly the privileges of `dbt_rw`, a unit test with a wrong expectation fails, and a build with the exclusion runs no unit test. The name collision was not tested: no two tests share a name |
| RF18 | 08 | `dbt test` and `dbt build` accept `--exclude-resource-type` (`dbt/cli/params.py:254-280`). Cosmos 1.15.0 and later applies `RenderConfig.exclude` to the test task of each model. The toolbox inherits the environment of the Airflow services (`compose/compose.yaml:345`) | `marts_build` uses `RenderConfig(exclude=["resource_type:unit_test"])`, and a DAG test asserts the exclusion. No variable of the Compose environment removes unit tests: the toolbox would lose the unit tests of the demo without an error | dbt [RV]; Cosmos [S] | Handled in WP-08. Read in the installed Cosmos 1.16.0 (`cosmos/airflow/graph.py`, function `forward_render_exclude_to_test`): `RenderConfig.exclude` reaches the test task of each model. `marts_build` uses the exclusion, the DAG test `test_no_task_runs_a_dbt_unit_test` asserts the exclusion for each of the 22 test tasks, and no task of a run on the demo stack carries a unit test. No variable of the Compose environment removes unit tests |
| RF19 | 08 | The DAG test skips only the task ids which start with `dbt.` (`tests/dags/test_dags.py:126`). Cosmos reads `dbt/target/manifest.json`, which the service `dbt-manifest` writes at `make up` (`compose/compose.yaml:266-273`). Cosmos builds a seed only when the selection holds the seed | The Cosmos group of `marts_build` has the name `dbt`. After a new model: `make up` again, or `dbt parse` in the toolbox. The selection holds the two seeds, and a DAG test asserts the two seed tasks | [RV]; the seeds [S] | Handled in WP-08. The Cosmos group has the name `dbt`. The selection holds `path:seeds`, and Cosmos renders the two seed tasks `dbt.watchlist.seed` and `dbt.metric_concept_map.seed`, each followed by its tests; a DAG test asserts both. The DAG tests read the manifest of the running stack |
| RF20 | 08 | The dbt project has no contract, no seed folder and no block for intermediate models, and `generate_schema_name` uses a custom schema as written. The demo reads 5 rows of a seed with 50 rows | The blocks of WP-08 step 2. A contract uses the exact type names of ClickHouse. One model `int_watchlist` cuts the seed with a dbt variable which takes the value of `GBMI_WATCHLIST_SIZE` | [RV]; the demo case [S] | Handled in WP-08. `dbt_project.yml` has the block for the intermediate models (views in the database `intermediate`); the seed block came with WP-04. Each of the five marts has a dbt contract with the type names of ClickHouse, for example `Nullable(DateTime64(6, 'UTC'))` and `Decimal(38, 6)`; no contract had to be replaced by a test. The model is named `int_company__watchlist`. The size comes from the setting `GBMI_WATCHLIST_SIZE` of the stack through the dbt function `env_var` (macro `watchlist_size`), so the models and the pipelines read the same number: 5 companies carry `is_watchlist` on the demo stack and 50 on the working stack |
| RF21 | 08 | Four DAGs with one dbt process for each Cosmos task can push the workflow `e2e` above 20 minutes; the limit of the job is 30 minutes (`.github/workflows/e2e.yml:23`) | WP-08 measures the workflow. First response: `TestBehavior.BUILD`, one task for each model. Second response: the row of week 3 plan §10 | [S]; the limit [RV] | Handled in WP-08 by measurement; no response was needed. One run of `marts_build` has 43 tasks (17 models and 2 seeds with one run task and one test task each, 3 tests which read two models, the pin task and the record task) and took 61 to 67 seconds on the demo stack and 61 seconds in each of the two runs on the working stack with the live data: Airflow runs four dbt tasks side by side, each for 3 to 7 seconds. `make demo` with the four new DAGs takes 208 to 251 seconds on the laptop. The workflow `e2e` took 12 minutes 6 seconds before `marts_build` (pull request #24); the time with `marts_build` is the time of the check of pull request #25 and is recorded with the next pull request |
| RF22 | 09 | `drills/common.py`: `world()` and `Drill` fit as written; `World.land`, `fixtures_dir()` and `AS_OF` belong to GDELT | SEC helpers beside the GDELT helpers; `DRILL_5` in the Makefile; one step in `e2e.yml`. Drill DR5 passes on the laptop before the live load | [RV] | Handled in WP-09 (pull request #23): the SEC helpers stand beside the GDELT helpers in `drills/common.py`, the Makefile has `DRILL_5`, and the workflow `e2e` runs `make drill DR=5`. The drill was committed at 08:47 UTC, before the live load of 09:28 UTC |
| RF23 | 10 | Eight services merge the anchor `airflow-common` one by one, and the backend variables stand in the shared environment (`compose/compose.yaml:15-84`). ClickHouse mounts the configuration and the lake in one list (lines 122-127); the server limit of ClickHouse defaults to 2 GiB (line 121) | The overlay names each of the eight services. The first step of WP-10 renders the two files with `docker compose config`; the fallback of three files (week 3 plan §3.3) is decided within 30 minutes | [RV]; the tags of Compose [S] | Open |
| RF24 | 10 | The service `dbt-manifest` runs `dbt parse` without a target, and every Airflow service waits for the service (`compose/compose.yaml:83-84,266-273`). `compose/smoke.sh` fixes `.env` and `compose/compose.yaml` and writes to `/lake` (lines 9-10, 29-36, 189-201). `make images` passes the short commit (`Makefile:129-132`), and no workflow builds the stage `runtime` today | The output `prod` of the dbt profile renders with the variables of the host. The smoke test gets the host mode of WP-10 step 5. The image receives the full commit. One local build of the stage `runtime` comes before the workflow `images` | [RV] | Open |
| RF25 | 10 | PyArrow on S3: `create_dir` writes an empty marker object, `move` is a copy and a delete, and a default output stream is a multipart upload of three requests | On the AWS backend, the bronze write and the quarantine write call `put_object` of boto3 once. boto3 1.44.11 is installed [RV]; the package declares boto3 without an own pin. One test uses the stub of botocore. `ensure_silver_namespace` asks first and creates second (finding F19 (k) of the week 3 plan) | [S] | Open |
| RF26 | 11 | The rule for `airflow-logs/` belongs to the resource `aws_s3_bucket_lifecycle_configuration.ops` of the module `lake_s3` (`infra/terraform/modules/lake_s3/main.tf:172-221`). The bucket keeps versions, so an expired log stays 30 more days as an old version | As found. The status report states the 60 days | [RV] | Open |
| RF27 | 11, 12 | `gbmi-ci-plan` trusts exactly the subject which ends with `:pull_request`; `gbmi-ci-deploy` trusts exactly the subject which ends with `:environment:prod` (`infra/terraform/bootstrap/github_oidc.tf:45-55,75-79`). `make plan` needs two git-ignored files, and `bootstrap` has three variables without a default (`bootstrap/variables.tf:7-25`) | The job `plan` runs for the event `pull_request` only and names no environment. The job passes the backend settings as options and takes the three variables from the `github` context. The plan text goes to a file, and the job summary shows the summary line only. The job of the workflow `deploy` names the environment `prod` | [RV] | Open |
| RF28 | 11 | Without the state policy of `gbmi-ci-deploy`, the data source `state_write` is unused, and tflint fails (`github_oidc.tf:105-113,199-203`). `TF_MODULES` lists the modules one by one (`Makefile:70-71`). The default tags hold `project` and `env` only | The policy and the data source are removed, and the description of the role is corrected. Four names join `TF_MODULES`. The instance gets the tag `Name = gbmi-prod-host` and ignores a change of the image and of the user data | [RV] | Open |
| RF29 | 11 | `gbmi-ci-plan` reads only resources named `gbmi-*` of S3, Glue, budgets and IAM (`github_oidc.tf:125-171`) | New read actions for EC2, the instance profile, the command document, the log groups, the alarms, the topic, the schedules and the public parameter of the image. The role never reads a parameter under `/gbmi/prod/` | [RV] | Open |
| RF30 | 11 | checkov has checks which the design fails by decision: the hop limit 2, the public address, detailed monitoring, flow logs, the key and the retention of the log groups, the key of the topic and of the schedules | `make tf-check` shows the real list. Each check is met, or skipped with a reason in `.checkov.yaml` (week 3 plan, WP-11 step 3) | [S] | Open |
| RF31 | 12 | The update call of GitHub for an environment can replace the settings which the call leaves out | The call for the branch rule sends the reviewer rule again. A read after the call proves the reviewer rule and the branch rule. Claude never approves and never bypasses a deployment | [S] | Open |
| RF32 | 10 | The token of `gh` cannot read packages, and Docker on the laptop can hold a login | The check of J6 is one request to the registry without a login | [V] | Open |
| RF33 | 11 | With the first spot request, AWS can add a service-linked role for spot instances to the account | Claude reads the roles of the account after the apply, and the status report lists each role which AWS added | [S] | Open |
| RF34 | 14 | The Makefile has no target `docs`, and `e2e.yml` grants `contents: read` only | The Pages job has own permissions and runs for a push to `main` only | [RV] | Open |

## Review 1: the fair-access client, before the first live request

On 2026-11-28 from 03:49 to 04:05 UTC, one independent review agent (a further instance of the AI model, with read-only duty) read the client, the limiter, the circuit, the object ledger and the two migration files in the open pull request #19 (change C11). No request had gone to the SEC. The review reported two blocking defects, eleven smaller defects, schema points and weak tests. Every point is repaired in the same pull request; the table lists each point.

| # | Finding | Repair |
|---|---|---|
| B1 (blocking) | The limiter spaced the moments of permission, not the moments of departure. After the permission the client writes a log row and may set up a connection. A slow set-up of request n could bring request n + 8 less than one second behind request n on the wire: 9 requests in one second | The client tells the limiter when the answer of a request has arrived (`settle`), and the limiter counts the request from that moment. Request n + 8 leaves at least 1.050 seconds after request n was answered, so the rule holds where the requests leave and where the requests arrive. Tests: a property test with made-up connection times, a client test with a slow first request, and the real-clock test with a first request of 150 ms. With the defect put back, five tests fail |
| B2 (blocking) | A block by the SEC was remembered in the database of whatever stack sent the request. The recording was planned with a throwaway database, which forgets a block at the end of the run, and the demo stack can be reset without a question. The error text of a locked circuit named a command which did not exist | Live requests leave from the working stack only: the stack alone decides the source, and neither the shell nor the command line of `make` changes the source (the override of choice 10 is withdrawn). A client with a network connection refuses a throwaway database and any source but `sec`. `make reset` of the working stack refuses while a block is recorded (`compose/circuit_guard.sh`). The commands `gbmi sec circuit status` and `gbmi sec circuit close --note` exist. Change C22 moves the recording of WP-04 to the working stack |
| 1 | A HEAD request for a missing file below `/Archives/` has no body, so the answer was read as a block | The client has no HEAD request. The answer of a download carries the size and the time of the last change |
| 2 | A "no such object" answer to a probe closed the circuit, so the guard of three missing listed objects could never lead to a lock | Only the object (status 200) closes the circuit. A "no such object" answer to a probe for a listed object counts as a refused probe and locks the circuit; for another object the answer decides nothing |
| 3 | The opening of the circuit could be skipped: the log row was completed first, and a wrong `Retry-After` header raised an error before the block was recorded | The circuit is opened first, the log row is completed in every case. The header is read as up to seven ASCII digits or as a date; any other value counts as no value, and a block lasts at most seven days |
| 4 | The lock and the circuit followed a free argument `source`, not the hosts of the SEC | A client with a network connection accepts only the source `sec`. Tests with a mock transport keep a source name of their own |
| 5 | One thread was assumed and not enforced | One request runs at a time inside a client; a test shares one client between two threads |
| 6 | The time limits did not hold: the limit of a download was checked once for each megabyte, and a read into memory had no limit for the whole body | Both paths read the body as the bytes arrive and check the limit each time: 300 seconds for an object in memory, 1,800 seconds for a download. A body in memory has a size limit of 64 MiB. Tests use a body which arrives in slow pieces |
| 7 | A download with a status 2xx other than 200 returned success without a file | Only the status 200 is the object |
| 8 | `Content-Length` counts the encoded bytes of a compressed body, and the client reported the decoded size beside the header | The answer gives `content_length` only for a body without a content encoding |
| 9 | A made-up identity such as an address at `example.org` could start a live client | A client with a network connection refuses the domains which are reserved for examples and tests |
| 10 | `supersede_older` did not check that the newer object is loaded; `superseded` is final | The statement moves nothing unless the newer object is loaded |
| 11 | `raise … from None` kept the error of the transport as the context of the raised error, and the request of the error holds the headers | The error is raised outside the handler: neither cause nor context. The test injects an error which carries the request and the user agent |
| Schema | Before the two files freeze: no rule stopped an UPDATE from moving an object outside the ten transitions; counts could be negative; the outcome of a request could be written in parts; a deleted circuit row would forget a block; the circuit had no history; the request log had only clocks of the client | `0004`: a row trigger keeps `ops.source_object` on the ten transitions, refuses another state for a new object, a changed key and a delete; checks for counts and for the form of the MD5 hash; a reason for `quarantined`; the outcome of a request is one statement with `completed_at`; `ops.source_request` gains `recorded_at` (clock of the database) and `probe`; `ops.source_circuit` refuses DELETE and TRUNCATE; the new append-only table `ops.circuit_event` holds one row for each opening, lock and close. `0005`: an empty `unit_label` is refused. A listing of a quarter will carry the year and the quarter in the object name (WP-06) |
| Weak tests | The real-clock tests passed with defect B1; the secrecy test injected a harmless error; no test had a slow body; the property test of `supersede_older` blessed the defect; `tests/conftest.py` did not stop a network transport | Each test is strengthened as the rows above say. `tests/conftest.py` lets the construction of a network transport fail in every test and clears the cache of the settings |

Not repaired, by decision: the review noted that `make redact-check` searches the working tree and not the Git history, and searches the addresses and not the name. Both follow the plan (§3.3): the name of the owner is public with the repository, and gitleaks covers the history for secrets.

## Review 2: the pipelines, before the live load

One independent review agent with read-only duty read pull request #22 at commit `1a642c5` on 2026-11-28 from 08:42 to 09:18 UTC: the three pipelines, the landing, the row functions, and the models, the index parser, the object ledger and the commit helper below them. The review found **no defect which had to block the live load**, four defects to repair before the merge, and ten notes. Every repair is in pull request #22.

What the review did beside reading: the tests which need no database (520 passed); the parsers and the row functions on the recorded files and on the 18 raw answers; one made-up archive with 1,000,000 members, which opens in 2.7 seconds with about 600 MiB of memory; and one facts load of 1.16 million rows, which needs about 1.0 GiB at its peak and 0.2 seconds for the commit into 50 partitions.

Two points about the review itself. The agent read the git-ignored folder `data/sec-raw/`, which the brief had not named. The agent ran `gh pr view 22` once: a read request to GitHub, which the brief had forbidden. No request went to the SEC, and the agent changed no file of the repository.

| # | Finding | What was done |
|---|---|---|
| R2-1 | The archive of 1.58 GB lived in a temporary folder until the ledger knew. A failure while one member was stored lost the file, and the next run of the day asked again. No count limited the tries of one day | Repaired. A member which cannot be stored, for any reason, counts as refused, and the copy goes on. The download and the use of the archive are recorded in one transaction. After three failed tries of one day the archive of the day is quarantined |
| R2-2 | A quarantined submissions document of a company outside the watchlist was never read again after a requeue, although the bytes were in bronze | Repaired. The reference pipeline brings each `expected` submissions object of the day to `verified` from the bytes in bronze, without a request (`Lander.recover`) |
| R2-3 | An index file which the listing names and which answered "no such object" became `not_published`, a final state. A later run opened the file again only inside its own seven days | Repaired. Every run opens each `not_published` index file again which a listing of the run names |
| R2-4 | Change C19 (no archive download between 04:30 and 05:30 UTC) was not in the code | Repaired. A run on the live source inside the hour builds the snapshot without the archive and creates no object for the archive; a later run of the day asks. The recorded files have no such hour |
| R2-5 | The listing of a quarter is one object of the UTC day. A run before the SEC publishes the newest index file (about 03:00 UTC) stores a listing without the file, and no later run of the day sees the file | Not repaired in code. The schedule is 08:30 UTC, and the host does not run between 00:00 and 08:00 UTC. The rule for a run by hand: not between 00:00 and 03:30 UTC. The next day loads the file |
| R2-6 | A value of 1E+74 or more raised an error of the decimal module, which stopped the facts of every company | Repaired: such a value refuses the one document |
| R2-7 | `value_text` wrote 0.0000004 as `4E-7` | Repaired: plain notation with every digit. The contract of `sec_xbrl_fact` says so |
| R2-8 | A crash after the commit of the snapshot and before its record is repaired by the next run of the same day only. A run of a later day does not build the day again | Not repaired in code. The recovery is one command, which sends no request because the files of the day are in bronze: `gbmi sec reference --as-of <the day>T12:00:00+00:00` |
| R2-9 | `gbmi sec objects requeue` without a dataset sent every quarantined object back, also an object without bytes in bronze, which means new requests | Repaired. The dataset is required. The result lists the objects which will be asked again under `asked_again` |
| R2-10 | After an operator accepted a fall of facts with a note, a later run of the day without the note failed as a whole. The selection counted from the load of a document, not from its download | Repaired. The fall rule applies to a document once, before its first load. The selection counts from the download |
| R2-11 | The volume rule needed the file of each of the four weeks before; one holiday switched the rule off for a weekday for four weeks | Repaired: the rule takes the four newest earlier files of the same weekday which exist, up to eight weeks back |
| R2-12 | A company which is new in the ticker file was asked as a listed object: three new companies in a row without a document would open the circuit | Repaired: such a request is not "listed" |
| R2-13 | A time of a former name which is not midnight in New York moves to another day without a count | The count was added, and the live load showed 7,938 such times: a defect of the rule of the plan. The rule was repaired after command 2 of the live load (section "The live load of WP-06") |
| R2-14 | A scope built without its builder gave the filter "every row"; the draw of the watchlist wrote a short list; the list of common words is empty before a dry run | Repaired: a scope with missing parts cannot be built; the draw writes 50 rows or nothing. The dry run comes first |

Tests which the review asked for and which now exist: the row counts of the tables after a second run; a requeued document outside the watchlist; a member which cannot be stored; three failed tries of the archive; a listed index file which is missing; a run which stops between the commit and its record; values with an exponent and beyond the limit; a former name with another time than midnight. After the repairs: `make lint` clean, `make test` with 849 tests, the coverage gate at 98%.

The review also gave advice for the live load, which the live load follows: one run identifier for each command; every command on one UTC day; no interruption of the archive download; a look at the refused documents before the draw of the watchlist.

## The live load of WP-06

The three pipelines ran once on the live source, on the working stack of the laptop, on 2026-11-28 from 09:28 to 09:36 UTC, after the repairs of review 2. `benchmarks/p2_sec/live_load_evidence.py` read every number of the section from the request log, the run records, the object ledger and the five tables; the raw result is `benchmarks/p2_sec/results/live-load.json`.

**Before the first request:** the laptop on mains power; the circuit `closed`; no SEC object in the ledger; 826 GB free for the temporary file of the archive; the time outside 04:30 to 05:30 UTC (change C19).

| # | Command | Run | Start | Seconds | Requests | Result |
|---|---|---|---|---|---|---|
| 1 | `gbmi sec filings --from 2026-10-26` | `live-load-filings-1` | 09:28:08 | 32.7 | 29 | The listing of the quarter, 23 index files with 127,763 rows, and the documents of the five demo companies with 4,972 filings. The two weekdays without a file, 2026-11-11 and 2026-11-26, became `not_published` without a request |
| 2 | `gbmi sec reference --bulk` | `live-load-reference-1` | 09:28:54 | 166.5 | 2 | The ticker file and the archive: 1,578,485,771 bytes in 106.4 seconds (14.8 MB in a second), 1,000,152 members. All 7,983 wanted documents were copied, none refused. One snapshot with 7,988 companies; 6,489 former names |
| 2b | `gbmi sec reference` | `live-load-reference-2` | 09:33:18 | 24.1 | 0 | A rebuild of the day after a repair (see "The times of a former name" below). The snapshot has the content checksum of command 2; the former names have the repaired days |
| 3 | `watchlist_candidates.py --write` | | 09:34 | 5 | 0 | The seed with 50 rows |
| 4 | `gbmi sec filings` | `live-load-filings-2` | 09:34:52 | 1.1 | 0 | The documents of the 50 companies were in bronze from the archive: 96,326 filings |
| 5 | `gbmi sec facts --full` | `live-load-facts-1` | 09:35:03 | 35.2 | 50 | 50 facts documents with 188 MB and 1,255,672 facts; no document refused |

**Requests.** The live load sent 81 requests; the plan estimated about 130. The difference: the 45 documents of command 4 came from the archive of command 2. The request log of the working stack holds 99 rows with the 18 requests of the recording of WP-04: 97 with the outcome `ok` and 2 with the outcome `missing` (the two planned ones of the recording). `gbmi sec requests max-burst` over the whole log: at most 4 requests in any second, by the wall clock and by the clock of each client; the ceiling is 8. The circuit never opened: `ops.source_circuit` has no row.

**"Done when" of WP-06.**

| Criterion | Result |
|---|---|
| One snapshot row for each CIK of the ticker file | 7,988 rows for 7,988 CIKs (10,400 rows of the ticker file) |
| One loaded index file for each published business day since 2026-10-26 | 23 files for 25 weekdays; the two other weekdays are US holidays and `not_published` |
| Filing detail for 50 companies | 96,326 rows for 50 companies |
| Facts for each watchlist company with a facts document | 50 of 50 companies have a facts document: 1,255,672 rows |
| Every object is in a final state | 8,064 objects `loaded` (1 archive, 1 ticker file, 23 index files, 1 listing, 7,988 submissions documents, 50 facts documents) and 2 `not_published`; `gbmi sec objects` lists no open object |
| No second of the request log holds more than 8 requests | At most 4 |

**The five tables after the load.**

| Table | Rows | Data files | Bytes |
|---|---|---|---|
| `sec_company_snapshot` | 7,988 | 1 | 362,049 |
| `sec_company_former_name` | 6,489 | 1 | 245,687 |
| `sec_filing_index` | 127,763 | 23 | 3,615,215 |
| `sec_filing_detail` | 96,326 | 1 | 3,042,070 |
| `sec_xbrl_fact` | 1,255,672 | 50 | 10,831,818 |

Bronze holds 669 MB of submissions documents, 188 MB of facts documents and 25 MB of index files for the day.

**Numbers which the plan asked for.** The download of the archive took 106.4 seconds. The 23 index files hold 1,739 rows which repeat a pair of accession number and CIK inside one file (finding F11 c). 3 pairs stand in two index files. 1,425 rows carry a date before the date of their file (finding F11 d). No line of the 23 files failed the parser. The volume rule ran for 3 files (2026-11-23, 2026-11-24 and 2026-11-27: passed) and was skipped for the 20 files without four earlier files of the same weekday.

**The times of a former name: one defect found by the live load.** The plan (finding F3) saw times at 04:00 and 05:00 UTC, which are midnight in New York, and turned a time into a date in New York. The 7,988 documents hold 12,982 such times: 7,938 at 00:00 UTC, 3,051 at 04:00 UTC and 1,993 at 05:00 UTC, and no other time. For the 7,938 times at midnight UTC the date in New York is the day before the day which the SEC means. Review 2 had asked for a count of such times (finding R2-13), and the count showed the defect after command 2. The rule is now: the day of a name is the UTC date of its time, which is right for the three forms; a time of another form is counted (`odd_times`, 0 in the live load). Command 2b built the day again from bronze without a request. 46 former names have an end before their start, and 2 are exact repeats; the dimension of WP-08 drops and counts the first kind.

**More which the live data shows beside the plan.**

| Finding of the plan | The live data |
|---|---|
| F4: `filings.recent` holds at most 1,000 filings | The lists hold "at least one year": the 50 documents of the watchlist list 96,326 filings, and 45,163 of them have the form `424B2` of the large banks |
| §3.3: the attributes of a company come from its submissions document | Of 7,988 companies, 6,886 have a SIC code, 7,089 a state of incorporation and 7,562 an end of the fiscal year; 155 have no exchange in the ticker file. No company is stale or inactive on the first day |
| §3.3: seven metrics | The 50 companies with at least one candidate concept of the metric seed: cash 50, equity 50, net income 50, total assets 50, revenue 49, operating income 43, employees 3. The metric `employees` has almost no data in the facts documents |
| Blueprint §8.7: `filed_date` ≥ `period_end`, warn otherwise | 166 of 1,255,672 facts were filed before the end of their period; the rows are kept and counted. No value needed rounding to six decimal places, and no document repeated a fact |
| WP-06 step 9: about 130 requests | 81 requests |

**The watchlist.** Version 0 has the 5 demo companies, 13 companies of the gold sheet and 32 companies by news volume. Three candidates have no periodic report (easyJet, Space Exploration Technologies and Deutsche Lufthansa), and four names are common words (Shell, Visa, Intel and Oracle). `config/watchlist/README.md` has the rules, the list of what was left out and the way to change a row.

**The gold sheet (open item of phase P0).** `benchmarks/source_profile/compare_gold_sheet.py` compared the 26 rows of `config/labelling/gold_sheet_v0.csv` which carry a CIK with the snapshot of 2026-11-28: all 26 CIKs are in the universe. The labels of phase P0 were made with a copy of the ticker list of 2026-10-22 and without a request to the SEC. No label is changed.

| String of the sheet | Label | CIK | Name of the SEC | Primary ticker |
|---|---|---|---|---|
| Instagram | `company_in_universe` | 1326801 | Meta Platforms, Inc. | META (Nasdaq) |
| Reuters | `company_in_universe` | 1075124 | THOMSON REUTERS CORP /CAN/ | TRI (Nasdaq) |
| Nvidia | `company_in_universe` | 1045810 | NVIDIA CORP | NVDA (Nasdaq) |
| Google | `company_in_universe` | 1652044 | Alphabet Inc. | GOOGL (Nasdaq) |
| Facebook | `company_in_universe` | 1326801 | Meta Platforms, Inc. | META (Nasdaq) |
| Youtube | `company_in_universe` | 1652044 | Alphabet Inc. | GOOGL (Nasdaq) |
| Espn | `company_in_universe` | 1744489 | Walt Disney Co | DIS (NYSE) |
| Amazon Autos International | `company_in_universe` | 1018724 | AMAZON COM INC | AMZN (Nasdaq) |
| Easyjet | `company_in_universe` | 1470419 | easyJet plc | ESYJY (OTC) |
| Nasdaq | `company_in_universe` | 1120193 | NASDAQ, INC. | NDAQ (Nasdaq) |
| Disney | `company_in_universe` | 1744489 | Walt Disney Co | DIS (NYSE) |
| Mattel | `company_in_universe` | 63276 | MATTEL INC /DE/ | MAT (Nasdaq) |
| Netflix | `company_in_universe` | 1065280 | NETFLIX INC | NFLX (Nasdaq) |
| New York Times | `company_in_universe` | 71691 | NEW YORK TIMES CO | NYT (NYSE) |
| Linkedin | `company_in_universe` | 789019 | MICROSOFT CORP | MSFT (Nasdaq) |
| Walmart | `company_in_universe` | 104169 | Walmart Inc. | WMT (Nasdaq) |
| Microsoft | `company_in_universe` | 789019 | MICROSOFT CORP | MSFT (Nasdaq) |
| Chevron | `company_in_universe` | 93410 | CHEVRON CORP | CVX (NYSE) |
| Cessna | `company_in_universe` | 217346 | TEXTRON INC | TXT (NYSE) |
| Twitter | `company_in_universe` | 1181412 | SPACE EXPLORATION TECHNOLOGIES CORP | SPCX (Nasdaq) |
| Cnn | `company_in_universe` | 2041610 | Skydance Corp | SKYD (NYSE) |
| Hasbro | `company_in_universe` | 46080 | HASBRO, INC. | HAS (Nasdaq) |
| Eastern Daily | `company_in_universe` | 1579684 | USA TODAY Co., Inc. | TDAY (NYSE) |
| shell | `ambiguous` | 1306965 | Shell plc | SHEL (NYSE) |
| Sony | `company_in_universe` | 313838 | Sony Group Corp | SONY (NYSE) |
| Eurowings | `company_in_universe` | 1049724 | DEUTSCHE LUFTHANSA A G /FI | DLAKY (OTC) |

**Not verified.** No test ran against S3 and Glue: the AWS halves follow in WP-10 and WP-11. The live load ran once, on one day: the behaviour of a second day (a snapshot from the snapshot before, the selection of the facts by new reports, the 5% rule) is proven on the recorded files only. The limits R2-5 and R2-8 of review 2 stay.

## The unit of the financial facts (change C15)

`benchmarks/p2_lake/refresh_facts.py` builds a made-up facts table of 50 companies with 26,000 facts each (1.3 million rows) in one commit, and then replaces the facts of 3 companies with `overwrite_scope` and a unit of keys. Each step runs in a process of its own, in the toolbox of the demo stack (Linux aarch64 in Docker Desktop, 7.65 GiB), on 2026-11-28 at 03:58 UTC. The raw result is `benchmarks/p2_lake/results/refresh_facts.json`.

| Refresh of 3 of 50 companies | Table without a partition | Table partitioned by `cik` |
|---|---|---|
| Duration of the commit | 1.52 s | 0.08 s |
| Peak memory of the process | 1,029 MiB | 284 MiB |
| Data files kept unchanged | 0 of 1 | 47 of 50 |
| Bytes written | 5.48 MB: the whole table again | 0.72 MB: three files |
| Snapshots of the commit | overwrite, append | delete, append |

The numbers confirm change C15: `sec_xbrl_fact` is partitioned by `cik`. The made-up rows compress far better than real facts, so the bytes are a lower bound; the ratio between the two columns is the result. A unit of keys holds at most 200 values: above 200 values PyIceberg 0.12.0 reads every data file.

## The recording of the SEC test files (WP-04)

The 18 requests of the recording are the first requests of week 3 to the SEC. Every time is UTC on 2026-11-28.

**Before the first request.**

| Step | Result |
|---|---|
| Review 1 | Pull request #19 with every repair of review 1 was merged at 04:36:08 |
| Rehearsal | The step `rehearse` of the recorder ran the same code against a made-up SEC inside the process, with a throwaway database, on the demo stack: 18 requests, at most 8 in any second, and no request left the laptop |
| Copy of the working database | The working stack was stopped. One cold copy of the PostgreSQL volume of the working stack stands in `data/backups/gbmi-postgres-data-20261128T0437Z-before-0004.tgz` (19 MB, git-ignored) |
| Migrations | `0004_sec_objects.sql` and `0005_partition_units.sql` were applied to the working database at 04:37:25. The two files are frozen from this moment (change C22): a later change of the schema is a new file |
| Ledger of phase P1 after the migrations | Unchanged: 9,432 slots in the state `loaded`, and 507 partition versions for 2026-10-26 to 2026-11-27 |
| Stack | `gbmi config show` in the toolbox of the working stack: `sec_source` is `live`, `sec_max_rps` is 8, and `sec_user_agent` shows a mask. `gbmi sec circuit status`: `closed`, no event |

**The recording.** `make toolbox CMD="python -m benchmarks.source_profile.record_sec_fixtures fetch"`, in the toolbox of the working stack, with the database of the working stack.

| Measure | Value |
|---|---|
| Run in `ops.source_request` | `record-sec-fixtures-20261128T043750Z` |
| First and last request | 04:37:51 and 04:37:58 |
| Requests | 18: one for each address of the plan, no second try |
| Outcomes | 16 `ok` (HTTP 200) and 2 `missing`: HTTP 404 for the facts document of easyJet, and HTTP 403 with the body `AccessDenied` for the index file of the holiday 2026-11-26 |
| Largest number of requests in any second | 4 by the wall clock, and 4 by the monotonic clock of the client (`max_requests_in_window` and `max_requests_in_session_window`). The ceiling is 8 |
| Blocks | None. The circuit of the working stack is `closed` |
| Raw answers | 19 MB in `data/sec-raw/`, git-ignored, with the manifest and the 18 rows of the request log |
| Test files | 19 files with 1.94 MB in `tests/fixtures/sec/`, written by the step `shorten` without a network. `ATTRIBUTION.md` in the folder lists the size and the MD5 hash of each raw answer and of each test file |

**What the recorded files show beside the findings of the plan.** The models follow the files (week 3 plan, WP-04, "When the work fails").

| Finding of the plan | The recorded files |
|---|---|
| F4: `filings.recent` holds "at most 1,000 filings" | The lists hold 1,010 filings for Mattel, 1,005 for Nvidia and 1,001 for Alphabet. The model sets no upper limit |
| F5: "Every fact has `end`, `val`, `accn`, `fy`, `fp`, `form` and `filed`"; `fp` is null for some forms | `fy` is null wherever `fp` is null: 40 facts of Mattel, 46 of Nvidia, 160 of Meta and 1,219 of Microsoft. Both fields are optional in the model. `label` and `description` of a concept are null for 398 of the 399 concepts of Thomson Reuters |
| F9: the body of the 403 answer has 211 bytes | 263 bytes on 2026-11-28. The client reads the code `AccessDenied` and no length |
| F10: the listing names 38 index files up to 2026-11-25 | 39 files: the file of 2026-11-27 arrived. The listing names five files for each day (`company`, `crawler`, `form`, `master`, `sitemap`): the platform reads `form.YYYYMMDD.idx` only |
| F11 (d): 13 rows carry an earlier date | 40 rows carry an earlier date, on 29 dates back to 2025-12-01. The forms are `DRS`, `DRS/A`, `CORRESP`, `UPLOAD`, `EFFECT`, `QUALIF`, `ATS-N/CA`, `TA-1` and `TA-1/A`: drafts, letters and notices which the SEC publishes later. The unit of work stays the index file (difference D33) |
| F11 (c): 124 rows repeat a pair | Confirmed: 116 pairs stand in 240 rows, so 124 rows are repeats |
| §3.3: the metric `employees` reads `dei:EntityNumberOfEmployees` | None of the five recorded facts documents holds the concept. The demo has no value for the metric. The live load of WP-06 measures the coverage for the 50 watchlist companies |
| WP-04 step 3: the index rows of the five demo companies | Two of the five demo companies have a row in the index file of 2026-11-25: Meta (form `4`) and Microsoft (form `PX14A6G`). Hasbro has the third row (form `4`). The contract test compares the three rows with the submissions documents, and all 300 rows with the positions of finding F11 (a) |

**Not verified.** The recorded index file holds no form longer than 14 characters and no byte above 127. The parser has tests on made-up lines for both cases, and no test on a real line.

## The SEC models and the mart build on the live data (WP-08)

**The models.** dbt builds seventeen models in the run of `marts_build`: fifteen for the SEC and the two operational models of phase P1.

| Layer | Model | One row for each |
|---|---|---|
| Staging, 5 views | `stg_sec__company_snapshot`, `stg_sec__company_former_name`, `stg_sec__filing_index`, `stg_sec__filing_detail`, `stg_sec__xbrl_fact` | row of the pinned table `lake.sec_*`; the columns are renamed and nothing else |
| Intermediate, 5 views | `int_company__watchlist` | company of the first `GBMI_WATCHLIST_SIZE` rows of the seed |
| | `int_company__snapshot_versions` | company and run of consecutive snapshots with equal attributes (blueprint §7.10) |
| | `int_company__former_name_versions` | company and range of a name before the first snapshot, with the status of the range |
| | `int_financials__metric_facts` | company, metric, unit and period: the first concept of the seed which has a value, with the first reported value and the newest known value |
| | `int_financials__quarters` | company, metric, unit and quarter: the three-month fact, or the year minus the nine months with a flag |
| Marts, 5 tables with a dbt contract | `dim_company` | company and validity interval |
| | `fct_filing` | filing and filer |
| | `fct_filing_item` | filing, filer and item of a form 8-K |
| | `fct_financial_fact` | company, concept, unit, period and filing, with the knowledge time `known_at` |
| | `fct_financial_metric` | company, metric, kind of period, period and unit |

Each mart is rebuilt in full and carries `_silver_snapshot_id`, `_pipeline_version`, `_built_at` and `_dbt_invocation_id`. The macro `facts_as_of` gives the values which were known at a time; the variable `facts_as_of` builds the two metric models as they were known at a past time.

**The tests.** `make dbt-build` on the demo stack: 188 nodes pass, with 159 data tests and 8 unit tests. Fourteen of the data tests are queries of their own: the declared grain of each mart, no overlap of the intervals of a company, one current row for each company, and three checks of a mart against silver (every fact, every filing, every company of the snapshots).

| # | dbt unit test | What the made-up rows prove |
|---|---|---|
| 1 | `snapshot_versions_a_b_a_gives_three_intervals` | The attributes A, B, A give three intervals; equal attributes on consecutive days stay one interval |
| 2 | `former_name_versions_end_where_the_snapshots_start` | Six companies, one for each case of the rule for former names; the last range ends on the first snapshot date |
| 3 | `dim_company_joins_former_names_and_snapshots_without_an_overlap` | The dimension joins both sources; a dropped range is no row; only the last interval is current |
| 4 | `financial_fact_known_at_is_the_moment_at_which_the_filing_was_public` | Six filings: the acceptance time, no acceptance time, an acceptance after 17:30 New York time in winter and in summer, over a weekend, and at 06:00 |
| 5 | `metric_facts_keep_the_first_and_the_latest_value_of_a_restated_fact` | A restated year gives a first and a latest value; nine months and three months with one period end stay two rows; the first concept of the seed wins; six months are a half year |
| 6 | `metric_facts_as_of_a_time_before_the_restatement_give_the_first_value` | A question for a time before the restatement gives the first value; a fact of the same second counts, a fact one second later does not |
| 7 | `quarters_derive_the_missing_quarter_as_year_minus_nine_months` | The derived quarter with its flag; no quarter from a half year; a reported quarter stays; two concepts give no quarter |
| 8 | `financial_metric_compares_with_the_same_period_one_year_before` | A year of 52 weeks finds the year before two days off; a loss which shrinks is a positive change; a base of 0 gives no change; 8 days off find nothing |

The first unit test passed seven minutes after the start of the work package, inside the time box of 30 minutes. A changed expected value made the same test fail.

The other checks on the laptop, after the last change: `make lint` clean; `make test` with 857 tests and the coverage gate at 98%; `make test-dags` with 32 tests, 10 of them for `marts_build`; `make integration` with 93 tests and 4 expected failures of phase P0; `make redact-check` passed; `make demo` twice in a row with 31 end-to-end tests, 9 of them for the marts (211 and 212 seconds).

**`marts_build`.** One run has 43 tasks: the pin task, one run task and one test task for each of the 17 models and the 2 seeds, three tests which read two models and run after both, and the record task. No task runs a dbt unit test. The pin task pins the five tables and creates a missing table empty first; the record task reads the snapshot and the pipeline version from each mart and fails the run when a mart does not come from the pinned tables. On a reset demo stack, before any SEC pipeline had run, the run created the five tables empty, built every model with zero rows and passed every test (67 seconds); the three SEC DAGs then loaded the same tables.

**The build on the live data of the laptop.** `marts_build` ran on the working stack on 2026-11-28 at 10:50 UTC and, after the repair of change C23, at 10:56 UTC: 43 tasks in 61 seconds each time, with the same row counts. No request went to the SEC: the request log holds 99 requests before and after. The DAG is paused on the working stack; `benchmarks/p2_sec/marts_evidence.py` unpaused the DAG for each run, paused the DAG again and wrote `benchmarks/p2_sec/results/marts-build.json`.

| Mart | Rows | What the rows hold |
|---|---|---|
| `dim_company` | 18,150 | 7,988 companies with one current row each (1,102 of them without a SIC code), and 10,162 rows from former names for 4,027 companies, back to 1993-08-13. 50 companies carry `is_watchlist` |
| `fct_filing` | 218,367 | 184,268 filings of 36,758 filers in 301 forms. 122,041 rows come from the 23 index files only, 90,607 from the submissions documents of the 50 watchlist companies only (back to 1999-02-11), and 5,719 from both. Three pairs of filing and filer stand in two index files and count once |
| `fct_filing_item` | 8,044 | 3,902 filings of the 8-K family (8-K, 8-K/A, 8-K12B, 8-K12B/A) of 44 companies, with 24 item numbers. The most frequent: 9.01 (2,937), 2.02 (1,291), 8.01 (990), 5.02 (826) |
| `fct_financial_fact` | 1,255,672 | Every fact of silver, for 50 companies, 5,560 concepts and 110 units |
| `fct_financial_metric` | 22,716 | 8,969 balance dates, 7,652 quarters (897 of them derived), 2,262 years, 1,938 half years and 1,895 periods of nine months |

*The company dimension.* CIK 1326801 on the live data: "Facebook Inc" from former-name data, 2005-05-06 to 2021-10-27; "Meta Platforms, Inc." from former-name data, 2021-10-27 to 2026-11-28; "Meta Platforms, Inc." from the snapshot of 2026-11-28 on. The 6,489 former names give 6,135 kept ranges. 338 ranges of 331 companies hold no day and are dropped: the name starts on the same day as the next name of the company, or starts and ends on one day. 16 ranges of 16 companies end before their start and are dropped. Each of the 4,027 companies has one row for its current name from the end of its last former name to the first snapshot date.

*The knowledge time of the facts.* 518,274 facts of 10,434 filings carry the acceptance time. 55,759 facts of 202 filings of 24 companies were accepted after 17:30 New York time and are known from 06:00 New York time on the filing date (change C23). 681,639 facts of 1,546 filings have no acceptance time and are known from 05:00 UTC of the day after the filing date: a submissions document lists the newest filings of a company only, and the facts reach back to 2009.

*The coverage of the metrics.* The numbers of companies equal the numbers of the live load of WP-06.

| Metric | Companies of 50 | Rows | Derived quarters | Restated rows |
|---|---|---|---|---|
| `cash` | 50 | 3,008 | | 66 |
| `equity` | 50 | 3,029 | | 104 |
| `net_income` | 50 | 5,220 | 328 | 345 |
| `total_assets` | 50 | 2,901 | | 198 |
| `revenue` | 49 | 4,712 | 296 | 513 |
| `operating_income` | 43 | 3,815 | 273 | 350 |
| `employees` | 3 | 31 | | 0 |

47 companies report in US dollars, two in pounds, one in euros and one in yen; one company has values in two currencies. The three companies with `employees` use three unit names (`employee`, `pure`, `numberOfEmployees`). 260 metric facts of 8 companies describe a period of another length than a quarter, a half year, nine months or a year, and are no rows of the mart.

**Findings of the work package.**

1. **The knowledge time (change C23, difference D51).** The blueprint and the plan take the acceptance time of a filing as `known_at`. The live build showed filings which EDGAR accepted on the evening before their filing date: the report of Mattel for the third quarter of 2026 was accepted on 5 November at 22:58 UTC (17:58 in New York) with the filing date 6 November. The SEC gives a filing which arrives after 17:30 New York time the filing date of the next business day and publishes the filing at 06:00 New York time on that date. The acceptance time lies before the public moment, so a question "as of" the night in between would use a number which nobody outside the SEC knew. `known_at` is now the later of the acceptance time and 06:00 New York time on the filing date. The rule of the SEC was read in the summary of a web search of 2026-11-28, which quotes the guidance of the SEC on filing dates; the page itself was not opened, because week 3 allows no request to a page of the SEC outside the fair-access client.
2. **No right for temporary tables (change C24).** The plan expected that a dbt unit test needs a new right for the role `dbt_rw`. The test passed with the right and after `REVOKE`: ClickHouse 26.8 lets a role which may create a table in a database create a temporary table. The role is unchanged.
3. **A name inside an aggregate function.** ClickHouse reads `min(index_date) as index_date` beside `argMin(form, (index_date, line_number))` as an aggregate inside an aggregate and refuses the query. The aliases of `fct_filing` have other names than the columns.
4. **The rights behind a view.** ClickHouse checks the rights of the reader on every table behind a view. The pipeline user could read the database `intermediate` and could not read a view which reads a seed. The role `pipeline_rw` now reads the database `seeds`.
5. **A table without a snapshot.** ClickHouse 26.8 reads an Iceberg table which holds no snapshot: zero rows, with the columns of the schema (integration test `test_pin_tables.py`).
6. **Items outside the 8-K family.** The submissions documents list items for 37 filings of the form `CT ORDER` and 36 of the form `EFFECT`. `fct_filing_item` holds the 8-K family only; the two forms stay in `fct_filing` with their items.

**Not verified.**

- The time of the workflow `e2e` with `marts_build`: the check of the pull request shows the time.
- The schedule of 11:45 UTC: the demo stack has no schedule, and the DAG is paused on the working stack. The first scheduled run is a run on the host (WP-13).
- The dbt target `prod`: WP-10 adds the output; a DAG test proves that the setting `DBT_TARGET` reaches the tasks.
- The two addresses of a filing in `fct_filing`: the forms come from the page "Accessing EDGAR Data" of the SEC, read in the summary of a web search; no address was opened.
- The collapse of equal snapshots over several days on real data: the laptop holds one snapshot date, so only the unit test proves the rule. The host adds a snapshot each day (WP-13).
- How often a year and its nine months use two concepts, so that no quarter is derived: not counted.

## Choices made during the run

Decisions which the week 3 plan left open, in the order of the work.

| # | WP | Choice | Why |
|---|---|---|---|
| 1 | 01 | PostgreSQL stays on the image digest of phase P1 | The tag `17` points at a new build of the same release 17.12. The rule of the plan takes a newer patch release, and a new build is not a release |
| 2 | 02 | `ops.source_object` has a check for the five dataset names, a column `detail` (jsonb) and the count `raw_records` | The plan fixes the five datasets (§3.3). Free detail, for example the archive from which a document came, needs no new column. A file of the SEC holds records, not lines |
| 3 | 02 | A failed try of gate G1 keeps the state `expected` and raises the counter `attempts`. `expected → verified` is one statement | The plan: "download and gate G1 in one transaction". An object has no state between the request and the gate |
| 4 | 02 | `mark_loaded` keeps an object which is loaded already and takes the new accounting | The rule of the slot ledger of phase P1: a rebuild is no change of state |
| 5 | 02 | A requeue keeps the bronze address of a quarantined object | The SEC keeps no old version of a document. The stored bytes can pass a repaired gate without a new request |
| 6 | 02 | The functions for the circuit row live in `gbmi.ops.circuit`; the plan lists `gbmi.ops.requests` only (§8) | One module for each table. The rules of a block follow in `gbmi.sources.sec.circuit` (WP-03) |
| 7 | 02 | A circuit row keeps `blocked_until` after the time has passed, until a probe or an operator closes the circuit | The plan: "The first request after a block is one probe". The recorded block tells the next client which request is the probe |
| 8 | 02 | A unit of the kinds `keys` and `table` has no `slot_date` | Every reader of phase P1 filters on `slot_date` or on the unit kind `day`, so no reader of phase P1 sees a row of the new kinds |
| 9 | 02 | `max_requests_in_session_window` stands beside `max_requests_in_window` | Change C14: the number by the monotonic clock of one client session does not depend on the wall clock of a machine |
| 10 | 03 | The default of `GBMI_SEC_SOURCE` is `off`. The Makefile sets `fixture` for the demo stack and `live` for the working stack, and nothing else changes the source: neither a value of the shell nor a value on the command line of `make` | A process without settings never calls the SEC. Review 1 (finding B2): live requests leave from one stack only, so one database remembers a block. The first version let the command line of `make` change the source for one command; change C22 withdraws the override |
| 11 | 03 | The lock of "one process at a time" is a PostgreSQL advisory lock whose key comes from the name of the source | The lock lasts as long as the connection of the client, and a killed process releases the lock. Tests use a source name of their own and do not wait for each other |
| 12 | 03 | `SecTransportError` is raised without the error of the transport as its cause | The error of httpx holds the request, and the request holds the headers. The message names the class of the error, the host and the path |
| 13 | 03 | The client checks the form of the user agent; the settings only store the value as a secret | A check in the client raises a fixed text. The settings never show the input of a failed validation |
| 14 | 03 | Only the object (status 200) closes the circuit after a block. A "no such object" answer to a probe for a listed object locks the circuit; for another object the answer decides nothing | Review 1 (finding 2): the first version closed the circuit on a "no such object" answer, so a block which looks like a missing file could never lead to a lock |
| 15 | 03 | `download` tries once and removes the partial file after a failure | The plan: "the archive is tried once in a run". A later run starts from nothing |
| 16 | 03 | The source `fixture` needs a transport as an argument until WP-04 adds the transport of the recorded files | The plan lists `transport.py` under WP-04. Until then no code path of the package can start a fixture client by accident |
| 17 | 03 | `tools/redact_check.py` is a script with the standard library only, and `tests/conftest.py` removes an inherited SEC source and identity from every test | The hook runs before any virtual environment exists. The Makefile exports the source of the stack, which is `live` for the working stack: no test may inherit the value |
| 18 | 05 | `value` of `sec_xbrl_fact` is a decimal with 38 digits and 6 decimal places; `value_text` keeps the number as the SEC wrote the number | The plan: "keep the number as text beside the decimal value". 38 digits hold every amount, and six decimal places hold a per-share value and a ratio. ClickHouse reads the column as `Decimal(38, 6)` |
| 19 | 05 | The label of a unit of keys is the column, the number of keys and 8 hexadecimal characters of a hash over the sorted keys, for example `cik:3:9b3f9976` | The same set of keys gives the same label. The keys themselves stand in the detail of the partition version |
| 20 | 05 | The three date-like units of the SEC tables use the unit kind `day` with `slot_date`: a snapshot date and the date of an index file | The question `day_rows` of phase P1 then finds the versions for the volume rules without new code |
| 21 | 05 | The made-up facts table and the measurement of change C15 live in `benchmarks/p2_lake/` | The plan lists no folder for the measurement. The rule of phase P1: a measuring script writes the raw result beside the script |
| 22 | 03 | The operator commands `gbmi sec circuit status` and `gbmi sec circuit close --note` arrive in WP-03; the plan lists the command group `gbmi sec` under WP-06 | Review 1 (finding B2): the error text of a locked circuit names the close command, so the command has to exist before the first live request |
| 23 | 03 | The client has no HEAD request, and the plan names `head` | Review 1 (finding 1): a HEAD answer has no body, so a missing file cannot be told from a block. The answer of a download carries the size and the time which the manifest of the archive needs |
| 24 | 04 | The recorder continues a run which stopped early, and has a step `rehearse` | A stored answer is not asked again, so a failure after request 12 costs 6 more requests and not 18. The rehearsal runs the same code against a made-up SEC inside the process before the first live request |
| 25 | 04 | One cold copy of the PostgreSQL volume of the working stack before the two migration files were applied | The working database holds the ledger of 33 days, and a migration cannot be taken back. The copy is the way back to the state before `0004` |
| 26 | 04 | The two recorded "no such object" bodies hold the letter X in place of the request identifier and the host identifier | The two values name one request of the recording in the logs of the SEC and nothing else. The length of each body stays as recorded |
| 27 | 04 | The shortened listing keeps the five entries of the index day, and a shortened submissions document also keeps every filing which the recorded index file names | The pipeline of WP-06 has to pick the form index among the files of a day. The contract test compares the form of an index row with the form of the same filing |
| 28 | 04 | A document without a required value fails as a whole with `PayloadError`. The error names at most five problems, each with its path, and shows no value of the document | The plan: "A document without a required field is quarantined" (WP-06). A value of a document does not belong in a log line or a run record |
| 29 | 04 | A number of a facts document is read as a decimal from the text of the JSON document | Choice 18: `value_text` keeps the number as the SEC wrote the number. A binary float would turn `1.10` into `1.1` and would lose digits of a long number |
| 30 | 04 | The index parser refuses a row without a company name, a row whose form runs past position 16, and a row whose CIK column differs from the CIK of the file name | The contract of `sec_filing_index` requires the name. Finding F11 (a) and (e): no row of the recorded file breaks one of the three rules, so a row which does is a sign of a changed format |
| 31 | 04 | The transport of the recorded files reports one fixed time of the last change for every file: 2026-11-26 05:06:36 UTC | The demo runs with a fixed clock, and two runs have to see the same answers. The time lies before the clock of the demo |
| 32 | 04 | The block `seeds` of `dbt/dbt_project.yml` arrives with the metric seed in WP-04; the plan lists the block under WP-08 | Without the block, `dbt seed` would write the seed into the database `marts` with guessed column types. `dbt seed` ran on the demo stack: 22 rows in `seeds.metric_concept_map` |
| 33 | 06 | The listing of a quarter is the object `index.<year>QTR<quarter>.json` of the day of the run; an index file is the object `form.YYYYMMDD.idx` of the date of the file | Each index file is an object of its own and is never superseded. A newer listing supersedes an older open listing |
| 34 | 06 | A listing and the archive become `loaded` with a reason of their own; neither gives a row to silver | Every object has to reach a final state (WP-06, "Done when") |
| 35 | 06 | Gate G1 is tried up to three times inside one run | An object belongs to one day. The object of the next day is another object, so three tries on three days cannot happen |
| 36 | 06 | A failed gate G3 quarantines the index file or the ticker file (`verified → quarantined`). The operator sends the object back and runs the command with `--accept-volume` | The transition is one of the ten of the plan. The bytes stay in bronze, so the second build sends no request |
| 37 | 06 | The partition versions of `sec_filing_detail`, `sec_company_snapshot` and `sec_company_former_name` store the committed rows only | The rows are not the lines of one source file: stored rows are kept, and a snapshot row joins two sources |
| 38 | 06 | The reference pipeline reads each document which is newer than the attributes of the snapshot before, not only the documents of the day | A document of an earlier day whose run failed is not lost, and the object reaches a final state |
| 39 | 06 | A daily run asks for the documents of at most 200 new companies | A bound for a day on which the ticker file changes much. The archive of the next Sunday brings the rest |
| 40 | 06 | The facts selection treats `not_published` as an answer | A company without a facts document is not asked again each day |
| 41 | 06 | `--accept-volume` of `gbmi sec facts` accepts a fall of the facts | The plan gives each of the three commands the option |
| 42 | 06 | The module `gbmi.sources.sec.silver` holds what the three pipelines share; the plan lists seven modules | One place for the run, the commit and its record |
| 43 | 06 | A company joins the watchlist with an annual report among the filings of its document, or with a periodic report in a loaded index file | The document of a company with very many filings can end before its last annual report (finding F4). The index files of 2026-10-26 and later hold the quarterly reports of the autumn |
| 44 | 06 | The setting `GBMI_WATCHLIST_FILE` names the seed of the watchlist | A task of Airflow does not run in the repository folder |
| 45 | 06 | The day of a former name is the UTC date of its time, not the date in New York of finding F3 | The live load found three forms of the time: midnight UTC, 04:00 UTC and 05:00 UTC. The UTC date is the day which the SEC means for each of the three |
| 46 | 06 | The names Shell, Visa, Intel and Oracle are left out of the news part of the watchlist as common words | The plan: "without names which are common words". The four were read in the result of a dry run; Jay can change a row by pull request |
| 47 | 06 | `benchmarks/p2_sec/live_load_evidence.py` writes the numbers of the live load | The rule of phase P1: a measuring script writes the raw result beside the script |
| 48 | 09 | The drill moves a clock of its own; a wait of 15 minutes takes no real time | The plan: "with a controlled clock". The drill runs in the workflow `e2e` on every pull request |
| 49 | 09 | The runbook gives the statement of the SEC on the end of a block as the blueprint records the statement, and says that the page was not read again | A request to a page of the SEC outside the fair-access client is not allowed in week 3. One web search on 2026-11-28 found the limit of 10 requests in a second on the developer page of the SEC and found the 10 minutes only in a text of 2015 |
| 50 | 07 | Each SEC DAG ends with a task `summary` which reads the record of the run and fails when the pipeline did not succeed | The plan names the task and not its work. The DAG run then shows the error of the pipeline, and the summary lists the objects of the day which still need work |
| 51 | 07 | The SEC DAGs take the clock of the run as `as_of`, and have no logical date | The plan: "A run which starts late reads the object ledger and does the same work" |
| 52 | 07 | The end-to-end test of the SEC DAGs holds for the first demo of a stack and for a later one | `make demo` has to pass twice in a row: the second demo finds every object of the fixed day loaded |
| 53 | 08 | The role `dbt_rw` gets no right for temporary tables (change C24) | The unit test runs without the right. A right which nothing needs is no part of a role |
| 54 | 08 | The rule for former names in full: a range is cut at the first snapshot date; the current name starts on the later of the two dates of the last former name; a former name without a last date holds until the first snapshot; a range which holds no day stays in the intermediate model with its reason (`ends_before_start`, `empty`, `after_first_snapshot`) | The plan gives the four steps of the rule and "dropped and counted". With the three additions the ranges of a company never overlap and never pass the first snapshot, also when a name changes after the first snapshot. One query on the intermediate model counts the dropped ranges |
| 55 | 08 | The unit and the kind of the period are part of the grain of `fct_financial_metric` (difference D52) | One of the 50 companies has values in two currencies: a newer filing restates old periods in another currency. A first value in one currency and a latest value in another must not stand in one row |
| 56 | 08 | `fct_financial_metric` holds half years and periods of nine months beside quarters, years and balance dates | The four lengths are the lengths of the plan (§3.3). A report for the second or the third quarter gives the year to date beside the quarter, and a company which reports half years only has no quarter at all: 45 of the 50 companies have half-year values and 46 have quarter values |
| 57 | 08 | A metric of a period takes one concept: the first candidate of the seed with a value. The first and the latest value come from that concept. A quarter is derived only when the year and the nine months use the same concept | Two candidate concepts can mean two things in one filing (all revenue, and revenue from contracts). A difference of two concepts would look like a restatement |
| 58 | 08 | The change against the year before compares the newest known values and divides by the absolute value of the year before | The plan names "the change against the same period one year before" and no formula. A loss which shrinks then shows a positive change. A base of 0 gives no change |
| 59 | 08 | `known_at` is the later of the acceptance time and 06:00 New York time on the filing date (change C23, difference D51) | Finding 1 of the section "The SEC models and the mart build on the live data" |
| 60 | 08 | `fct_filing` takes the form and the filing date from the index row when the index and the submissions document both hold the filing. `detail_source` has three values: `index_only`, `submissions_only`, `index_and_submissions`. The model gives two addresses: the index page of the filing and the main document | The index is the one source which covers every filer. The plan names the column and "the address of the filing"; the main document is the page which a reader wants to open (use case UC-1) |
| 61 | 08 | `fct_filing_item` holds the forms which start with `8-K` | The blueprint: "filing × 8-K item". Two other forms carry items in the submissions documents (finding 6) |
| 62 | 08 | The intermediate models are views. `fct_financial_fact` is built from the lake once, and the two metric models read the mart table | A view over the lake is read again by each model and each test after it. The 1.26 million facts are read from Parquet once in a run |
| 63 | 08 | `marts_build` gives each model one run task and one test task (`TestBehavior.AFTER_EACH`); a test which reads two models is a task of its own after both | The plan's first response to a slow run, one task for each model, was not needed (finding RF21: 61 seconds). Without the own task, Cosmos would run such a test with the first of its two models, before the second model is built |
| 64 | 08 | The last task of `marts_build` records the row counts and also checks the provenance of each mart | The plan: "record the row counts". The check costs one query for each mart and proves that dbt received the snapshots of the pin task |
| 65 | 08 | The as-of logic lives in the macro `facts_as_of`, and the model `int_financials__metric_facts` calls the macro with the variable `facts_as_of` | A dbt unit test checks a model, not a macro. With the variable, the unit test asks the question for a past time, and an operator can build the metric models as they were known on a past day. ClickHouse refuses a value which is no time, so a wrong value stops the build |
| 66 | 08 | The dbt unit tests run inside the pytest call of `make demo`, with `dbt test --select test_type:unit`, after `marts_build` | Finding RF14. The plan (WP-08 step 9) names `dbt build` with the unit tests; `dbt test` builds no model again |
| 67 | 08 | The role `pipeline_rw` reads the database `seeds`; `make lint` and the commit hook lint the SQL files of `dbt/tests` | Finding 4. The test queries were the only SQL files of the project which no check read |
