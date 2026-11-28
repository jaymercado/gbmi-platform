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
| 1 | No second holds more than 8 requests to the SEC: (a) in the tests, (b) in the request log of the AWS host | Open | |
| 2 | The company dimension reproduces the name change from "Facebook Inc" to "Meta Platforms, Inc." from former-name data | Open | |
| 3 | The dbt unit tests for validity intervals and for as-of logic pass | Open | |
| 4 | The host is reachable only through Session Manager | Open | |
| 5 | The platform runs on AWS: the stack on the host uses S3 and Glue, and two GDELT DAGs ran for at least 6 hours in a row, in a period which holds one scheduled daily run | Open | |
| 6 | The three SEC DAGs each completed one scheduled run on the host; silver on the host holds the data of the criterion; every SEC object is in a final state | Open | |
| 7 | Drill DR5 passes as a script, and the runbook for a block by the SEC exists | Open | |
| 8 | The delivery path works from end to end: image, deployment through the environment `prod`, `terraform plan` in a pull request, smoke test | Open | |
| 9 | Remote logging works: task logs on S3, container logs in CloudWatch with a retention of 30 days | Open | |
| 10 | The four new Terraform modules are applied; `make plan` reports no changes; `make tf-check` passes; no secret in the state; no account number and no address in the repository | Open | |
| 11 | The first dbt documentation is published | Open | |
| 12 | The host is measured: memory, the build of one GKG day, CPU credits, commit latency on S3 | Open | |
| 13 | The Git commit is the pipeline version on the host | Open | |
| 14 | `make demo` passes on the laptop and in CI with the recorded SEC files; the four workflows of phase P1 are green on `main`; ADR-010, ADR-011 and ADR-015 exist; five contract files; the gold-sheet comparison; the version pins | Open | The version pins: checked on 2026-11-28 (section "Version pins") |

## Work packages

The order follows change C10 of the week 3 plan (§3.5). Every time is UTC. The working time of a work package excludes the waits: GitHub checks, downloads, reviews, answers of Jay and the run on the host (change C21).

| WP | Subject | Start | End | Pull request | State |
|---|---|---|---|---|---|
| 01 | Start: checks, pins, plan in the repository | 2026-11-28 03:10 | | | Running |
| 02 | Operational tables for SEC objects, circuit and requests | | | | |
| 03 | Fair-access client; review 1 | | | | |
| 05 | Lake: general units and five SEC tables | | | | |
| 04 | Recorded files and payload models | | | | |
| 06 | Pipelines PL-3, PL-4 and PL-5; the watchlist; review 2; one live load | | | | |
| 09 | Drill DR5 and one runbook | | | | |
| 07 | DAGs, DAG tests and the demo | | | | |
| 08 | dbt models, unit tests and `marts_build` | | | | |
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
| RF1 | 02 | `tests/ledger/test_schema.py:73-87` expects exactly six tables in the schema `ops`; lines 260-267 expect the constraint name `silver_partition_unit` | The table list of the test grows by four names. `0005` drops the check and adds the wider check under the same name | [RV] | Open |
| RF2 | 02, 05 | `ops.silver_partition`: `feed`, `file_count`, `fileset_hash` and `content_digest` are NOT NULL (`0002_silver_partition.sql:8,25-27`), and the one index starts with (`table_name`, `slot_date`). `PartitionVersion` requires `slot_date` and derives the unit kind from `slot_hour` (`src/gbmi/ops/partitions.py:29,49-51`) | An SEC unit writes `feed` = the dataset, `file_count` = the objects of the unit, `fileset_hash` = a hash over the names and the MD5 hashes of the objects, `content_digest` = the checksum of the committed rows. `0005` adds an index on (`table_name`, `unit_label`). `PartitionVersion` gains optional fields with defaults, so each call of phase P1 stays unchanged | [RV] | Open |
| RF3 | 02 | `StaleStateError` takes a GDELT key (`src/gbmi/ledger/repository.py:135-145`). The trigger function `ops.refuse_change` refuses every UPDATE of a table (`0001_ledger.sql:18-22,113-114`) | One base class for the stale error with a subclass for objects. `ops.source_request` gets an own row trigger: no DELETE, no change of the fixed columns, one update of the outcome | [RV] | Open |
| RF4 | 02 | The migration runner needs the numbers 1 to n without a gap (`src/gbmi/ledger/migrate.py:84-118`). The role `gbmi_ro` reads a new table through default privileges, and no migration file may name the role (`docker/postgres/roles/01_readonly.sql:7-8,20-22`) | The test "`0005` on a database with rows" applies a temporary folder with `0001` to `0004`, inserts rows of phase P1 and then applies the full folder | [RV] | Open |
| RF5 | 03 | `sec_user_agent` is plain text, and `gbmi config show` prints the settings as JSON (`src/gbmi/config.py:63`, `src/gbmi/cli.py:97-101`). `dict_tracebacks` of structlog 26.1.0 renders local variables (`structlog/tracebacks.py:49`). A run record stores the text of an exception (`src/gbmi/ops/runs.py:96`) | The type `SecretStr`. The renderer `ExceptionRenderer(ExceptionDictTransformer(show_locals=False))`. The settings hide the input of a validation error; the format check of the value lives in the client and raises a fixed text | [RV]; the echo of pydantic [S] | Open |
| RF6 | 03 | `HttpFetcher` belongs to GDELT: index lines, redirects followed, no header (`src/gbmi/sources/gdelt/land.py:142-182`). The fake GDELT server records file names only (`tests/ledger/gdelt_server.py:81`). `Settings` is frozen and cached, and `fixtures_dir` is the GDELT folder | `SecClient` is new code with an own `httpx.Client`. `tests/ledger/sec_server.py` records time, host, path and headers. A new field `sec_fixtures_dir`; a test which changes the environment clears the cache of the settings | [RV] | Open |
| RF7 | 03, 06 | The coverage gate is the list `COVERAGE_GATE` of the Makefile (`Makefile:117-124`). `make test` runs `tests/unit`, `tests/ledger`, `tests/transforms` and `tests/contract` only | WP-03 adds `src/gbmi/sources/sec/*` to the list. The pipeline tests which count for the gate live in `tests/ledger` | [RV] | Open |
| RF8 | 03, 12 | The value of `GBMI_SEC_USER_AGENT` stands in double quotes in `.env` | `make host-secrets` and `make redact-check` remove the quotes. One test of WP-03 proves which value a container receives through Compose | [V]; Compose [S] | Open |
| RF9 | 04, 07 | The fixtures hold one index file, and the demo clock is fixed. A weekday without a file stays `expected` until the listing names a newer file, and the volume band needs four earlier files | The recorded listing names the one recorded index file, so each earlier weekday of the window becomes `not_published`. The volume rule records "skipped" below four files. The demo clock lies after the publication of the file | [S] | Open |
| RF10 | 05 | `Unit` is imported in about 25 places and used by the quarantine helper, the audit functions and `DayFile` (`src/gbmi/lake/commit.py:51-159`, `src/gbmi/quality/audit.py:65,181`) | `Scope` is a protocol with `label`, `row_filter` and `check`. `Unit` keeps the constructor and fulfils the protocol. `overwrite_scope` is the general function, and `overwrite_unit` calls `overwrite_scope` | [RV] | Open |
| RF11 | 05 | `create_table_if_missing` always partitions by `slot_date`, and `_schema` always adds `_slot_ts` (`src/gbmi/lake/tables.py:51-83`). `tests/integration/lake/test_silver_tables.py:57` compares the fixture hour with `SILVER_SCHEMAS`. The contract test fixes the partition `slot_date` and six provenance columns (`tests/contract/test_silver_contracts.py:52-74`) | A parameter for the partition column and a switch for `_slot_ts`. The SEC schemas live in an own dictionary. The contract test reads the partition and the provenance columns from the definition of the table | [RV] | Open |
| RF12 | 05, 06 | `check_volume` has a fixed band of 0.5 to 2.0 and a fixed history of four values (`src/gbmi/quality/audit.py:36-38,257-298`). One commit of PyIceberg can hold three snapshots; the comment in `src/gbmi/lake/commit.py:8` says two | Parameters with the values of phase P1 as defaults; the 5% rule and the 2% rule are new functions. WP-05 tests the maintenance code with a commit of three snapshots | [RV] | Open |
| RF13 | 06 | `Bronze.read` loads a whole object, and `Bronze` has no method `exists` (`src/gbmi/sources/gdelt/land.py:258-301`). One test of phase P1 holds the behaviour of the `.part` file on the local file system | New methods `exists` and `size`. The archive never passes through `Bronze`. The local write path stays | [RV] | Open |
| RF14 | 07, 08 | `make demo` has no step for the GDELT DAGs: the DAGs run inside the one pytest call (`Makefile:236-250`, `tests/e2e/test_gdelt_dags.py`) | `tests/e2e/test_sec_dags.py` runs the three SEC DAGs and `marts_build` inside the pytest call, after the GDELT file, with the assertions of the week 3 plan. The dbt unit tests run after `marts_build` and build no model again; the step fails when no unit test ran | [RV] | Open |
| RF15 | 07 | `DEFAULT_ARGS` uses a growing delay, and the demo stack has 0 retries (`dags/common/__init__.py:30-42`). The lists and the loops of `tests/dags/test_dags.py` name the GDELT DAGs only (lines 23-47, 166, 209-225); each task needs a time limit (lines 109-114) | `SEC_ARGS`: `retries = min(TASK_RETRIES, 1)`, a delay of 20 minutes, no growth. The tests loop over every DAG | [RV] | Open |
| RF16 | 03, 07 | `GBMI_SEC_SOURCE`, `GBMI_SEC_FIXTURES_DIR` and `GBMI_WATCHLIST_SIZE` stand neither in the Compose environment nor in the stack block of the Makefile (`compose/compose.yaml:21-54`, `Makefile:18-45`) | WP-03 adds the first two variables, WP-07 the third | [RV] | Open |
| RF17 | 08 | A unit test of dbt-clickhouse 1.10.4 creates one temporary table named after the test and drops another name (`dbt/include/clickhouse/macros/materializations/unit.sql:9-13,37`). An input in the format `dict` or `csv` needs the input relation in the warehouse (`dbt/parser/unit_tests.py:140,195-203`). NULL for a column which is not Nullable becomes the default value of the type (`macros/utils/safe_cast.sql:6-12`). The role `dbt_rw` has no right to create a temporary table (`db/clickhouse/init/02_roles.sql`) | Each unit test has a name which is unique in the project. A fixture quotes dates and decimals and gives no NULL for a column which is not Nullable. The grant is added. Claude runs the first proof (week 3 plan, WP-08 step 1) early, on the demo stack, during the wait for the checks of WP-02. WP-08 commits the proof in the project of `tests/integration/dbt`, so the workflow `dbt` holds the proof of finding F18 of the week 3 plan in CI | [RV]; the name collision [S] | Open |
| RF18 | 08 | `dbt test` and `dbt build` accept `--exclude-resource-type` (`dbt/cli/params.py:254-280`). Cosmos 1.15.0 and later applies `RenderConfig.exclude` to the test task of each model. The toolbox inherits the environment of the Airflow services (`compose/compose.yaml:345`) | `marts_build` uses `RenderConfig(exclude=["resource_type:unit_test"])`, and a DAG test asserts the exclusion. No variable of the Compose environment removes unit tests: the toolbox would lose the unit tests of the demo without an error | dbt [RV]; Cosmos [S] | Open |
| RF19 | 08 | The DAG test skips only the task ids which start with `dbt.` (`tests/dags/test_dags.py:126`). Cosmos reads `dbt/target/manifest.json`, which the service `dbt-manifest` writes at `make up` (`compose/compose.yaml:266-273`). Cosmos builds a seed only when the selection holds the seed | The Cosmos group of `marts_build` has the name `dbt`. After a new model: `make up` again, or `dbt parse` in the toolbox. The selection holds the two seeds, and a DAG test asserts the two seed tasks | [RV]; the seeds [S] | Open |
| RF20 | 08 | The dbt project has no contract, no seed folder and no block for intermediate models, and `generate_schema_name` uses a custom schema as written. The demo reads 5 rows of a seed with 50 rows | The blocks of WP-08 step 2. A contract uses the exact type names of ClickHouse. One model `int_watchlist` cuts the seed with a dbt variable which takes the value of `GBMI_WATCHLIST_SIZE` | [RV]; the demo case [S] | Open |
| RF21 | 08 | Four DAGs with one dbt process for each Cosmos task can push the workflow `e2e` above 20 minutes; the limit of the job is 30 minutes (`.github/workflows/e2e.yml:23`) | WP-08 measures the workflow. First response: `TestBehavior.BUILD`, one task for each model. Second response: the row of week 3 plan §10 | [S]; the limit [RV] | Open |
| RF22 | 09 | `drills/common.py`: `world()` and `Drill` fit as written; `World.land`, `fixtures_dir()` and `AS_OF` belong to GDELT | SEC helpers beside the GDELT helpers; `DRILL_5` in the Makefile; one step in `e2e.yml`. Drill DR5 passes on the laptop before the live load | [RV] | Open |
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
