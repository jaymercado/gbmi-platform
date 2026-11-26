# GBMI Platform — Week 1 Implementation Plan

**Phase P0: walking skeleton and de-risking**

| | |
|---|---|
| **Document** | Implementation plan for week 1 of the project blueprint (phase P0 in §11.2, next steps in §16.7). Planning only; no code exists yet |
| **Version / date** | 1.0 — 25 November 2026 |
| **Prepared for** | Jay Mercado (solo developer) |
| **Source of truth** | `GLOBAL_BUSINESS_MARKET_INTELLIGENCE_PROJECT_BLUEPRINT.md` v1.1, cited as "BP §…". Where this plan differs from it, §3.4 says so |
| **Execution model** | **One phase.** Eleven work packages run once, in order, towards a single exit gate (§2). There are no sub-phases, intermediate milestones or sign-offs |
| **Planned effort** | 40 hours (BP §11.2); about 60 with the blueprint's contingency of 50% (BP §11.1). Labelling adds about 20 minutes a day |
| **Illustrative dates** | Monday 30 November – Sunday 6 December 2026 (BP §11.1) |
| **Tags** | **[V]** verified on 2026-11-25 for this plan — on the laptop, at the package index and image registries, at the live GDELT index, or in the documentation and source code of the pinned version · **[R]** decision made in this plan · **[E]** estimate · **[A]** assumption. Statements cited to the blueprint keep the standing they have there |

> **What approval of this plan triggers.** One action only: this document is saved as `WEEK_1_IMPLEMENTATION_PLAN.md` next to the blueprint. Nothing is installed, created or deployed until week 1 is started explicitly.

---

## 1. Context

The blueprint is finished and nothing has been built. Week 1 (phase P0) does two things before any production pipeline code is written:

1. **It proves every integration once**, end to end, on one hour of GDELT Events: source → bronze → silver (Iceberg) → ClickHouse → dbt mart → Metabase chart, run by Airflow, on the laptop and in CI.
2. **It settles the three technical risks that could change the design** — R1 (ClickHouse reading a pinned Iceberg metadata file), R2 (the experimental `insert_overwrite` strategy) and R6 (Cosmos on Airflow 3) — each with a recorded pass or a recorded fallback (BP §13, §16.4).

It also lays the baseline every later phase assumes: repository, tooling, CI, the Compose stack, Terraform state, the lake buckets and a spend limit.

**Intended outcome:** `make demo` passes locally and in CI on committed fixtures; the Terraform baseline is applied, with a budget that notifies a verified address; each spike has a decision backed by a test that stays in the suite; memory use is measured.

Preparing this plan turned up eight facts that shape how the week has to be run. They are why the plan is more specific than BP §11.2. All are **[V]**.

| # | Finding | Evidence | Effect on the plan |
|---|---|---|---|
| F1 | The laptop is not ready. uv, Terraform, the AWS CLI and the Session Manager plugin are missing; Docker is installed but its daemon is stopped; `gh` is not logged in; git has no identity; there is no `~/.aws/config`; the only Python 3.12 is Anaconda's | Inspection of the machine | WP-01; a uv-managed interpreter |
| F2 | The project folder's path contains spaces and `&` | Inspection | The repository goes in a new folder (§3.3) |
| F3 | Every BP §6.1 pin needed this week still exists and is the latest release of its line | PyPI, Docker Hub, GitHub releases | Pin table in §4.2 |
| F4 | dbt-clickhouse 1.10.4 `insert_overwrite` replaces only the partitions present in the new data. A partition whose rebuild returns no rows keeps its old rows | Adapter source at tag `v1.10.4` (`incremental.sql`) | SK3 test T4 is expected to fail on the stock strategy; the blueprint's fallback materialisation is the likely result |
| F5 | PyIceberg 0.12.0: writes accept `branch=`. An overwrite is committed as up to two snapshots — a delete and an append — in one catalog commit. `dynamic_partition_overwrite` returns without doing anything when its input is empty. Streaming `RecordBatchReader` writes raise `NotImplementedError` on partitioned tables. `expire_snapshots` removes snapshot metadata only and has no "keep the last N". There is no fast-forward; a branch is published with `set_current_snapshot(ref_name=…)` | Source at tag `pyiceberg-0.12.0` | The commit primitive in §3.3; SK1 questions; D4, D5, D8 |
| F6 | PyIceberg's Glue catalog puts a table at `<database location>/<table>` when the database has a location, otherwise at `<warehouse>/<database>.db/<table>`. The `s3.profile-name` property is honoured only by the fsspec file IO | Source and documentation at the same tag | The Glue database gets an explicit location; the laptop uses short-lived environment credentials |
| F7 | ClickHouse: `iceberg_metadata_file_path` is an engine-level setting given at table creation and overrides every other resolution rule; it is not among the settings the documentation lists as changeable with `ALTER TABLE … MODIFY SETTING`; `iceberg_snapshot_id` is a query-level setting; `date` maps to `Date32`, `timestamptz` to `DateTime64(6, 'UTC')`, `list` to `Array`. The documentation says nothing about which local paths `IcebergLocal` may read | Current ClickHouse documentation (not specific to 26.8) | SK2 design and its fallback ladder |
| F8 | Cosmos 1.16.0 documents the isolation pattern the blueprint chose (`ExecutionConfig.dbt_executable_path`), but its compatibility table stops at Airflow 3.3 | Cosmos documentation at tag `astronomer-cosmos-v1.16.0` | SK3 part B has to prove it on 3.4 |

Nothing exists to reuse: the project folder holds only the blueprint and the one-page brief **[V]**. The blueprint's illustrative DDL and SQL (BP §7.3, §7.7, §7.10) are the starting point for the code named below.

---

## 2. Exit gate

The week is done when every row is true. Rows 1–3 are the blueprint's acceptance criteria for P0; rows 5–10 are its deliverables and the week-1 steps of BP §16.7. Row 4, the blueprint's fourth criterion, was removed on 2026-11-26 (D12). Row 10 depends on elapsed days, not effort; §11 says what may start while row 10 is open.

| # | Criterion | Evidence |
|---|---|---|
| 1 | `make demo` passes on the laptop and in CI | Local run log; `e2e` green on `main` after the last merge |
| 2 | The chart's total equals source rows minus quarantined rows minus duplicate rows | `tests/e2e/test_demo.py`: the Metabase card result equals a count computed independently from the fixture zips; duplicates are counted, not assumed to be zero |
| 3 | Each spike has a recorded pass or fallback | `docs/reports/p0-spikes.md`; the spike tests stay in the suite; a fallback is also written into its decision record |
| 4 | *Removed on 2026-11-26 (D12).* Was: a budget alert email has been received | — |
| 5 | CI is green | Workflows `ci`, `dbt`, `e2e` and `terraform` on `main` |
| 6 | The Terraform baseline is applied | `terraform plan` reports no changes for `bootstrap` and `envs/prod`, run locally |
| 7 | Memory is measured | `docs/reports/p0-memory.md`: the BP §9.1 table with measured laptop values for idle, the tracer run and the GKG probe — or the probe recorded as moved to P1 |
| 8 | ADR-001 and ADR-004 are drafted | `docs/adr/`, in the BP §15.4 template |
| 9 | Version pins are re-verified on the start date | Pin table in `docs/reports/p0-walking-skeleton.md`; any change noted in the affected decision record |
| 10 | Labelling has started | At least 100 organisation strings labelled in `config/labelling/gold_sheet_v0.csv` |

---

## 3. Scope and fixed decisions

### 3.1 In scope

| Blueprint task (§11.2 P0, §16.7) | Work package |
|---|---|
| (1) Scaffold repository, tooling and CI | WP-02 |
| (2) Compose core profile; measure memory | WP-05, WP-10 |
| (3) Spike SK1 — PyIceberg | WP-06 |
| (4) Spike SK2 — ClickHouse pinned read | WP-07 |
| (5) Spike SK3 — partition replace; Cosmos | WP-08 (and the image build in WP-05) |
| (6) Tracer DAG: four slots → bronze → silver → ClickHouse → one mart → one chart | WP-09 |
| (7) Terraform bootstrap, buckets, Glue database, budget (the test alert is optional, D12) | WP-03 |
| (8) Draft ADR-001 and ADR-004; record spike outcomes | WP-02 (ADR-001), WP-06 to WP-08, WP-11 |
| §16.7 steps 2, 10 and 11 — tools and accounts, labelling, pin check | WP-01, WP-04 |

### 3.2 Deferred

| Item | Phase |
|---|---|
| Ledger state machine, slot calendar, Postgres migrations, master-list reconciliation | P1 |
| Mentions and GKG parsers; gates G1–G3 at full strength; commit-conflict retries; 30-day backfill; the DagBag test suite | P1 |
| All SEC ingestion, the five demo companies and their recorded payloads | P2 |
| Network, host, host IAM roles, deploy workflow, remote logging; the scheduled S3 and Glue test job of BP §9.5 | P2 |
| The real `gbmi er sample-gold` command with all strata | P3 |
| Watermarks, reconciliation loop, Athena workgroup, backups, the budget's stop action; clean-up of orphaned files and staging tables; the full `security` workflow | P4 |

Where a deferred component will later sit, the tracer uses the simplest stand-in — for example a committed index file instead of the ledger.

### 3.3 Decisions fixed for this week [R]

The blueprint leaves these open, and week 1 cannot start without them.

| Topic | Decision | Reason |
|---|---|---|
| Repository location | `/Users/jaymercado/Jay/Projects/gbmi-platform` — a new folder; the blueprint and this plan are copied into it | Spaces and `&` in the current path **[V]** break Make, shell recipes and, later, local Spark. BP §10 already names the root `gbmi-platform/` |
| Python interpreter | uv-managed CPython 3.12, pinned in `.python-version`, with `UV_PYTHON_PREFERENCE=only-managed` | Keeps laptop, image and CI on the same interpreter; avoids the Anaconda build |
| Dependency locking | Three environments, three lock sources: `uv.lock` (package `gbmi`); `dbt/uv.lock` (dbt-core, dbt-clickhouse, sqlfluff); Airflow's 3.4.0 constraints file plus `docker/airflow/requirements.txt` | BP §5.5 isolates the three environments; each has to be reproducible |
| CLI and logging libraries | Typer; structlog with JSON output | Typed entry points; structured logs with a run identifier (BP §5.10) |
| Licence | Apache-2.0, with a `NOTICE` file carrying the GDELT and SEC attributions | BP §15.2 asks for a licence and a notice |
| Fixture hour | The four `export` slots of 2026-11-25 08:00–08:45 UTC | Contains the slice profiled in BP §4.2 |
| Tracer mart | `fct_gdelt_event_daily` (BP §7.6), partition-replaced by month | The tracer then exercises the incremental contract, not a throwaway model |
| Lake commit primitive | `overwrite` with an explicit unit filter, for the hour and for the day alike. `dynamic_partition_overwrite` is not used | The unit comes from the task, not from the rows; the alternative infers partitions from the data and does nothing on empty input (F5), so it cannot empty a unit |
| Form of a spike | An integration test that stays in the suite, plus a section of the spike report written in the same work package. A rejected option is kept as a strict expected failure, so a later version that fixes it is noticed | Evidence that keeps guarding the decision |
| Laptop access to AWS | Short-lived credentials from `aws login` (D13). Terraform and the AWS CLI read them through the profile `gbmi`; the AWS test halves get them as environment variables (`aws configure export-credentials --profile gbmi`), with `AWS_DEFAULT_REGION=us-east-1` | F6. Environment credentials behave the same for PyIceberg, boto3 and ClickHouse, and nothing long-lived is stored |
| Glue database location | `gbmi_prod_silver` is created with location `s3://<lake>/warehouse/silver.db` | F6; gives the BP §7.1 layout |
| Namespace naming | `silver` on the local catalog; `gbmi_<env>_silver` on Glue; resolved in the catalog factory | BP §7.1; application code does not branch on environment (BP §5.1) |
| AWS test namespace | Glue database `gbmi_ci_silver` at `s3://<lake>/ci/warehouse/silver.db`, created and dropped by a test fixture shared by SK1 and SK2 | BP §9.5 reserves the `ci/` prefix; production names stay untouched |
| Terraform values kept out of Git | Backend settings in `backend.hcl` and the alert-test amount in `budget-test.auto.tfvars`, both git-ignored with committed examples; `alert_email` and `github_repository` as `TF_VAR_*` | The repository is public; a local override file cannot be undone by accident the way a `-var` flag can |
| Workflow triggers | Every workflow runs on pull requests and on pushes to `main` | Makes "green on `main`" checkable |
| Checksums | A row count and an order-independent hash over a named list of business columns, with floating-point values rounded. Excluded: `_ingested_at`, `_run_id`, `_built_at`, `_dbt_invocation_id` and `_silver_snapshot_id`. At fixture size, comparisons between engines use exact row equality | A rerun creates a new snapshot and new timestamps by design (BP §5.6) |
| Airflow sign-in | The FAB auth manager, as in the 3.4.0 reference Compose file **[V]** | Stays on the documented path |
| Pull requests | One short-lived branch and pull request per work package | BP §10, §15.2 |

### 3.4 Differences from the blueprint

| # | Blueprint | This plan | Why |
|---|---|---|---|
| D1 | §16.7 runs the spikes (step 6) before Terraform (step 7) | Terraform is WP-03, before the spikes | SK1 and SK2 need the bucket and Glue for their AWS halves, and the budget alert takes up to a day to arrive **[A]** |
| D2 | §11.2 says "measure memory"; §9.1 says P0 measures a full GKG day build beside a mart build | WP-10 builds one GKG day with a crude probe. It is the first item moved to P1 if time runs short (R4 allows P0 and P1) | The GKG parser and the large marts are later work |
| D3 | §7.7 step 3 prefers the adapter's `insert_overwrite` | SK3 expects to adopt the fallback materialisation | F4 |
| D4 | §8.4 passes data to the Iceberg writer in record batches | SK1 tests two other bounded-memory patterns | F5 |
| D5 | §7.9 expires snapshots "keeping at least the last 5" | Expiry selects snapshots by id and counts commits; file clean-up on the local backend is noted for P4 | F5 |
| D6 | §8.8 draws gold samples with `gbmi er sample-gold`; §10 has no place for a sheet before the gold seed exists | A profiling script writes `config/labelling/gold_sheet_v0.csv`; the real command is P3 work | Labelling starts in week 1 (R25) without building the matching package early |
| D7 | §16.7 step 9 commits "four GDELT slots, five companies" | Week 1 commits the four slots; the companies and their SEC payloads follow in P2 | No SEC request is made before the fair-access client exists (FR-07) |
| D8 | §5.6 and §8.4 expect one Iceberg snapshot per overwrite | One catalog commit, up to two snapshots. ADR-004 records the consequences: a rollback goes to the snapshot before the commit, expiry counts commits, and `_silver_snapshot_id` is the commit's last snapshot | F5 |
| D9 | §11.2: the chart's row count equals source rows minus quarantined rows | Minus duplicate rows as well, each term counted | Silver deduplicates on the logical key (BP §8.4) |
| D10 | §6.2 (TD-09) and §10 commit Metabase dashboard exports | A provisioning script is the committed definition | Metabase OSS is assumed to have no file export of dashboards **[A]** |
| D11 | §16.4: a phase does not start until the previous one's acceptance criteria are met | P1 may start while exit-gate row 10 is still open, if elapsed time is the only thing missing | Labelling days are not effort |
| D12 | §11.2 lists "budget alert email received" among the acceptance criteria of P0 | Since 2026-11-26 an alert email is not a condition of P0 or of the tag `p0`. The budget and its notifications stay (WP-03). The forced alert test stays available as an optional procedure (`infra/terraform/README.md`, §3) | Decision of the owner. The test costs up to a day of waiting for one email. The budget notifies a verified address, and the zero-spend budget (H2) reports the first real cost, so the first real alert gives the same proof |
| D13 | §16.7 step 2 sets up SSO with multi-factor authentication; H1 planned IAM Identity Center with one user | Since 2026-11-26 the root user of the AWS account does all AWS work: MFA on the root user, no access key, and short-lived credentials from `aws login` (profile `gbmi-login`, handed on by the process profile `gbmi`). There is no IAM Identity Center, no organization and no second user | Decision of the owner, for an account with one person: fewer parts to set up and to keep. AWS advises against the root user for everyday tasks; a second user can be added later, because every resource belongs to the account and not to a user. Credentials last 15 minutes and are renewed for up to 12 hours, so the AWS test halves run one test at a time (`make integration-aws`) |

---

## 4. Verified starting state

### 4.1 Laptop [V]

| Item | State on 2026-11-25 | Action |
|---|---|---|
| Hardware and OS | MacBook Pro, arm64, 16 GB, 12 cores; macOS 27.2.1; 486 GB free | — |
| Docker | CLI 28.2.2 and Compose v2.36.2 installed; daemon not running | Start it; set 9 GB (H4) |
| Python | 3.12.7, from Anaconda only | uv-managed 3.12 (WP-01) |
| uv, Terraform, AWS CLI, Session Manager plugin, tflint | Not installed | Install (WP-01) |
| Java | OpenJDK 17.0.15 | None this week |
| git, gh, make | git 2.49.0 with no identity and no default branch; gh 2.86.0, not logged in; GNU Make 3.81 | H3, H5 |
| AWS configuration | No `~/.aws/config` | H1 |
| Ports 3000, 5432, 8080, 8123, 9000 | Free | — |
| Project folder | Blueprint and brief only; not a git repository | New repository folder (§3.3) |

### 4.2 Version pins [V]

| Component | BP §6.1 pin | Found on 2026-11-25 |
|---|---|---|
| Apache Airflow | 3.4.0 | 3.4.0 on PyPI (released 2026-11-06); image `apache/airflow:3.4.0-python3.12` includes arm64 |
| astronomer-cosmos | 1.16.0 | 1.16.0, latest; requires `apache-airflow>=2.11.0` |
| DuckDB | 2.0.x | 2.0.1 (2026-11-20) |
| PyIceberg | 0.12.0 | 0.12.0, latest; extras `pyarrow`, `sql-sqlite`, `glue`; PyArrow 26.0.1 satisfies its ≥ 18 |
| dbt Core + dbt-clickhouse | 1.12.7 + 1.10.4 | Both latest; the adapter requires `dbt-core>=1.9,<2.0` |
| ClickHouse | 26.8 LTS | 26.8.46.6-lts; image tag `26.8` |
| PostgreSQL | 17 | Image `postgres:17` is 17.12 |
| Metabase OSS | 0.65.x | v0.65.7.1 |
| Terraform; AWS provider | 1.17.x; — | v1.17.2; v6.74.0 |
| uv, ruff, mypy, pytest, pre-commit, sqlfluff | current | 0.13.10, 0.17.6, 2.4.1, 9.2.0, 4.7.0, 4.5.0 |
| Libraries not pinned by the blueprint | — | clickhouse-connect 1.13.0, pydantic 2.14.4, pydantic-settings 2.16.3, httpx 0.28.1, tenacity 9.2.1, typer 0.29.2, structlog 26.1.0, psycopg 3.3.9, checkov 3.4.11 |

The arm64 variants of the ClickHouse, PostgreSQL and Metabase images were not inspected individually **[A]**; BP §9.3 records the Metabase one as verified.

---

## 5. Actions only Jay can take

| # | Action | When | Check |
|---|---|---|---|
| H1 | AWS access (as done on 2026-11-26, D13): an AWS account; MFA on the root user; `aws login --remote --profile gbmi-login` as the root user; the process profile `gbmi`, region `us-east-1`. *Planned before D13:* IAM Identity Center with one user and the `AdministratorAccess` permission set | WP-01 (may slip — §10) | `aws sts get-caller-identity --profile gbmi` |
| H2 | In the Billing console, create a budget from the "Zero spend budget" template with the alert email — before any resource exists (BP §16.7 step 2) | Before WP-03 | Budget listed |
| H3 | GitHub: `gh auth login`; create the public repository `gbmi-platform`; after the first green run, protect `main` and create environment `prod` with a required reviewer | WP-01, WP-02 | `gh repo view` |
| H4 | Docker Desktop: start it and set memory to 9 GB (BP §9.1) | WP-01 | `docker info` |
| H5 | Supply, outside the repository: the alert email (`TF_VAR_alert_email`), the repository slug (`TF_VAR_github_repository`) and a git identity (a GitHub no-reply address suits a public repository) | WP-01, WP-03 | — |
| H6 | *Removed on 2026-11-26 with exit gate row 4 (D12).* Was: confirm that the budget alert email arrived and note the time | — | — |
| H7 | Label about 20 organisation strings a day | From WP-04 | Exit gate row 10 |
| H8 | Run `aws login --remote --profile gbmi-login` whenever the session has expired (after 12 hours at the latest; D13) | WP-03; the AWS halves of WP-06 and WP-07; the clean-up in WP-11 | `aws sts get-caller-identity --profile gbmi` |

Everything else in this plan can be executed without him.

---

## 6. Execution sequence

| Order | Work package | Hours | Needs | Produces |
|---|---|---|---|---|
| 1 | WP-01 Preflight and accounts | 1.5 | H1–H5 | Working toolchain; AWS and GitHub access |
| 2 | WP-02 Repository, tooling, first CI, ADR-001 | 4 | WP-01 | Skeleton, locks, hooks, `ci` green |
| 3 | WP-03 Terraform baseline and budget alert | 4.5 | WP-01, WP-02 | State, buckets, Glue database, budget |
| 4 | WP-04 Labelling sheet | 1 | WP-02 | Sheet; daily labelling begins |
| 5 | WP-05 Images and Compose core profile | 5 | WP-02 | Stack up; idle memory |
| 6 | WP-06 Spike SK1 — PyIceberg | 3.5 | WP-05; WP-03 for the AWS half | Lake contract decision |
| 7 | WP-07 Spike SK2 — ClickHouse pinned read | 3 | WP-06 | Pinning decision |
| 8 | WP-08 Spike SK3 — partition replace; Cosmos | 5.5 | WP-05 | Incremental and Cosmos decisions |
| 9 | WP-09 Tracer DAG and `make demo` | 8.5 | WP-06, WP-07, WP-08 | Walking skeleton; `e2e` green |
| 10 | WP-10 Memory under load | 1.5 | WP-09 | Memory report |
| 11 | WP-11 ADR-004, reports, close-out | 2 | All | Decision record, reports, tag |
| | **Total** | **40** | | About 60 with contingency |

Rules for the sequence:

- The order is the plan. The only permitted reordering is the one in §10 for late AWS access.
- Spikes run SK1, SK2, SK3 in that order, local half first. No tracer code that depends on a spike is written before that spike's local decision (BP §16.4). A failed AWS half reopens the decision.
- A spike that reaches its time box without a pass takes its fallback and records it. Where no fallback exists, the work stops and the question is escalated; the package says which case applies.

**Stretch items** are outside the 40 hours and are taken only when the week is ahead:

| Stretch item | Hours |
|---|---|
| A `plan` job in CI through OIDC as `gbmi-ci-plan` (BP §9.4) | 1 |
| Dependabot configuration and a `security` workflow beyond secret scanning (BP §9.7) | 0.5 |
| SK2 questions Q7 and Q8 when Q1 and Q2 have already passed | 0.5 |
| A `postgresql()` read from ClickHouse to Postgres in `make smoke` (BP §5.2) | 0.25 |

---

## 7. Work packages

### WP-01 — Preflight and accounts (1.5 h)

**Goal.** A laptop and accounts that meet the prerequisites of BP §11.2, with the pins confirmed.

1. Install what is missing **[V]**:
   ```bash
   brew install uv awscli tflint
   brew tap hashicorp/tap && brew install hashicorp/tap/terraform
   brew install --cask session-manager-plugin
   uv python install 3.12
   ```
2. Start Docker Desktop and set its memory to 9 GB (H4); confirm with `docker info`.
3. Set the git identity and `init.defaultBranch main` (H5). Complete H1–H3.
4. Re-check §4.2 against PyPI, Docker Hub and GitHub releases on the start date (BP §16.7 step 11). Rule: take a newer patch release of the same minor version; anything else stays on the pin and is noted for a decision record (BP §6.1, R7).

**Done when.** `uv --version`, `terraform version`, `aws --version`, `docker compose version` and `gh auth status` succeed, and Docker reports about 9 GB. `aws sts get-caller-identity --profile gbmi` returns the account — or H1 is still in progress and the reordering in §10 applies.

### WP-02 — Repository, tooling, first CI and ADR-001 (4 h)

**Goal.** The BP §10 skeleton with locked dependencies, hooks, a Makefile, a green `ci` workflow and the first decision record.

1. Create the repository folder (§3.3) and run `git init -b main`. Copy in the blueprint and this plan. Add `LICENSE`, `NOTICE`, a README stub (problem, status, how to run, GDELT and SEC attribution — BP §4.7), `.gitignore` (`data/`, `.env`, virtual environments, `target/`, `dbt_packages/`, Terraform state, `backend.hcl`, `*.tfvars`), `.env.example` and `.python-version`.
2. `pyproject.toml` for package `gbmi`: `src` layout, `requires-python = ">=3.12,<3.13"`, entry point `gbmi = "gbmi.cli:app"`. Runtime dependencies: `duckdb`, `pyiceberg[pyarrow,sql-sqlite,glue]==0.12.0`, `clickhouse-connect`, `psycopg[binary]`, `httpx`, `tenacity`, `pydantic`, `pydantic-settings`, `typer`, `structlog`. Development group: `pytest`, `pytest-cov`, `ruff`, `mypy`, `pre-commit`. Run `uv lock`.
3. `dbt/pyproject.toml` with `dbt-core==1.12.7`, `dbt-clickhouse==1.10.4` and `sqlfluff`; run `uv lock` there. `docker/airflow/requirements.txt` with `astronomer-cosmos==1.16.0`, installed in WP-05 under the Airflow 3.4.0 constraints file.
4. Package skeleton: `src/gbmi/config.py` (one typed settings class for the `GBMI_*` variables of BP §9.5), `logging.py`, `cli.py`. Each further sub-package is created by the work package that first needs it.
5. Quality tooling: ruff for lint and format; mypy — strict for `gbmi.core`, `gbmi.sources` and `gbmi.er` as BP §10 requires, standard elsewhere; `.sqlfluff` with the ClickHouse dialect and the Jinja templater with dbt built-ins, so linting needs no database; `.pre-commit-config.yaml` with ruff, mypy, sqlfluff, gitleaks, `uv lock --check` and `terraform fmt`.
6. `Makefile`: `setup` (creates the environments and installs the hooks), `lint` and `test` now. `up`, `down`, `reset`, `smoke`, `integration`, `dbt-build`, `demo` and `plan` are added by the packages that make them real. macOS ships GNU Make 3.81 **[V]**, so nothing newer than that is used.
7. First tests: settings load from the environment; a compliance test asserts that the GDELT citation and link and the SEC attribution are present in `README.md` and `NOTICE` (BP §4.7).
8. `.github/workflows/ci.yml` on the arm64 hosted runner (`ubuntu-24.04-arm` **[A]**): ruff, mypy, sqlfluff, `uv lock --check`, pytest with coverage, and a gitleaks secret scan. Actions are pinned by commit hash and the default token is read-only.
9. `docs/adr/001-scope-non-goals-success-criteria.md` from BP §3 and §16 (BP §16.7 step 4). Template: Context — Decision — Alternatives considered — Consequences — Evidence — Status (BP §15.4).
10. Push, open the first pull request, merge when green, then protect `main` (H3).

**Done when.** `make setup lint test` passes on a clean clone; `ci` is green; `main` is protected.

### WP-03 — Terraform baseline and budget alert (4.5 h)

**Goal.** State, CI identity, the two buckets, the Glue database and the budget exist as code (BP §9.2, §9.4).

1. `infra/terraform/bootstrap/`, applied by hand (BP §9.4): the state bucket `gbmi-tfstate-<account>` (versioned, encrypted, public access blocked, `prevent_destroy`); the GitHub OIDC provider; role `gbmi-ci-plan` (trusted for this repository's pull requests; read-only plus state access) and role `gbmi-ci-deploy` (trusted for environment `prod`; state access only until P2). Apply with local state, then add the S3 backend with `use_lockfile = true` and run `terraform init -migrate-state -backend-config=backend.hcl`.
2. Modules `lake_s3`, `glue_catalog` and `budgets`, composed in `envs/prod` (S3 backend, provider `aws ~> 6.74`, default tags `project=gbmi` and `env=prod`):
   - `lake_s3` — buckets `gbmi-prod-lake-<account>` and `gbmi-prod-ops-<account>`: versioning, server-side encryption, public access blocked, a TLS-only bucket policy, `prevent_destroy`. Lifecycle: `quarantine/` expires at 180 days, non-current versions at 30 days, `athena-results/` at 7 days, `tmp/` at 1 day. The Glacier rule for GKG files is defined but disabled until P5 (BP §7.9).
   - `glue_catalog` — database `gbmi_prod_silver` with the location given in §3.3.
   - `budgets` — one monthly cost budget whose amount is the variable `monthly_budget_usd` (default 30), with email notifications at 50, 80 and 100% of actual spend. The stop action is P4 work.
3. `terraform apply` in `envs/prod` with the profile `gbmi` (H8).
4. **Budget alert test** (optional since 2026-11-26, D12; it was exit gate row 4). Put `monthly_budget_usd = 0.02` in the git-ignored `budget-test.auto.tfvars` and apply. Then create real spend of about USD 0.03 **[E]**: upload 5,000 empty objects under `s3://gbmi-prod-ops-<account>/tmp/budget-test/` and delete them. Billing data lags by hours, so the email is expected within about a day **[A]**; work continues meanwhile. When it arrives, delete the file and apply again. The hand-made zero-spend budget (H2) should fire on the same spend and is a second confirmation.
5. `.github/workflows/terraform.yml`: `fmt -check`, `validate`, tflint and checkov. A committed `.checkov.yaml` lists each skipped check with its reason — at this budget there is no KMS key, replication or access logging. `make plan` runs the plan locally.

**Done when.** Both configurations plan clean locally; `aws s3 ls`, `aws glue get-database --name gbmi_prod_silver` and `aws budgets describe-budgets` show the resources; the `terraform` workflow is green.

**If it fails.** Only when the optional alert test is run: if no alert has arrived 48 hours after the spend, compare the budget's actual amount in the console with its threshold and leave the test amount in place.

### WP-04 — Labelling sheet (1 h)

**Goal.** The daily labelling habit can start on day 2 (BP R25, §16.7 step 10).

1. `benchmarks/source_profile/sample_org_strings.py`, a profiling script and not package code: download the eight GKG slots of 2026-11-25 08:00–09:45 UTC to a scratch directory, checking each MD5 against its index line; read the organisations and page-title fields; write `config/labelling/gold_sheet_v0.csv` with 200 distinct organisation strings — the 100 most frequent and 100 drawn at random with seed 20261125 — each with its document count and up to five sample titles. Columns to fill in: `label` (`company_in_universe`, `company_not_in_universe`, `not_a_company`, `ambiguous`), `cik`, `note`.
2. The sheet is blind by construction, because no matching system exists yet (BP §8.8). CIKs are looked up by hand in EDGAR's company search; no programmatic SEC request is made this week.

**Done when.** The sheet is committed and the first 20 strings are labelled (H7). The strata of BP §8.8 are drawn in P3; these labels count towards the 200 needed for the MVP.

### WP-05 — Images and Compose core profile (5 h)

**Goal.** The core profile of BP §9.1 and the images of BP §9.3 run on the laptop within the memory budget.

1. `docker/airflow/Dockerfile` from `apache/airflow:3.4.0-python3.12`. The Airflow environment receives only `requirements.txt`, under the 3.4.0 constraints file, and `pip check` must be clean with Cosmos installed (SK3 question B1). `/opt/venvs/gbmi` is built from `uv.lock` and `/opt/venvs/dbt` from `dbt/uv.lock` with `uv sync --frozen`. A `dev` target adds the development group and expects source bind mounts; the default target bakes in `dags/` and `dbt/`. All four images — Airflow base, ClickHouse, PostgreSQL, Metabase — are pinned by digest (BP §9.3).
2. `compose/compose.yaml` starts from the Airflow 3.4.0 reference file **[V]** and changes it: `LocalExecutor`; no `redis`, `airflow-worker` or `flower`; keep `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor` and `airflow-triggerer`; `postgres:17`; add `clickhouse/clickhouse-server:26.8`; `AIRFLOW__CORE__PARALLELISM=4`; examples off; ports published on `127.0.0.1` only. Two helper services: `dbt-manifest`, a one-shot service that runs `dbt parse` from `/opt/venvs/dbt` and must finish before the Airflow services start, because a DAG that renders dbt tasks needs the manifest to import; and `toolbox`, built from the `dev` target, which runs integration tests on the Compose network. `compose/compose.bi.yaml` adds `metabase` (v0.65.x, heap capped, application data in Postgres).
3. Mounts: `./data/lake` at `/lake` — read-write in the Airflow services and the toolbox, read-only in ClickHouse — so that file URIs in Iceberg metadata resolve identically everywhere (BP §9.1); `./tests/fixtures` at `/fixtures`, read-only. Local catalog `sqlite:////lake/catalog/catalog.db`; warehouse `file:///lake/warehouse`. On Linux runners `AIRFLOW_UID` is set to the runner's user id so that both containers can read the mount.
4. Initialisation: `docker/postgres/init/` creates databases `airflow`, `gbmi` and `metabase`; `db/clickhouse/init/` creates databases `lake`, `staging`, `intermediate`, `marts`, `ops` and `seeds` and roles `dbt_rw` and `bi_ro` (BP §5.8); `airflow-init` migrates the metadata database and creates pools `sec_api`, `silver_writer` and `heavy` with one slot each (BP §5.5).
5. `make setup` writes a git-ignored `.env` from `.env.example` and generates the Fernet key, the JWT secret and the passwords. `make reset` stops the stack and removes its volumes and `data/`.
6. `make smoke`: every health check is green; `airflow dags list-import-errors` is empty; the three pools exist; ClickHouse reports 26.8; the Airflow container can write to `/lake` and ClickHouse can read it.
7. Memory, in this order. First run without limits: after ten idle minutes take five `docker stats` samples 30 seconds apart and keep the median per container. Then set limits that add up to the BP §9.1 budget of 7.5 GB — API server 0.8 GB with its worker count set explicitly to two **[A]**, DAG processor 0.5, triggerer 0.4, scheduler 2.8 (under LocalExecutor it also hosts the task processes), Postgres 0.5, ClickHouse 2.5 with `max_server_memory_usage` set, and Metabase 1.2 on demand — and sample again.

**Done when.** On a clean checkout `make up` reaches healthy and `make smoke` passes, with limits in force.

**If it fails.** A dependency conflict in the image is solved by moving the package into its own virtual environment, never by loosening Airflow's constraints. If Cosmos itself cannot be installed under the constraints, take the R6 fallback at once: dbt runs from a command task, Cosmos is left out, and part B of SK3 shrinks to question B5. If SQLite reports locking errors on the bind mount, the catalog file moves to a named volume shared by the Airflow services.

### WP-06 — Spike SK1: the PyIceberg contract (3.5 h)

**Goal.** Show that PyIceberg 0.12.0 delivers the idempotency contract of BP §5.6 on both backends through one catalog factory. (A catalog records each table's current metadata file; a snapshot is one committed version of a table.)

**Produces.** `src/gbmi/lake/` (catalog factory, table definition, stage–audit–commit helper); `tests/integration/lake/test_sk1_pyiceberg.py`, parametrised by backend; a section of the spike report.

**Test table.** `slot_date` (date, identity partition), `slot_hour`, an id, a `timestamptz`, a double, a `list<string>` column and the provenance columns of BP §7.12; format version 2; ZSTD.

| Q | Question | Pass when |
|---|---|---|
| Q1 | Does one factory create the same table on SQLite/filesystem and on Glue/S3? | Created on both; on Glue the table lands at `<database location>/<table>` (F6) |
| Q2 | Does an overwrite filtered on (`slot_date`, `slot_hour`) replace only that hour (BP §8.4)? | One catalog commit and one new metadata file; the hour's rows are replaced; data files of other hours keep their paths — a whole-file delete, not a rewrite. The snapshots the commit created are recorded (expected: a delete and an append — F5) |
| Q3 | Does an overwrite filtered on `slot_date` replace exactly one day and compact its hourly files? | One catalog commit; other days untouched; snapshots recorded |
| Q4 | Are reruns idempotent, including an empty result? | A repeat gives the same content checksum; an empty input leaves the unit empty |
| Q5 | How is a whole day written with bounded memory, given F5? | One of two patterns commits atomically: (a) one transaction that deletes the day and appends hour by hour; (b) Parquet files written first, then `add_files` in one commit. The chosen daily pattern is recorded |
| Q6 | Can a result be audited on a branch and then published? | A write with `branch="audit"` leaves `main` unchanged; `set_current_snapshot(ref_name="audit")` publishes it; the branch is removed |
| Q7 | What does snapshot expiry do? | "Older than 7 days, keep the last 5 commits, never a branch head" is implemented by snapshot id; current data is intact; the files left on storage are listed (expected: all of them — F5) |
| Q8 | Do Q1–Q4, Q6 and Q7 hold on Glue/S3? | The same tests pass in `gbmi_ci_silver` (§3.3) with environment credentials; commit latency is recorded |

**Decision.** Q1, Q3 and Q4 have no fallback this week: a failure stops the work and is escalated, because every later package writes through this contract. If Q2 fails, the day becomes the only overwrite unit and the hourly job rewrites the day, with the cost measured in P1. For Q5, take whichever pattern passes; if neither does, a day is built as one in-memory table and its peak is measured in WP-10. If Q6 fails, nothing changes for the routine path, which audits before it commits (BP §5.6); the Spark job publishes its own branch in P5. For Q7, file clean-up is Athena `VACUUM` on AWS (BP §7.9) and an open item for the local backend in P4.

**Time box.** 2.5 h local, 1 h AWS.

### WP-07 — Spike SK2: ClickHouse reads a pinned table (3 h)

**Goal.** Settle R1: a mart build can read exactly one Iceberg metadata version (BP §5.12).

**Produces.** `src/gbmi/warehouse/pin.py`; `tests/integration/warehouse/test_sk2_pinned_read.py`, run from the toolbox; a section of the spike report.

| Q | Question | Pass when |
|---|---|---|
| Q1 | Does an engine-level pin read exactly the named metadata file? `CREATE OR REPLACE TABLE lake.t ENGINE = IcebergLocal('/lake/warehouse/<ns>.db/<t>/') SETTINGS iceberg_metadata_file_path = 'metadata/<file>'` | Rows equal a PyIceberg scan of the same snapshot; pinning an older file returns the older rows |
| Q2 | Does the pin hold, and does re-pinning take effect? | After a new commit the pinned table is unchanged; after re-creating it with the new file the new rows appear (the metadata cache is on by default — F7) |
| Q3 | Are the types mapped as documented (F7)? | `Date32`, `DateTime64(6, 'UTC')` and `Array(String)` as expected; nullability recorded |
| Q4 | Which local paths may `IcebergLocal` read (F7)? | `/lake/…` works as mounted, or the server setting it needs is recorded and applied |
| Q5 | Does a plain ClickHouse view over `lake.t` survive re-pinning? | The view returns the new rows without being re-created |
| Q6 | Do Q1 and Q2 hold on S3 with short-lived credentials? | `IcebergS3('s3://…/')` with key, secret and session token reads the SK1 table in `gbmi_ci_silver` and passes the same assertions; read latency recorded |
| Q7 | Do the query-level alternatives work? Required only if Q1 or Q2 fails; otherwise stretch | Recorded for both: the table function with `SETTINGS iceberg_metadata_file_path`, and `SETTINGS iceberg_snapshot_id` on an unpinned table |
| Q8 | Is a `slot_date` filter pruned to its partition? Stretch | The query log shows only that partition read |

**Decision.** Q1, Q2 and Q5 locally make engine-level pinning the working mechanism (BP §7.7) and unblock WP-09; Q6 confirms it for AWS. Otherwise take the first rung that passes: (1) query-level `iceberg_metadata_file_path` in each staging model; (2) query-level `iceberg_snapshot_id`; (3) on AWS, the Glue `DataLakeCatalog` database; (4) exporting the partitions as Parquet. A rung other than the first choice is written into ADR-004 and a draft of ADR-006.

**Dialect checks** (BP §17.3 item 16; 30 minutes). Run three of the blueprint's illustrative statements on 26.8 with inline data and record the result or the corrected syntax: the type-2 interval query with `lagInFrame` and `leadInFrame`, where A, B, A must give three intervals; the as-of `argMax(value, (known_at, accession_number))` query (both BP §7.10); and the array expressions of BP §8.12.

**Time box.** 1.75 h local, 0.75 h S3, 0.5 h dialect.

### WP-08 — Spike SK3: partition replace and Cosmos (5.5 h)

**Part A — the incremental contract (3.5 h).** Settles R2.

**Produces.** The dbt project skeleton in `dbt/` (`dbt_project.yml`; `profiles/` with targets `local` and `ci`; a `generate_schema_name` override so that models land in `staging`, `intermediate` and `marts`). A test-only dbt project in `tests/integration/dbt/project/` that uses the main project's macros and holds the spike models, so they never enter the tracer's task group or `make dbt-build`. `tests/integration/dbt/test_sk3_partition_replace.py`, which becomes the permanent build-twice test of BP §7.7 item 6. `.github/workflows/dbt.yml`, which runs it against a service-container ClickHouse.

What the adapter does **[V]** (F4): it builds a staging table named `<model>__dbt_new_data_<invocation id>`, reads the partitions present in it, and issues one `ALTER TABLE … REPLACE PARTITION ID … FROM …` for those partitions, with `allow_replace_partition_from_empty_source = 1` on ClickHouse 26.6 and later.

Each test runs for a model partitioned by day (`slot_date`) and for one partitioned by month (`toYYYYMM(date)`, as the tracer mart is), over a source table of three days in two months.

| T | Test | Pass when |
|---|---|---|
| T1 | First build | Table created; count and checksum per partition recorded |
| T2 | Identical rebuild | Same count and checksum per partition; no duplicates |
| T3 | One partition's input changes; build with `partitions=[p]` | Only `p` changes |
| T4 | One partition's input becomes empty | The partition ends empty — **expected to fail** on the stock strategy (F4) |
| T5 | The replace step fails (run as a ClickHouse user without the privilege to replace partitions) | Target unchanged; the left-over staging table is visible in `system.tables`; the next normal run gives the correct result |
| T6 | `--full-refresh` | Result equals the incremental result |

**Decision.** If T1–T6 pass on the stock strategy, keep it. If T4 fails, as expected, write the blueprint's fallback (BP §7.7 step 3) as a custom materialisation `partition_replace` in `dbt/macros/materializations/`: it replaces every *requested* partition, whether or not the new data contains it, using the same staging table and the empty-source setting. Rerun T1–T6 against it. If it is not green at the end of the time box, stop: the tracer mart is then built as a full rebuild with an atomic table exchange (BP §5.6, small tables), and the partition-replace question is escalated as a blocker for P3. A fallback is recorded in a draft of ADR-006.

**Part B — Cosmos on Airflow 3.4 (2 h).** Settles R6. **Produces** `dags/spike_sk3_cosmos.py`, removed when the tracer supersedes it; the tracer in `e2e` then remains as the standing evidence for B2, B4 and B5.

| Q | Question | Pass when |
|---|---|---|
| B1 | Does the image build with Cosmos 1.16.0 under the Airflow 3.4.0 constraints? | Answered in WP-05: build succeeds and `pip check` is clean |
| B2 | Does a `DbtTaskGroup` render from a prebuilt manifest and run dbt from `/opt/venvs/dbt`? | With `LoadMode.DBT_MANIFEST`, `ExecutionConfig(dbt_executable_path=…)` and `ProfileConfig(profiles_yml_filepath=…)` (F8) there is one task per model and the run is green |
| B3 | Is DAG parsing free of dbt calls (BP §5.5)? | No dbt process during parsing; parse time recorded |
| B4 | Do runtime variables reach dbt? | A partition list passed at trigger time changes what is built |
| B5 | Does `@task.external_python` run a `gbmi` function from `/opt/venvs/gbmi`? | The task returns a small result through XCom |

**Decision.** B1–B4 confirm Cosmos as designed. Otherwise the fallback is one command task running `dbt build` from the dbt environment (BP R6), recorded in a draft of ADR-010. If B5 fails, tasks call the `gbmi` CLI from a command task.

### WP-09 — Tracer DAG and `make demo` (8.5 h)

**Goal.** The walking skeleton: four slots → bronze → silver → ClickHouse → one mart → one chart.

1. **Land** (`gbmi.sources.gdelt`). Read index lines (size, MD5, URL); rewrite `http://` to `https://`; download with httpx and a bounded retry, or in fixture mode copy from `/fixtures`; verify size, MD5 and that the zip opens (gate G1 in miniature); write to the deterministic bronze key of BP §7.1; skip an object that already exists with the expected MD5. The mode is the DAG parameter `source` (`fixture` or `live`), passed to the function.
2. **Fixtures.** Run the land step in live mode for the fixture hour, taking the index lines from the tail of `masterfilelist.txt` with an HTTP range request — the list is 130 MB and the server honours ranges **[V]**. Commit the four zips unchanged under `tests/fixtures/gdelt/v2/` with their index lines and an attribution file (about 0.4 MB **[E]**). This is also the proof of the live download path.
3. **Parse** (`gbmi.core`, `gbmi.transforms.duckdb`). The 61-column positional schema for `export`, taken from the codebook. DuckDB reads each file as text, checks the column count of every row, casts with safe conversions, writes failing rows to `quarantine/…` as Parquet with a reason, removes duplicate `global_event_id` values, and returns an Arrow table shaped as `gdelt_event` (BP §7.4) with the provenance columns. *Parsed* means typed rows before deduplication: raw lines = parsed + quarantined, and committed = parsed − duplicates.
4. **Audit and commit** (`gbmi.quality`, `gbmi.lake`). Gate G3 in miniature: the two identities above; the key is unique and not null; the unit holds only its date and hour. Then one overwrite filtered on (`slot_date`, `slot_hour`) through the SK1 helper.
5. **Pin** (`gbmi.warehouse.pin`). Re-create `lake.gdelt_event` on the catalog's current metadata file, using the SK2 result. The snapshot id it pinned is passed to dbt as a variable and stored as `_silver_snapshot_id`.
6. **Model** (dbt). Source `lake.gdelt_event`, whose description carries the GDELT citation; `stg_gdelt__event`; `fct_gdelt_event_daily` — one row per UTC day × action country × CAMEO root code × first-actor type, with events, mentions and mean tone; partitioned by month; built with the SK3 strategy; tests for a unique grain and non-null keys.
7. **Orchestrate** (`dags/tracer_gdelt_events.py`). Manual trigger with parameters `hour` and `source`; tasks `land` → `build_silver` → `pin_lake` → dbt task group → `assert_counts`; `max_active_runs=1`, `catchup=False`, retries and timeouts per BP §8.1; `build_silver` takes the `silver_writer` pool. The DAG file contains wiring only (BP §10).
8. **Chart** (`monitoring/metabase/provision.py`). An idempotent set-up through Metabase's REST API: admin user, a ClickHouse connection as `bi_ro`, one question (events by CAMEO root code on the mart) and one dashboard with a GDELT attribution text card (BP §4.7).
9. **`make demo`.** `make up` with the core and BI services (the manifest is built first — WP-05) → `smoke` → trigger the tracer on the fixture hour and wait for success → provision Metabase → `pytest tests/e2e` → open the dashboard when run interactively.
10. **`tests/e2e/test_demo.py`** asserts: (a) the card's total, read through Metabase's API, equals raw lines − quarantined rows − duplicate rows, each counted independently from the fixture zips; (b) silver, mart and card agree; (c) a second run of the DAG leaves the silver and mart content checksums of §3.3 unchanged (BP §1.5 item 7).
11. **`tests/transforms/`.** A synthetic three-line file — one good row, one with 60 columns, one with a non-numeric id — proves the quarantine path and the accounting identity, because real slots rarely contain bad rows.
12. **`.github/workflows/e2e.yml`:** build the images with a layer cache, `make integration`, `make demo`.

**Done when.** Exit gate rows 1, 2 and 5 hold.

**If it fails.** `airflow dags test tracer_gdelt_events` is the fast inner loop. If waiting on the scheduler proves unreliable in CI, `make demo` may use it there, with the difference recorded.

### WP-10 — Memory under load (1.5 h)

1. Run the tracer while sampling `docker stats` every two seconds — memory and CPU — and reading each container's cgroup `memory.peak`; record the peaks.
2. **GKG probe** (BP §9.1). Download one closed UTC day of GKG through the land step — 96 files, about 0.43 GB compressed and 1.3 GB raw (BP §4.6). Inside the scheduler container, so that the task-process limit applies, build the whole day with the daily pattern chosen in SK1 Q5: DuckDB reads every column as text with GCAM dropped and with `memory_limit` and a spill directory set, and the result goes into a scratch Iceberg table. At the same time ClickHouse aggregates that table through a pinned `lake` table. Record peak memory and CPU of the task processes and of ClickHouse, and the duration. The code lives in `benchmarks/p0_memory_probe/` and is labelled a probe, not a parser.
3. `docs/reports/p0-memory.md`: the BP §9.1 table with the estimate beside the measured values — idle with and without limits, the tracer run, the probe. The conclusion is limited to what was measured: whether a daily silver build fits the 2 GB task budget, and whether anything already contradicts the 8 GB host (R4). The host column and the load of the large marts stay "to be measured" (P1 to P3).

### WP-11 — ADR-004, reports and close-out (2 h)

1. `docs/adr/004-iceberg-glue-local-backend.md` with SK1 and SK2 as its evidence, including the consequences listed in D8. A decision without evidence is marked provisional (BP §15.4). Drafts of ADR-006 and ADR-010 exist only where SK2 or SK3 took a fallback (BP §16.4).
2. `docs/reports/p0-spikes.md`: assemble the sections written during the spikes — for each question the result, the evidence (test name and versions), the decision and any follow-up.
3. `docs/reports/p0-walking-skeleton.md`: the exit gate table with links to evidence; the pin table; rows, bytes and durations of the demo; open items.
4. README: what works now, how to run `make demo`, the limits of the week-1 slice, attribution.
5. Drop `gbmi_ci_silver` and the current objects under `ci/` (H8); confirm `terraform plan` is clean; tag the commit `p0`.

---

## 8. Repository at the end of the week

```text
gbmi-platform/
├── README.md, LICENSE, NOTICE, Makefile, .gitignore, .env.example, .python-version
├── GLOBAL_BUSINESS_MARKET_INTELLIGENCE_PROJECT_BLUEPRINT.md, WEEK_1_IMPLEMENTATION_PLAN.md
├── pyproject.toml, uv.lock, .pre-commit-config.yaml, .sqlfluff, .checkov.yaml
├── src/gbmi/
│   ├── config.py, logging.py, cli.py
│   ├── core/                 export schema (61 columns), hashing and checksum helpers
│   ├── sources/gdelt/        index lines, download, verification
│   ├── lake/                 catalog factory, table definitions, stage–audit–commit
│   ├── transforms/duckdb/    parse and cast SQL, runner
│   ├── quality/              accounting identities, pre-commit audit
│   └── warehouse/            ClickHouse client, pin step
├── dags/                     tracer_gdelt_events.py, common/
├── dbt/                      pyproject.toml, uv.lock, dbt_project.yml, profiles/,
│                             macros/ (schema names, partitions, materializations/),
│                             models/staging/gdelt/, models/marts/news/
├── db/clickhouse/init/       databases, roles
├── docker/                   airflow/ (Dockerfile, requirements.txt), clickhouse/, postgres/init/
├── compose/                  compose.yaml, compose.bi.yaml
├── infra/terraform/          bootstrap/, modules/{lake_s3,glue_catalog,budgets}/, envs/prod/
├── tests/                    unit/, transforms/, integration/{lake,warehouse,dbt}/, e2e/,
│                             fixtures/gdelt/v2/
├── benchmarks/               source_profile/, p0_memory_probe/
├── monitoring/metabase/      provision.py
├── config/labelling/         gold_sheet_v0.csv
├── docs/                     adr/, reports/
└── .github/workflows/        ci.yml, dbt.yml, e2e.yml, terraform.yml
```

Directories of BP §10 that have no content yet (`drills/`, `docs/runbooks/`, `src/gbmi/ledger/`, `src/gbmi/er/` and others) are not created empty.

---

## 9. Verification

| Check | Command | Expected |
|---|---|---|
| Tooling | `make setup lint test` | Clean on a fresh clone |
| Stack | `make up && make smoke` | All services healthy; every smoke check passes |
| Spikes, local | `make integration` | SK1 and SK2 tests pass; rejected options show as expected failures |
| Spikes, AWS | `aws login --remote --profile gbmi-login`, then `GBMI_TEST_AWS_LAKE_BUCKET=<lake bucket> make integration-aws` (D13) | The AWS halves pass. After WP-11, `gbmi_ci_silver` is gone and no current objects remain under `ci/` |
| Incremental contract | The SK3 suite, in the `dbt` workflow or locally against the Compose ClickHouse | T1–T6 pass for the chosen strategy, by day and by month |
| End to end | `make demo`, twice | Passes; the second run leaves checksums unchanged; the dashboard shows one chart and the attribution |
| From nothing | `make reset && make demo` | Passes in under ten minutes **[E]** (BP §15.3) |
| CI | Merge the last pull request | `ci`, `dbt`, `e2e` and `terraform` are green on `main` |
| Infrastructure | `make plan` | No changes |
| Budget | `aws budgets describe-budgets` | `gbmi-prod-monthly` is listed with the default amount |

---

## 10. Risks, time boxes and cut order

| Risk | Signal | Response |
|---|---|---|
| A spike overruns | Its time box passes without a pass | Take the documented fallback and record it; where the package names none, stop and escalate (BP §16.4) |
| AWS access is not ready on day 1 | H1 incomplete | Do WP-02, WP-04, WP-05 and the local halves of the spikes first; WP-09 may start on the local decisions. WP-03 and the AWS halves follow |
| The stack does not fit in 9 GB | Out-of-memory kills during `make demo` | Start Metabase only for the chart step; lower ClickHouse's limit; record it in the memory report (R4) |
| CI is slow or unreliable on the arm64 runner | `e2e` takes over 20 minutes or fails intermittently | Layer cache; run the DAG with `airflow dags test` in CI |
| macOS and Linux differ on paths or permissions | The lake mount is unreadable in CI | `AIRFLOW_UID`; a world-readable lake directory in CI |
| The week runs long (R27) | More than 34 hours are spent and WP-09 has not started (28 are planned before it) | Apply the cut order |

**Cut order** inside the 40 hours, first to go at the top:

| Cut | Hours freed | Consequence |
|---|---|---|
| The GKG probe in WP-10 | 1 | Moves to P1 (D2); exit gate row 7 records it as moved |
| SK3 tests T5 and T6 | 0.5 | Failure recovery of the mart build is first tested in drill DR2 (P1 to P3) |
| The dialect checks in WP-07 | 0.5 | BP §17.3 item 16 stays open until P2 |
| SK1 questions Q6 and Q7 on AWS (kept locally) | 0.5 | Confirmed on AWS in P4 and P5, where they are first used |
| SK1 pattern (b) of Q5 | 0.25 | Only pattern (a) is tested |

**Never cut** (BP §16.5): the three spike decisions; `make demo` in CI with the chart check; the Terraform baseline with the budget; the build-twice test.

---

## 11. Hand-off to week 2

P1 starts when §2 holds. It may also start while row 10 is the only thing open and elapsed time is the only thing missing (D11); every other row has to be true.

- **P1 inherits** the catalog factory and commit helper (SK1), the pin step (SK2), the chosen incremental strategy (SK3), the Compose stack, the fixtures and the `export` schema.
- **P1 must read first** SK1 questions Q2 and Q5 — the overwrite unit and the bounded-memory pattern decide how `gdelt_silver_hourly` and `gdelt_silver_daily` write — and the conclusion of the memory report.
- **Open items carried forward:** file clean-up on the local backend and clean-up of orphaned dbt staging tables (P4); cost-allocation tags (P4); SEC fixtures and the five demo companies (P2); any stretch item not taken.
- **Optional, one request a day.** Company attributes cannot be backfilled before the first snapshot (BP §7.10, §8.5). Saving `company_tickers_exchange.json` to bronze each day from now would preserve ticker and name history ahead of P2. It needs the declared SEC user agent, and the blueprint rates the loss as low impact (R16), so it is a choice, not a requirement.
