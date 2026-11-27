# GBMI Platform

An entity-resolved company-intelligence lakehouse. The platform ingests GDELT 2.0 news metadata and SEC EDGAR filings, links the organisation names found in news to SEC registrants, and serves dimensional models to a dashboard. The design goal: one person runs the platform for about USD 30 a month.

## Status

**Phase P1 — GDELT at production quality — is done; the tag `p1` marks the commit.** Three scheduled jobs land each GDELT file of three feeds, verify the file, check each row behind three quality gates and commit the rows to four Apache Iceberg tables. A ledger in PostgreSQL holds one row for each expected file, so a missing file is visible and a rerun is safe.

```text
GDELT files → ledger (each expected file) → bronze (verified zips) → silver (Apache Iceberg, four tables)
            → ClickHouse (pinned read) → dbt models → Metabase chart          run by Airflow 3.4
```

| | |
|---|---|
| What runs | Three DAGs: `gdelt_ingest` and `gdelt_silver_hourly` every 15 minutes, `gdelt_silver_daily` once a day at 08:25 UTC. `make demo` runs the tracer DAG and each of the three DAGs once on twelve recorded GDELT files, then builds a chart and checks the result end to end |
| Proven on 30 real days | The 8,640 files of 2026-10-26 to 2026-11-24 (12.9 GB) are in silver. Completeness is 100% for each day. Accounting is 100%: 14,395,666 raw lines = parsed rows + quarantined rows. A second build of a day gives the same checksum. See the [P1 status report](docs/reports/p1-gdelt-production.md) and the [quarantine and reconciliation report](docs/reports/p1-quarantine-reconciliation.md) |
| Proven on the live source | The jobs ran for 6 hours and 17 minutes in a row without manual action. A file was in silver about 6 minutes after the upload by GDELT. The laptop slept four times during the 21 hours of the run, and the pipeline caught up without help each time |
| Measured | [Benchmark report v1](docs/reports/benchmark-v1.md): the largest day of the largest feed builds in 20 seconds with a peak of about 1.0 GiB of memory; 30 days need 12.9 GB as zip files and 0.8 GB in silver; the master list of GDELT lacks 1.8% of the 15-minute slots since 2015 |
| Failure drills | Three scripts (`make drill`): a crash during the silver commit, a corrupt file, a rerun of a day. Two [runbooks](docs/runbooks/) say what to do when the pipeline reports a problem |
| Decided | Four decision records so far, each with evidence. The newest one is [ADR-007](docs/adr/007-ledger-state-machine-and-completeness.md): the ledger state machine and the meaning of completeness. The [spike report](docs/reports/p0-spikes.md) and the [memory report](docs/reports/p0-memory.md) are from phase P0 |
| On GitHub and AWS | The four workflows are green on `main`. The Terraform baseline of phase P0 is applied in `us-east-1`; phase P1 changed no AWS resource |
| Labelled | The first 200 organisation strings of the entity-resolution gold set, by an AI model; a review by a person is open. See the [labelling README](config/labelling/README.md) |
| Not done yet | Nothing is deployed: no host exists, and the schedules ran on a laptop. No SEC data is ingested, and no organisation name is linked to a company |

The 30 days are real GDELT data on one laptop, not a production service. SEC ingestion and the first deployment are phase P2, entity resolution is phase P3 of the [blueprint](GLOBAL_BUSINESS_MARKET_INTELLIGENCE_PROJECT_BLUEPRINT.md). The plan of the week is [WEEK_2_IMPLEMENTATION_PLAN.md](WEEK_2_IMPLEMENTATION_PLAN.md).

## Run the demo

Requirements: Docker with 8 GB of memory or more, [uv](https://docs.astral.sh/uv/) and GNU Make. No cloud account is needed. The demo has run on macOS on Apple silicon and, in a container rehearsal, on Linux on arm64.

```bash
make setup   # virtual environments, Git hooks, and a .env with generated secrets
make demo    # start the demo stack, run the pipeline on the recorded files, build the chart, check the result
```

The first run pulls four images and builds one; on a fast connection the first run took about four minutes in all. A later run of `make demo` takes about two minutes. After a pass:

| | |
|---|---|
| Dashboard | <http://127.0.0.1:3000> — sign in with `METABASE_ADMIN_EMAIL` and `METABASE_ADMIN_PASSWORD` from `.env` |
| Airflow | <http://127.0.0.1:8080> — user `admin`, password `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| Stop | `make down STACK=gbmi-demo` keeps the data of the demo; `make reset STACK=gbmi-demo` removes the data |

`make demo` runs on a separate stack, the demo stack (Compose project `gbmi-demo`, lake `data/demo/lake`). The working stack (`make up`, Compose project `gbmi`, lake `data/lake`) holds real data. Both stacks publish the same ports, so one stack runs at a time, and a command names the demo stack with `STACK=gbmi-demo`.

Other commands (`make help` lists every target):

```bash
make lint test      # ruff, mypy, sqlfluff, lock files; unit, ledger, transformation and contract tests (the ledger tests use the PostgreSQL of the running stack)
make up smoke       # start the core profile of the working stack and check the stack
make test-dags      # the DAG tests, with the interpreter of Airflow inside the image; needs a running stack
make integration    # the integration tests against the running stack: the three spikes, the ingestion and the silver build
make drill DR=2     # a failure drill on the recorded files: 2 (crash during the silver commit), 4 (corrupt file), 8 (rerun of a day)
make bench B=4      # a benchmark on the real days of the working stack: 1 (ingestion), 2 (transformation), 4 (storage)
make psql SQL="SELECT state, count(*) FROM ops.source_slot GROUP BY state"   # one SQL statement on the ledger database
make integration-aws  # the Glue/S3 halves of the spikes; needs an AWS sign-in (infra/terraform/README.md)
make tf-check       # Terraform format, validate, tflint and checkov, with no AWS access
```

Two runbooks say what to do when the pipeline reports a problem: [a slot is quarantined or overdue](docs/runbooks/gdelt-slot-quarantined-or-overdue.md) and [a silver build failed, or silver holds wrong data](docs/runbooks/silver-build-failed-or-wrong-data.md).

## Run the pipeline on real data

The working stack runs the three DAGs on the live GDELT files. Read the runbook for [silver builds](docs/runbooks/silver-build-failed-or-wrong-data.md) before the first run: the order of the steps matters.

```bash
make up                                                                    # the working stack
make toolbox CMD="gbmi gdelt enqueue --from <first day> --to <last day>"   # one ledger row for each expected file of a range of closed UTC days
make toolbox CMD="gbmi ledger status --from <first day> --to <last day>"   # the state of each file, by feed and day
```

Then unpause the DAGs in Airflow. `gdelt_ingest` downloads and verifies the listed files. The command `airflow backfill create` builds each closed day with `gdelt_silver_daily`; the runbook has the arguments. `gdelt_silver_hourly` builds the files of the current day. The [P1 status report](docs/reports/p1-gdelt-production.md) shows the same steps for 30 days, with the times. One day of the three feeds has about 430 MB of zip files.

## How the platform is built

| Layer | What | Where |
|---|---|---|
| Ledger | One row for each expected file, with seven states and twelve guarded transitions; the completeness of a day comes from the ledger | `src/gbmi/ledger/`, `db/postgres/migrations/` |
| Ingestion | A file is verified against the size and the MD5 hash of the GDELT file list and stored unchanged; a rerun downloads nothing | `src/gbmi/sources/gdelt/` |
| Silver | Each row is checked field by field before the cast to a type; a bad row goes to quarantine with a reason; a unit, one hour or one day, is replaced in one Iceberg commit after an audit | `src/gbmi/transforms/`, `src/gbmi/quality/`, `src/gbmi/lake/` |
| Warehouse | ClickHouse reads a table pinned to one Iceberg metadata file, so a build never sees a table in the middle of a write | `src/gbmi/warehouse/` |
| Marts and operational models | dbt models; a large fact is rebuilt one partition at a time with the materialisation `partition_replace`; the completeness model reads the ledger | `dbt/` |
| Orchestration | Airflow 3.4 with LocalExecutor: three scheduled DAGs and the tracer DAG; one image, three isolated Python environments (Airflow, `gbmi`, dbt) | `dags/`, `docker/airflow/`, `compose/` |
| Proof | Failure drills, benchmarks with raw results, reports | `drills/`, `benchmarks/`, `docs/reports/` |
| Infrastructure | Terraform for the AWS baseline: state, CI identities, two buckets, the Glue database and a budget | `infra/terraform/` |

The decision records, each with evidence, are in [docs/adr](docs/adr/).

## Limits of the slice

- **One source.** The three GDELT feeds are parsed: Events, Mentions and the Global Knowledge Graph. SEC data comes in phase P2.
- **The pipeline runs on the local backend.** The lake is a directory and the catalog is SQLite. The tests of the spikes pass on S3 and Glue, but no pipeline runs there yet.
- **No deployment.** No host, no alert by email and no monitoring exist yet. A failed gate writes a log line and a row in `ops.dq_result`.
- **The routine jobs look back seven days.** A file which arrives for an older day, or a file which the source does not serve for an older day, needs the steps of a runbook. The repair queue of phase P4 closes the gap.
- **One mart.** The events mart of phase P0 is the only mart. The marts on mentions and organisations come in phase P3.

## Data sources and attribution

- **GDELT.** The platform uses data from [The GDELT Project](https://www.gdeltproject.org/), which permits unrestricted use and redistribution with a citation and a link to the GDELT website. Only GDELT metadata, article titles and URLs are stored; no article text is fetched or kept. The files under `tests/fixtures/gdelt/` are GDELT files: the Events and Mentions files are redistributed unchanged, and each of the four GKG files holds the first 60 records of the real file ([ATTRIBUTION.md](tests/fixtures/gdelt/ATTRIBUTION.md)).
- **SEC EDGAR.** The platform will use public data from the EDGAR system of the [U.S. Securities and Exchange Commission](https://www.sec.gov/). The platform is not affiliated with the SEC and not endorsed by the SEC. Automated access declares a user agent and stays below the published request limit of the SEC.

See [NOTICE](NOTICE) for the full attribution text.

## Licence

Code is licensed under the [Apache License 2.0](LICENSE).
