# GBMI Platform

An entity-resolved company-intelligence lakehouse: it ingests GDELT 2.0 news metadata and SEC EDGAR filings, links the organisation names found in news to SEC registrants, and serves dimensional models to a dashboard — built to be run by one person for about USD 30 a month.

## Status

**Phase P0 — walking skeleton and de-risking — is done; the tag `p0` marks the commit.** One hour of GDELT Events flows end to end with one command:

```text
GDELT files → bronze (verified zips) → silver (Apache Iceberg) → ClickHouse (pinned read)
            → dbt mart (partition-replaced) → Metabase chart          run by Airflow 3.4
```

| | |
|---|---|
| What runs | `make demo`: the tracer DAG on four committed GDELT slots, then a chart and end-to-end assertions |
| What it proves | 4,023 raw lines = 4,023 rows in silver = 4,023 events in the mart = the chart's total; a second run changes nothing |
| Decided by spikes | How PyIceberg commits a unit, how ClickHouse reads one exact table version, how dbt replaces a partition, and that Cosmos runs dbt on Airflow 3.4 — see the [spike report](docs/reports/p0-spikes.md) |
| Measured | [Memory](docs/reports/p0-memory.md): about 2 GB idle, 4 GB during the demo; a whole day of the largest feed builds in 7 seconds with a 0.5 GB peak |
| On GitHub and AWS | The four workflows are green on `main`. The Terraform baseline is applied in `us-east-1`, and the spike tests pass on Glue and S3 — see the [status report](docs/reports/p0-walking-skeleton.md) |
| Labelled | The first 200 organisation strings of the entity-resolution gold set, by an AI model; a review by a person is open — see the [labelling README](config/labelling/README.md) |
| Not done yet | Nothing is deployed: there is no host, no schedule and no monitoring |

Nothing here is production data yet: there is no ledger, no SEC ingestion and no entity resolution. Those are phases P1 to P3 of the [blueprint](GLOBAL_BUSINESS_MARKET_INTELLIGENCE_PROJECT_BLUEPRINT.md).

## Run it

Requirements: Docker with 8 GB of memory or more, [uv](https://docs.astral.sh/uv/) and GNU Make. No cloud account is needed. It has been run on macOS on Apple silicon and, in a container rehearsal, on Linux on arm64.

```bash
make setup   # virtual environments, Git hooks, and a .env with generated secrets
make demo    # start the stack, run the pipeline on the fixtures, build the chart, check it
```

The first run pulls four images and builds one; on a fast connection that took about four minutes in all. After that `make demo` takes under two minutes. When it has passed:

| | |
|---|---|
| Dashboard | <http://127.0.0.1:3000> — sign in with `METABASE_ADMIN_EMAIL` and `METABASE_ADMIN_PASSWORD` from `.env` |
| Airflow | <http://127.0.0.1:8080> — user `admin`, password `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| Stop | `make down` keeps the data; `make reset` removes it |

Other commands (`make help` lists them all):

```bash
make lint test      # ruff, mypy, sqlfluff, lock files; unit and transformation tests
make up smoke       # start the core stack and check it
make integration    # the three spikes, against the running stack
make integration-aws  # the Glue/S3 halves of the spikes; needs an AWS sign-in (infra/terraform/README.md)
make tf-check       # Terraform format, validate, tflint and checkov, with no AWS access
```

To run the pipeline on live data instead of the fixtures, trigger the DAG `tracer_gdelt_events` in Airflow with `source` set to `live` and an `hour` such as `2026-11-25T08`.

## How it is built

| Layer | What | Where |
|---|---|---|
| Ingestion | Files are verified against GDELT's size and MD5 and stored unchanged; a rerun downloads nothing | `src/gbmi/sources/gdelt/` |
| Silver | Rows are validated field by field before they are typed; bad rows go to quarantine with a reason; a unit is replaced in one Iceberg commit after an audit | `src/gbmi/transforms/`, `src/gbmi/quality/`, `src/gbmi/lake/` |
| Warehouse | ClickHouse reads a table pinned to one Iceberg metadata file, so a build never sees a table that is being written | `src/gbmi/warehouse/` |
| Marts | dbt models; large facts are rebuilt one partition at a time with the `partition_replace` materialisation | `dbt/` |
| Orchestration | Airflow 3.4 with LocalExecutor; one image, three isolated Python environments (Airflow, `gbmi`, dbt) | `dags/`, `docker/airflow/`, `compose/` |
| Infrastructure | Terraform for the AWS baseline: state, CI identities, two buckets, the Glue database and a budget | `infra/terraform/` |

Decisions and their evidence are in [docs/adr](docs/adr/).

## Limits of this slice

- **One feed, one hour.** Only GDELT Events is parsed. Mentions, the Global Knowledge Graph and all SEC data come in phases P1 and P2.
- **No ledger.** A missing source file is simply absent; tracking every expected slot through a state machine is phase P1.
- **The demo runs on the local backend.** The lake is a directory and the catalog is SQLite. The same code passes its tests on S3 and Glue, but no pipeline runs there yet.
- **No deployment.** There is no host, no schedule and no monitoring yet.

## Data sources and attribution

- **GDELT.** This project uses data from [The GDELT Project](https://www.gdeltproject.org/), which permits unrestricted use and redistribution with a citation and a link to its website. Only GDELT metadata, article titles and URLs are stored; no article text is fetched or kept. The fixtures under `tests/fixtures/gdelt/` are GDELT files, redistributed unchanged.
- **SEC EDGAR.** This project will use public data from the EDGAR system of the [U.S. Securities and Exchange Commission](https://www.sec.gov/). It is not affiliated with or endorsed by the SEC. Automated access declares a user agent and stays below the SEC's published request limit.

See [NOTICE](NOTICE) for the full attribution text.

## Licence

Code is licensed under the [Apache License 2.0](LICENSE).
