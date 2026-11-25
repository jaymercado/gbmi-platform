# GBMI Platform

An entity-resolved company-intelligence lakehouse: it ingests GDELT 2.0 news metadata and SEC EDGAR filings, links the organisation names found in news to SEC registrants, and serves dimensional models to a dashboard — built to be run by one person for about USD 30 a month.

## Status

**Phase P0 — walking skeleton and de-risking (week 1).** Nothing here is production data yet. The goal of this phase is one hour of GDELT Events flowing end to end on a laptop — source → bronze → silver (Apache Iceberg) → ClickHouse → dbt mart → Metabase chart, run by Airflow — and a recorded decision for each of three technical risks.

| Document | Purpose |
|---|---|
| [Project blueprint](GLOBAL_BUSINESS_MARKET_INTELLIGENCE_PROJECT_BLUEPRINT.md) | The design and its evidence; the source of truth |
| [Week 1 implementation plan](WEEK_1_IMPLEMENTATION_PLAN.md) | What phase P0 builds and how it is checked |
| [Decision records](docs/adr/) | Why the design is what it is |

## The problem

Open data says a great deal about companies — news attention, regulatory filings, financial trajectory — but not in a usable form. News metadata names organisations as noisy free text; filings identify companies by a registrant number, not by the names the press uses; financial facts are restated over time. The value of this platform is the link between the two sources and the evidence behind it: every company-level number can be traced to a source file, a filing or an article URL, and to the version of the rule that produced it.

## Run it

Requirements: macOS or Linux on arm64 or x86-64, [uv](https://docs.astral.sh/uv/), GNU Make, and Docker with about 9 GB of memory for the full stack.

```bash
make setup   # create the virtual environments and install the Git hooks
make lint    # ruff, mypy, sqlfluff, lock-file checks
make test    # unit tests
```

`make demo` — the whole pipeline on committed fixtures, with no cloud account — arrives with the tracer pipeline later in this phase.

## Data sources and attribution

- **GDELT.** This project uses data from [The GDELT Project](https://www.gdeltproject.org/), which permits unrestricted use and redistribution with a citation and a link to its website. Only GDELT metadata, article titles and URLs are stored; no article text is fetched or kept.
- **SEC EDGAR.** This project uses public data from the EDGAR system of the [U.S. Securities and Exchange Commission](https://www.sec.gov/). It is not affiliated with or endorsed by the SEC. Automated access declares a user agent and stays below the SEC's published request limit.

See [NOTICE](NOTICE) for the full attribution text.

## Licence

Code is licensed under the [Apache License 2.0](LICENSE).
