# ADR-001 — Scope, non-goals and success criteria

| | |
|---|---|
| **Date** | 2026-11-25 |
| **Status** | Accepted |
| **Sources** | Blueprint §1.3, §2, §3, §11.1, §16 |
| **Related** | TD-01 (blueprint §6.2) |

## Context

The project began from a one-page brief for a "global business and market intelligence platform": collect open data, process it, and show it on a dashboard. The brief is a good starting shape but leaves the hard questions open — which sources, how much data, what "correct" means, what it may cost, and when it is finished.

Three constraints are fixed. The work is done by **one developer** at about 40 hours a week. The cloud budget is about **USD 30 a month**. And the project has two purposes at once: to be a useful platform, and to close the developer's verified gaps in data engineering — production Python, Airflow, dbt, a columnar warehouse, a lakehouse table format, Spark, Terraform and data quality (blueprint §1.3, objective O2).

Without a written scope, a solo project of this size fails in one of two ways: it grows until it is never finished, or it is declared finished without a way to check. This record fixes what is built, what is deliberately left out, and the measurements that decide whether each milestone has been reached.

## Decision

### What is built

An **entity-resolved company-intelligence lakehouse** (blueprint §1.1, §16.1):

- **Sources.** The GDELT 2.0 English feed (Events, Mentions, Global Knowledge Graph) and SEC EDGAR (company reference data, the filings index, XBRL company facts).
- **Universe.** About 8,000 US-listed registrants with tickers, and a watchlist that grows from 50 companies (MVP) to 500 (production-grade).
- **History.** Twelve months of GDELT data, the last eleven of them loaded by a bulk Spark job.
- **Processing.** Batch and micro-batch only. Raw files land unchanged; silver tables are Apache Iceberg, replaced one partition at a time after an audit; marts are dimensional models in ClickHouse built by dbt.
- **The centrepiece.** Entity resolution that links organisation strings in news to SEC registrants, with every decision recorded and its precision *measured* on a held-out labelled set.
- **Serving.** Four Metabase dashboards — company watchlist, industry trends, review queues, pipeline health — and a published data dictionary.
- **Operation.** One scheduled AWS host in one region, defined in Terraform, with service-level objectives, scripted failure drills, backups and a tested restore.
- **A data mining layer**, added only after the core is frozen: five classical algorithm families, each promoted into the dashboards only if it beats the rule it would replace.

### What is not built

| Excluded | Reason |
|---|---|
| Article full text; scraping publisher sites | Copyright and terms of use. GDELT supplies metadata only |
| Market prices and returns | No free source permits redistribution |
| Deep learning, language models in the core path, forecasting of prices or events | They need labels and compute this project cannot justify, and predicting markets is not its purpose |
| A custom web application | Already well represented in the developer's portfolio; Metabase serves the users |
| The GDELT translingual feed | About 24 GB a month more data without a new engineering lesson |
| Private companies and non-US registries | Different identifiers and sources |
| Multi-tenant access control and personal-data handling | Public data, single operator |
| High availability | A single host with a tested recovery is the honest design at this budget |
| Kubernetes, managed Airflow, a local S3 emulator, a separate metrics stack, a second data-quality framework | Tooling that does not pay for itself at this size (blueprint §16.5) |

### How "done" is measured

The project has three milestones. Each is reached only when its criteria are met, and every criterion is computed from data the platform records (blueprint §3.4, §3.6).

| Milestone | Planned | Criteria (summary) |
|---|---|---|
| **MVP** | End of week 4 | Thirty consecutive days of data for a 50-company watchlist, queryable on the AWS host; completeness ≥ 99.5% and row accounting 100%; two dashboards with working filters; the backfill and kill-and-rerun drills pass with identical checksums; `make demo` runs in CI; the data dictionary is published |
| **Production-grade** | End of week 10 | A 14-day soak on the final configuration meets the freshness, completeness, correctness and reliability targets; all ten drills pass and are written up as runbooks; twelve months processed by Spark with a parity suite and a benchmark; match precision at auto-accept ≥ 0.95 with a 95% Wilson lower bound ≥ 0.90, enforced in CI; a normal month costs ≤ USD 30; at least eight decision records with a measurement each |
| **Data mining layer** | End of week 13 | Five mining tasks, each with a published evaluation against its baseline and a recorded promote-or-reject decision; reproducible refits; promotions that leave earlier dates unchanged; a demonstrated rollback |

The service-level targets that the soak is judged against are: GDELT data in silver within 45 minutes and in marts within 90 minutes at the 95th percentile; completeness ≥ 99.5% evaluated 72 hours after a day ends; zero duplicate keys and zero failed error-severity tests in published marts; a zero checksum difference between two builds of a closed day; ≥ 98% of scheduled runs succeeding without manual action.

### Rules that protect the scope

1. **A phase does not start until the previous phase's acceptance criteria are met** (blueprint §16.4).
2. **The core's scope is frozen at the production-grade milestone.** The data mining layer is built on top and changes the core only at three gated extension points.
3. **If time runs short, features are cut in a fixed order** (blueprint §16.5), starting with the optional modules. The following are never cut: the ledger state machine and backfill drill, pre-commit audits, the incremental contract with its build-twice test, the type-2 company dimension and bitemporal facts, the versioned bridge and gold-set evaluation, CI, Terraform, and the recovery drill.

## Alternatives considered

| Alternative | Why it was not chosen |
|---|---|
| Build the brief as written: Parquet on a local disk, no cloud, no infrastructure as code, no CI | It would not close the cloud, lakehouse, Terraform or CI/CD gaps, and nothing in it could be operated or measured |
| A general "market intelligence" platform over many sources | Breadth without depth. Two sources that must be reconciled teach more than ten that are only loaded |
| A dashboard-first project | The dashboard is the easy part. The value is in ingestion that survives gaps and rate limits, idempotent reprocessing and measured matching |
| Use BigQuery's public GDELT tables | It removes the ingestion problem the project exists to demonstrate |
| Add streaming, Kubernetes and a second warehouse to cover more keywords | Each would take weeks without improving the data platform. Streaming stays an optional, self-contained lab |
| No fixed success criteria ("build until it feels complete") | A solo project needs an external definition of done; without one the roadmap cannot be checked or cut |

## Consequences

- **Easier:** every later decision can be tested against a written scope; the roadmap has a cut order; the milestones can be audited from recorded data.
- **Harder:** two sources with real defects — permanent gaps in the GDELT archive, SEC rate limits, restated financial facts — have to be handled properly, not avoided.
- **Given up:** high availability, real-time processing, and the name recognition of a cloud warehouse. Recall of company matches is traded for precision, because a wrong link damages trust more than a missing one.
- **Accepted risk:** the scope is large for one person (blueprint risk R8, rated high likelihood and high impact). The mitigations are the MVP at week 4, the scope freeze, the cut order, and a stated contingency of about 50% on planned effort.

## Evidence

This is a scope decision, so its evidence is the research behind the blueprint, not a benchmark:

- Direct, read-only measurement of the live GDELT feed on 2026-11-25: 13.3 GB a month compressed, of which the knowledge graph is 96%; 72.7% of knowledge-graph records name organisations; 1,311 of 2,880 expected file slots exist for June 2025 (blueprint §4.2, §4.6).
- The sources' terms: GDELT permits unrestricted use with a citation and a link; the SEC requires a declared user agent and at most 10 requests a second (blueprint §4.7).
- The effort estimate: about 370 planned hours to the production-grade milestone and 120 more for the data mining layer (blueprint §11.1).

Whether the criteria are *met* is recorded at each milestone, in `docs/reports/`.

## Status

**Accepted** on 2026-11-25. It is revisited at the MVP milestone, and whenever a phase's acceptance criteria cannot be met without changing the scope.
