# GBMI Platform — Week 2 Implementation Plan

**Phase P1: GDELT at production quality**

| | |
|---|---|
| **Document** | Implementation plan for week 2 of the project blueprint (phase P1 in BP §11.2). Planning only: no code of phase P1 exists yet |
| **Version / date** | 1.0 — 26 November 2026. §3.5 lists every change made after the approval |
| **Prepared for** | Jay Mercado (solo developer) |
| **Source of truth** | `GLOBAL_BUSINESS_MARKET_INTELLIGENCE_PROJECT_BLUEPRINT.md` v1.1, cited as "BP §…". The results of phase P0 are cited from three reports in the repository: `docs/reports/p0-walking-skeleton.md`, `docs/reports/p0-spikes.md` and `docs/reports/p0-memory.md`. §3.4 lists every difference between the plan and the blueprint |
| **Execution model** | **One phase.** Twelve work packages run once, in order, towards one exit gate (§2). The plan has no sub-phases, no intermediate milestones and no sign-offs |
| **Planned effort** | 45 hours (BP §11.2); about 68 hours with the contingency of 50% (BP §11.1) |
| **Starting point** | The tag `p0` on commit `989ccfb`; `main` at `b0e91ef` **[V]** |
| **Illustrative dates** | Monday 7 December – Sunday 13 December 2026 (BP §11.1). Phase P0 ended on 26 November 2026, so week 2 can start earlier |
| **Tags** | **[V]** verified on 2026-11-26 for the plan: on the laptop, in the running containers, at the package index, or by a read-only request to the live GDELT servers · **[R]** decision made in the plan · **[E]** estimate · **[A]** assumption. A statement cited to the blueprint keeps the tag which the blueprint gives |

> **What approval of the plan triggers.** One action only: the document is saved as `WEEK_2_IMPLEMENTATION_PLAN.md` next to the blueprint in the project folder. No code is written, no GDELT file is stored and no commit is made until Jay starts week 2 explicitly (§5, action J1).

---

## 1. Context

Phase P0 proved every integration once. One hour of the GDELT Events feed flows from the source to a chart. Three spikes fixed how the platform commits to the lake, how ClickHouse reads one exact table version and how dbt replaces a mart partition. The tracer of phase P0 has no memory. A missing source file is simply absent, only one of the three feeds is parsed, and no query can say whether a day is complete.

Week 2 (phase P1) builds the three parts which make the GDELT ingestion trustworthy:

1. **A ledger.** The ledger is a PostgreSQL table with one row for every expected source file. GDELT publishes one file for each of three feeds (Events, Mentions and the Global Knowledge Graph, short GKG) for each 15-minute *slot*. One day has 96 slots and 288 expected files. Each ledger row moves through explicit states, from `expected` to `loaded` or `confirmed_missing`. The ledger is the only place which knows what exists and what is pending (BP §5.1, principle 4).
2. **All three feeds in silver, behind three quality gates.** Gate G1 checks each downloaded file, gate G2 checks each row, and gate G3 audits each unit of work before the commit (BP §5.7). Every raw line ends as a silver row, as a counted duplicate or as a quarantined row with a reason.
3. **Thirty days as proof.** A backfill of 30 closed days runs through the same code as the routine jobs. The backfill gives the first measured numbers of the project: completeness, accounting, throughput, memory and storage.

**Intended outcome:**

- 30 consecutive closed UTC days of Events, Mentions and GKG are in four silver tables on the laptop.
- The ledger shows a completeness of at least 99.5% for each day and an accounting of 100%.
- A second build of a closed day gives equal checksums.
- Three scheduled DAGs run on the live source.
- `make demo` and the four workflows stay green.

The preparation of the plan found thirteen facts which shape the week. Each fact is **[V]**: measured on 2026-11-26 with read-only requests, or read from the installed software.

| # | Finding | Evidence | Effect on the plan |
|---|---|---|---|
| F1 | GDELT uploads a file 9.2 to 11.1 minutes **before** the time in the file name. Example: the files named `20261126140000` were uploaded at 13:50:30 UTC | HTTP header `last-modified` of 27 files (9 slots between 2026-11-19 and 2026-11-26) against the stamp in each file name | The calendar creates expected rows up to 15 minutes ahead of the clock. The ingest job polls at minutes 8, 23, 38 and 53, which is 2 to 4 minutes after an upload (D15). The ledger stores the upload time of each file |
| F2 | One silver run per hour cannot meet the freshness targets of the blueprint. Four files arrive in each hour, 15 minutes apart. With the minutes of BP §8.1 (ingest at 3, 18, 33, 48; silver at 20) the four files wait 17, 2, 47 and 32 minutes. One file in four waits longer than 45 minutes, so the 95th percentile is above the target of 45 minutes (BP §3.4) and above the target of 35 minutes (BP §8.4) | Arithmetic on the schedule table of BP §8.1 | `gdelt_silver_hourly` runs every 15 minutes. The unit of work stays one slot-hour (D16) |
| F3 | The master file list (`masterfilelist.txt`) is 129,858,361 bytes and serves byte ranges. The list holds three lines for each slot, in the order export, mentions, gkg; one line has 106.5 bytes on average. New lines are appended at the end: in the last 18,781 slots, two slots stand out of time order (slot 2026-11-15 02:45 stands after slot 04:30) | Range request for the last 6,000,000 bytes; `HEAD` request | The index reader takes a tail whose size follows from the oldest wanted slot, and never assumes sorted lines. A late file appears at the end of the list, where the daily reconciliation reads |
| F4 | The 30 days from 2026-10-27 to 2026-11-25 are complete in the master list: 8,640 of 8,640 files, 12.95 GB compressed (Events 0.197 GB, Mentions 0.274 GB, GKG 12.478 GB). One GKG day has between 217 MB and 541 MB. From 2026-09-01 to 2026-11-25 no day has fewer than 96 slots in any feed | The same range request | The backfill downloads about 13 GB. Automatic confirmation of missing slots will not occur in real data of the window, so tests with a controlled clock prove the rule |
| F5 | One Mentions file holds repeated pairs of event and article which are not exact copies: 2,880 rows, 2,855 distinct pairs (`GLOBALEVENTID`, `MentionIdentifier`), 2,880 distinct full rows. The repeated rows differ in the character offsets and in `Confidence` | Slot 2026-11-26 13:45 UTC, read in memory | The logical key of BP §7.4 stays. The deduplication keeps the row with the highest confidence and counts the other rows as duplicates (D19) |
| F6 | In one GKG file all 1,047 lines have 27 columns, `V2GCAM` is 69.9% of the bytes, every `GKGRECORDID` is unique and no record is a translation. `V1ORGANIZATIONS` gives names in lower case; `V2ENHANCEDORGANIZATIONS` gives the same names in title case with a character offset. 21 of 783 records with organisations fill only the V1 column. 4 of 12,522 location blocks contain one extra `#` character inside a place name | The GKG file of the same slot | Silver takes organisations from the V2 column and falls back to the V1 column (D20). The normalised string, not the raw string, gives the identifier. The country code is read from the right end of a location block |
| F7 | Every GKG record of the file has a `<PAGE_TITLE>` tag. 99 of 1,047 titles carry XML character references such as `&#xA3;`. 46 normalised titles occur more than once; one title occurs 27 times | The same file | A pure Python function cleans each title. The story identifier is a hash of the normalised title (BP §7.4) |
| F8 | In each file the batch time equals the stamp of the file name for every row: `DATEADDED` (Events), `MentionTimeDate` (Mentions), `V2.1DATE` (GKG). Events mentioned in one Mentions file are up to one year old | The three files of the same slot | Cross-hour duplicates are not possible for Mentions and GKG by construction of the keys; the daily build still checks all three feeds |
| F9 | Airflow 3.4.0 has the command `airflow backfill create --dag-id --from-date --to-date --max-active-runs --reprocess-behavior {none,failed,completed} --run-backwards --dry-run`. A backfill creates only runs whose data interval has ended, and a run of a paused DAG stays queued. `CronTriggerTimetable("25 8 * * *", timezone="UTC", interval=timedelta(days=1))` gives the run which starts at 08:25 on day X the logical date X−1 08:25. The new `CronPartitionTimetable` accepts only an integer offset ("Only int supported in this version") | `airflow backfill create --help`, the source of `airflow.models.backfill` and of `airflow.timetables.trigger`, and one timetable calculation, all in the running scheduler container | `gdelt_silver_daily` uses the trigger timetable with an interval of one day: the date of the logical date is the day to build. A backfill can build a day only after 08:25 UTC of the following day. The partition timetable is not used in P1 |
| F10 | The size of a day is stable against the same weekday: the ratio of one day to the median of the previous four same weekdays lies between 0.86 and 1.15 (58 comparisons for each feed, compressed bytes, 2026-09-01 to 2026-11-25). A Sunday has about half the GKG volume of a Wednesday | The master list | Gate G3 compares a day with the same weekday, as BP §5.7 asks, with a band from 0.5 to 2.0 |
| F11 | DuckDB 2.0.1 rejects the lambda form `x -> …` ("Deprecated lambda arrow (->) detected") and accepts `lambda x: …`. Negative list indexes and `regexp_extract` with the option `'s'` work | One query in the virtual environment of the repository | The parsers use the new lambda form |
| F12 | ClickHouse 26.8.46.6 has the privileges `NAMED COLLECTION`, `CREATE NAMED COLLECTION` and `POSTGRES`; no named collection exists. The PostgreSQL database `gbmi` holds only the schema `public` and no table; the roles are `postgres`, `airflow`, `gbmi` and `metabase` | `system.privileges`, `system.named_collections`, `pg_namespace`, `pg_roles` in the running containers | The schema `ops` starts empty. A read-only PostgreSQL role and one named collection are new in WP-08 |
| F13 | No version pin of phase P0 moved. One library has a newer patch release: typer 0.29.3 (2026-11-26). The newest hypothesis is 6.170.17. The Airflow environment of the image has no pytest. Docker Desktop gives the virtual machine 7.65 GiB | PyPI, `pip list` in the image, `docker info` | Pin table in §4.2. The DAG tests use the standard library. The memory measurements name the size of the virtual machine |

**Code which week 2 reuses.** The package of phase P0 already holds the building blocks; week 2 extends the blocks and adds the ledger around the blocks.

| Existing code | Reuse in week 2 |
|---|---|
| `gbmi.core.gdelt` (`Slot`, `Feed`, `parse_file_name`, `slots_of_hour`, `Slot.bronze_key`) | The slot calendar, the ledger keys and the bronze keys of all three feeds |
| `gbmi.core.gdelt_export` (`EXPORT_COLUMNS`, `Kind`, `SilverColumn`, `EVENT_COLUMNS`) | The pattern for the Mentions and GKG schemas; `Kind` and `SilverColumn` move to a shared module |
| `gbmi.sources.gdelt.index` (`IndexLine`, `parse_index`) and `gbmi.sources.gdelt.land` (`Fetcher`, `HttpFetcher`, `FixtureFetcher`, `verify`, `Bronze`, `land_file`) | Gate G1 and the idempotent landing of one file are extended in two points: the fetcher also returns the upload time, and a file which fails gate G1 is handed back for the quarantine. The ledger-driven ingest calls `land_file` |
| `gbmi.transforms.duckdb.gdelt_event` (`raw_lines`, `check_sql`, `typed_sql`, `parse_events`) and `runner.connect` | The row check and the typed cast become generic over a feed definition |
| `gbmi.quality.audit` (`UnitCounts`, `audit_unit`), `gbmi.quality.quarantine` (`write_quarantine`) | Gate G3 grows from the same function; the quarantine file of a unit keeps the same key rule |
| `gbmi.lake.commit` (`Unit`, `overwrite_unit`, `overwrite_day_streaming`), `gbmi.lake.catalog`, `gbmi.lake.tables` | The commit of an hour and of a day (spike SK1, decisions 1 and 3) |
| `gbmi.core.checksum.content_checksum` | The checksum of a unit for the rerun tests and for drill DR8 |
| `gbmi.warehouse.pin`, the materialisation `partition_replace`, `dags/common`, `tests/e2e/airflow.py` | The tracer DAG and `make demo` keep working on the new code path |
| `benchmarks/source_profile/sample_org_strings.py` (`organisations`, `page_title`) | The rule "V2 column first, V1 column as fallback" and the title cleaning, so the gold sheet of phase P0 and silver use the same strings |
| `benchmarks/p0_memory_probe/docker_stats.py`, `clickhouse_aggregate.py` | The memory measurements of WP-10 |

---

## 2. Exit gate

Week 2 is done when every row is true. Rows 1 to 4 are the acceptance criteria of the blueprint for P1 (BP §11.2). Rows 5 to 12 are the deliverables of P1 and the open items which phase P0 handed to P1.

| # | Criterion | Evidence |
|---|---|---|
| 1 | Thirty consecutive closed UTC days of the three feeds (the window of §3.3) are in silver, loaded by the backfill of WP-09 | The ledger report for the window: every slot is `loaded` or `confirmed_missing`; four silver tables hold a day-level version for each of the 30 days |
| 2 | Completeness is at least 99.5% for each of the 30 days (definition in BP §8.2) | `ops.ops_slot_status_daily`, quoted in `docs/reports/p1-gdelt-production.md`, with the time of the evaluation |
| 3 | Accounting is 100%: for every loaded file, raw lines = parsed rows + quarantined rows (BP §3.4) | One query over the ledger for the window, quoted in the same report |
| 4 | A rerun gives equal checksums; unit-test coverage of the parsers and of the ledger is at least 80% | Drill DR8 on the fixtures in the workflow `e2e` and on two real days of the window; `make test` fails below 80% for the modules named in §3.3 |
| 5 | Gates G1, G2 and G3 are in force | Tests: a wrong MD5 never reaches bronze; a file with more than 1% bad rows is quarantined as a whole; a failed audit commits nothing. Rows in `ops.dq_result` |
| 6 | The DAGs `gdelt_ingest`, `gdelt_silver_hourly` and `gdelt_silver_daily` pass the DAG test suite, and the 30 days were built by `airflow backfill create` | `make test-dags` in the workflow `e2e`; the backfill identifier and the run list in the status report |
| 7 | `gdelt_ingest` and `gdelt_silver_hourly` ran on the live source for at least 6 hours in a row without manual action, and the live run held one scheduled run of `gdelt_silver_daily` (08:25 UTC) | Latency from `listed_at` to the silver commit for each slot of the live run, with median and 95th percentile, in the status report |
| 8 | Drills DR2 (silver part), DR4 and DR8 (silver part) pass as scripts; two runbooks exist | `make drill DR=2`, `DR=4`, `DR=8` in the workflow `e2e`; `docs/runbooks/` |
| 9 | Benchmark report v1 holds the first numbers of B1, B2 and B4, the memory of the real GKG parser and the source profile | `docs/reports/benchmark-v1.md` with scripts and raw results under `benchmarks/` |
| 10 | The quarantine report and the reconciliation report exist | `docs/reports/p1-quarantine-reconciliation.md` |
| 11 | `make demo` passes on the laptop (demo stack, §3.3) and in CI; the four workflows are green on `main` after the last merge | Local run log; `gh run list --branch main` |
| 12 | ADR-007 is written, the four silver tables have a contract file, and the version pins are re-verified on the start date | `docs/adr/007-…`, `docs/contracts/`, the pin table in the status report |

---

## 3. Scope and fixed decisions

### 3.1 In scope

| Task of the blueprint (BP §11.2, P1) | Work package |
|---|---|
| Ledger schema and migrations | WP-02 |
| Slot calendar; ledger state machine | WP-03 |
| Verified ingestion; master-list reconciliation; automatic missing-slot confirmation | WP-04 |
| Positional parsers; GKG derivations (title, story id, organisation explode, themes); quarantine | WP-05 |
| Pre-commit audit; partition versions | WP-06 |
| Hourly and daily silver DAGs | WP-07 |
| 30-day backfill through the backfill of Airflow | WP-09 |
| Source-profiling scripts; benchmarks B1, B2, B4 (first run) | WP-10 |
| Drills DR2 (silver), DR4, DR8 (silver); two runbooks | WP-11 |
| Quarantine and reconciliation reports; benchmark report v1 | WP-09, WP-10, WP-12 |

| Open item which phase P0 handed to P1 (status report of P0, "Open items") | Work package |
|---|---|
| The DAG test suite, which also guards spike answer B3 | WP-07 |
| The cost of an hourly overwrite inside a compacted day; no commit for a unit which stays empty; "no values" against "values missing" | WP-06 (rules), WP-10 (measurement) |
| The memory of the real GKG parser, and a silver build beside the hourly jobs | WP-10 |
| A named collection for the PostgreSQL connection, so no dbt model carries a password; no nullable arrays in the operational tables | WP-02 (tables), WP-08 (named collection) |
| Commit latency on S3, measured on the host | Moved to P2: the host exists from P2 (D26) |

### 3.2 Deferred

| Item | Phase |
|---|---|
| All SEC ingestion; the SEC fixtures and the five demo companies; `ops.source_object`, `ops.source_circuit` | P2 |
| The AWS host, the deployment, a pipeline run on S3 and Glue; commit latency on S3; the `prod` target of dbt | P2 |
| Entity resolution, the alias dictionary, news marts, dashboards, `ops.mart_watermark`; the formal drill DR1, whose assertion needs marts | P3 |
| Gates G4 to G6 with the repair queue; a rollback command for a silver table (D29); SLO computation; alerts by webhook and email; the heartbeat; drills DR3, DR6, DR7, DR9, DR10; clean-up of unreferenced files | P4 |
| The Spark job and the parity suite. The claim column `reserved_for` exists from P1, and the routine jobs already skip claimed slots | P5 |
| The translingual feed | Out of scope (BP §3.5) |

### 3.3 Decisions fixed for the week [R]

The blueprint leaves the following points open, and week 2 cannot start without an answer to each point.

| Topic | Decision | Reason |
|---|---|---|
| Where P1 runs | On the laptop, with the local backend (`./data/lake`, SQLite catalog, PostgreSQL and ClickHouse in Compose). No AWS resource changes | The host arrives in P2. Credentials from `aws login` last 15 minutes, less than a backfill takes |
| The 30 days | The 30 consecutive closed UTC days which end two days before the day on which WP-09 starts. The scheduled run of `gdelt_silver_daily` builds the day before the start day | F9: a backfill can build a day only after 08:25 UTC of the following day. A closed day no longer receives regular files (F1) |
| Migration tool | Plain SQL files in `db/postgres/migrations/` (`0001_….sql`, `0002_….sql`), applied in order by a small runner, `gbmi db migrate`. The runner records each file with a SHA-256 hash in `ops.schema_migration`, takes a PostgreSQL advisory lock, runs each file in one transaction and refuses a changed file. Forward only | BP §9.7 asks for additive, forward-only changes. The operational schema is hand-written SQL with check constraints and partial indexes. A runner of about 100 lines needs no second database library beside psycopg; Alembic would add SQLAlchemy as a second path to PostgreSQL |
| Start-up order of the stack | Two new one-shot Compose services run at every `make up`, in the order: `ops-migrate` (`gbmi db migrate` as the role `gbmi`; the migration files reach the development image through a read-only bind mount), then `postgres-roles` (idempotent SQL as the superuser: the read-only role of WP-08 with the grants on the schema `ops`). The Airflow services and the toolbox wait for both. No migration file names the read-only role | The same pattern as `clickhouse-init` of phase P0: a change reaches an existing volume without `make reset`. A migration which grants to a role would fail on every PostgreSQL without the role, for example in the workflow `ci` |
| Two stacks on the laptop | The *working stack* (Compose project `gbmi`, lake `data/lake`) holds real data. The *demo stack* (Compose project `gbmi-demo`, own volumes, lake `data/demo/lake`) runs `make demo` and the end-to-end tests. The Makefile variable `STACK` selects the project. Both stacks publish the same ports, so one stack runs at a time, and a target which starts one stack stops with a message while the other stack runs. CI has the demo stack only | The fixture hour 2026-11-25 08:00 lies inside the 30 days. The four GKG fixture files are shortened, so one shared ledger and lake would mix shortened files into the real day. The end-to-end test compares the whole month of the mart with the 4,023 rows of the fixture hour |
| Isolation of tests | No test, no drill and no scratch run of a benchmark writes to the database `gbmi` of a stack or to the lake outside the directory `_test/`. One exception exists: benchmark B2 rebuilds real days on the working stack with the normal operator command, and each rebuild adds a partition version with an equal checksum. One helper creates a throwaway database, applies the migrations and drops the database afterwards; the toolbox receives an administrator connection string for the helper. The helper is requested by name and never runs for every test, because the workflow `dbt` collects `tests/integration` without PostgreSQL | Every test and drill needs the same twelve fixture slots in a different starting state, and no transition leads back from `loaded` |
| Ledger states | The seven states and the twelve transitions of BP §8.2, unchanged. A successful download records two transitions in one transaction: `listed → downloaded` and `downloaded → verified`. A file reaches bronze only after gate G1, as in phase P0 | The state diagram and the stage list of BP §8.3 differ in the order of "store" and "verify"; one transaction keeps both true |
| Ledger columns beyond the illustrative DDL of BP §7.3 | `published_at` (upload time, from the HTTP header `Last-Modified`; NULL when bronze already held the file and no request was made), `absent_count` (reconciliations in a row which did not find the slot), `unavailable_since` (first HTTP 404 of a listed file), `raw_lines`, `parsed_rows`, `quarantined_rows` (the accounting of the file) | F1; the two rules of automatic confirmation need a counter and a time; the accounting of 100% becomes one query |
| Operational tables of P1 | `ops.source_slot`, `ops.slot_transition`, `ops.silver_partition`, `ops.pipeline_run`, `ops.dq_result`, `ops.schema_migration`. No column is an array; free detail is `jsonb` | ClickHouse cannot read a PostgreSQL relation with a nullable array column (P0 finding) |
| Time as an argument | Every function which depends on the current time takes the time as the argument `as_of`. The DAGs accept an optional parameter `as_of` for manual runs | Tests and drills control the clock; no test depends on the day of the run |
| Schedules (UTC) | `gdelt_ingest`: minutes 8, 23, 38, 53. `gdelt_silver_hourly`: minutes 11, 26, 41, 56. `gdelt_silver_daily`: 08:25 with a data interval of one day. The reconciliation is due once in each UTC day from 08:00; the task `reconcile` runs before the task `ingest`, so the run at 08:08 lists and lands what the reconciliation found, before the daily build at 08:25 | F1, F2, F9. A reconciliation which is due, not a second cron line, also runs when the laptop was off in the morning |
| Batch and concurrency | One ingest run downloads at most 200 files with 4 parallel downloads; a manual run can raise the limit (the backfill uses 1,500). One silver run builds at most 48 units. After the first HTTP 429 of a run, the run continues with one download at a time and honours `Retry-After` | BP §8.1 (bounded batches), BP §8.3 (concurrency 4 to 8; reduce on 429) |
| Which unit the silver jobs build | A *unit* is what one commit replaces: one slot-hour or one whole slot-date. A *pending slot* is a slot in state `verified` without a claim. `gdelt_silver_hourly` looks at each feed and date with pending slots in the trailing 7 days. When the tables of the feed hold no day-level version for the date, the job builds one hour unit for each hour with a pending slot. When a day-level version exists, the job rebuilds the whole day. `gdelt_silver_daily` always builds the whole day of the logical date | A finalised day is always the output of the day build, so the checksum of a closed day is deterministic and the day stays one file. SK1 showed the alternative: an hour overwrite inside a compacted day rewrites the day file (D17) |
| Empty unit | When a unit has no rows and the current snapshot holds no rows for the unit, the build makes no Iceberg commit and still records a partition version with 0 rows | SK1 finding: an empty input creates an empty snapshot on every run |
| Logical keys and duplicates | Events: `global_event_id`, first occurrence in slot and line order (as in P0). Mentions: (`global_event_id`, `mention_identifier`, `slot_ts`), the row with the highest `confidence`, then the first line. GKG documents: `gkg_record_id`, first occurrence. GKG organisations: (`gkg_record_id`, `org_string_id`), one row for each normalised string of a document | BP §7.4; F5 |
| Text normalisation, version 1 | One pure function in `gbmi.core.text`: Unicode NFKC, case-fold, remove diacritics, replace every character which is not a letter or a digit with one space, collapse spaces, strip. `org_string_id` and `story_id` are the first 16 hexadecimal characters of the SHA-256 hash of the normalised string. A document without a usable title takes the hash of `"url:"` plus the document URL. The split of a legal suffix (BP §8.8) is matching logic of P3 and works on the normalised string | BP §8.4: pure Python functions shared with Spark. Facts store the identifier and are never rewritten (BP §8.8), so the rule is fixed now and guarded by committed test vectors. 64 bits make a collision among one million strings unlikely (about 3 in 100 million) **[E]** |
| Lists in silver | `themes` and `countries` are never NULL: a document without values has an empty list. `themes` holds the distinct codes of `V1THEMES` in sorted order; `countries` holds the distinct two-letter codes of `V1LOCATIONS` in sorted order | ClickHouse reads a NULL list as an empty array (SK2 finding). An empty GKG field means "no values"; the source has no state "values missing". Sorted lists keep the checksum stable |
| Gate G2 | A row fails when the column count differs from 61, 16 or 27, when a key is missing, or when a typed field does not match the pattern of the type. A failed row goes to the quarantine file of the unit with the reason. A file with more than 1% failed rows is quarantined as a whole: the file is copied to `quarantine/gdelt/<feed>/dt=…/`, the `verified` slot moves to `quarantined`, and the file gives no row to silver | BP §5.7, §7.8 |
| Gate G3 | Before each commit: raw lines = parsed + quarantined; committed = parsed − duplicates; the key is unique and not NULL; every row belongs to the unit; lists are not NULL. For the organisation table the line identities do not apply: the row count equals the sum of `org_count` over the committed documents, and the key is unique. For a day unit also: the row count lies between 0.5 and 2.0 times the median of four row counts — the current day-level version of each of the four preceding dates with the same weekday; with fewer than four such dates the check is recorded as skipped. The operator can accept a day outside the band with a note (`--accept-volume`) | BP §5.7; F10 |
| Evidence of a build | Each partition version stores the hash of the file set (SHA-256 over the lines `<stamp>:<md5>` in slot order), the row counts and the content checksum of the unit | BP §8.4 (file-set hashes); drill DR8 compares the stored checksums |
| Failpoints | `gbmi.core.failpoint` stops the process with `SIGKILL` at a named point when the environment variable `GBMI_FAILPOINT` names the point. Three points exist: before the bronze move, before the Iceberg commit, between the Iceberg commit and the ledger update | Drill DR2 needs a crash at an exact place, not at a random moment |
| Alerts in P1 | A failed gate, a quarantined slot and an overdue slot write a log line of level error and a row in `ops.dq_result`. The webhook and the email follow in P4 | BP §11.2 gives alerting to P4 |
| Test levels | `tests/unit` (pure code, hypothesis), `tests/ledger` (needs PostgreSQL), `tests/transforms` (DuckDB on fixture files), `tests/contract` (schemas against the recorded slots and the contract files), `tests/integration` (toolbox: lake, ledger and builds together), `tests/dags` (inside the image), `tests/e2e`, `drills/`. `make test` runs the first four levels and needs a reachable PostgreSQL; the workflow `ci` gets a PostgreSQL service container | BP §9.6 |
| Coverage gate | `make test` fails when the coverage of `gbmi.ledger`, `gbmi.core` and `gbmi.transforms.duckdb` together is below 80% | Acceptance criterion of P1; the three packages are "parsers and ledger" |
| DAG tests | `tests/dags` uses the standard library module `unittest` and runs with the interpreter of Airflow inside the image (`make test-dags`) | F13: the Airflow environment has no pytest, and no package is added to the Airflow environment without need (P0 rule) |
| GKG fixtures | The four Mentions slots of the fixture hour are committed unchanged (288,245 bytes). Each of the four GKG fixture files holds the first 60 records of the real slot, zipped again; the fixture index gives the size and the MD5 of the shortened files. The attribution file names the difference | The four real GKG files have 14.5 MB; 60 records of slot 08:45 have 211,366 bytes zipped **[V]** |
| Numbering | Differences from the blueprint continue the numbering of the week 1 plan and start at D15 | References such as "D13" stay unambiguous across both plans |
| Pull requests | One short-lived branch and one pull request for each work package, merged with a merge commit after the four checks | Rule of phase P0; the reports quote commit ids |

### 3.4 Differences from the blueprint

| # | Blueprint | Plan for week 2 | Why |
|---|---|---|---|
| D15 | §8.1: `gdelt_ingest` runs at minutes 3, 18, 33, 48 | Minutes 8, 23, 38, 53 | F1: a file is uploaded about 10 minutes before the stamp. The blueprint's minutes poll about 2 minutes before the next upload |
| D16 | §8.1: `gdelt_silver_hourly` runs hourly at minute 20 | Every 15 minutes, 3 minutes after each ingest run. The unit stays one slot-hour, so an hour is built up to four times | F2. The extra builds parse at most 10 files in place of 4 for each hour; WP-10 reports the cost from the live run |
| D17 | §8.4: the hourly job overwrites single hours in the trailing 7 days | The hourly job overwrites single hours only while the day has no day-level version; afterwards a change rebuilds the whole day | Decision in §3.3. WP-10 measures both ways, unless cut 3 of §10 applies |
| D18 | §8.1: the master-list reconciliation runs daily at 08:10 | The reconciliation runs once in each UTC day, inside the first ingest run at or after 08:00 UTC and before the download of the run | Level-triggered (BP §5.1, principle 3): a laptop or a host which was off in the morning still reconciles. The order keeps the reconciliation ahead of the daily build at 08:25 |
| D19 | §7.4: `gdelt_mention` is deduplicated on the logical key | The same key; the surviving row is the row with the highest confidence. The dropped rows are not exact copies | F5 |
| D20 | §7.4 names one organisation string for each mention | The string comes from `V2ENHANCEDORGANIZATIONS`; a record which fills only `V1ORGANIZATIONS` uses the V1 names with `first_offset` NULL and `mention_count` 1 | F6; the same rule drew the gold sheet of phase P0 |
| D21 | §8.4: backfill "two or three dates at a time" | One date at a time (`--max-active-runs 1`) | The pool `silver_writer` has one slot (BP §5.5), so two dates cannot write at the same time |
| D22 | §8.4: data reaches the Iceberg writer in record batches | A day is staged hour by hour into one Parquet file for each table and added with `add_files`; one pass over the GKG files feeds two tables | SK1, decision 3 |
| D23 | §8.3: download "to a temporary file" | A file is held in memory and written to bronze through a `.part` name, as in phase P0 | The largest files are a few megabytes (F4: a GKG day has at most 541 MB in 96 files) |
| D24 | §9.6: DAG tests run "every push" with pytest | The DAG tests run in the workflow `e2e`, inside the image, with `unittest` | F13. The workflow `e2e` runs on every pull request and on every push to `main` |
| D25 | §16.7 and the fixtures rule of §10: recorded slots | The GKG fixture files are shortened to 60 records each | Size (§3.3) |
| D26 | The P0 status report gives "commit latency on S3, measured on the host" to P1 | Moved to P2 | The host does not exist in P1 |
| D27 | §9.8: drill DR1 (30-day backfill) asserts on marts | P1 runs the backfill and reports completeness and accounting; the formal drill DR1 with the mart assertion is P3 work | The news marts are P3 work |
| D28 | §5.10, §8.1: failures alert a webhook and an email | P1 writes log lines and rows in `ops.dq_result` | Alerting is P4 work (BP §11.2) |
| D29 | §8.4: a bad publication is reverted by a rollback to the previous snapshot (runbook) | The runbook of P1 repairs a bad unit by a rebuild from bronze. A rollback command follows in P4 | After a rollback the slots stay `loaded` while silver no longer holds the rows, and no transition leads out of `loaded`. A rollback needs the repair queue of gate G6, which rebuilds the units of the removed commit |

### 3.5 Changes after approval

Jay approved the plan on 2026-11-26 and started week 2 on the same day (action J1). Claude, the AI model which works in the repository, carries out the week. Nine changes to the plan were fixed on 2026-11-26, before the first work package started, and carry the numbers C1 to C9. A change enters the table with the pull request of the work package which applies the change.

| # | Date | Change | Why |
|---|---|---|---|
| J1, J2 | 2026-11-26 | Jay gave the go-ahead J1 for all Git work of the week. For J2 Jay decided: WP-09 may start at any hour | Decisions of Jay |
| C1 | 2026-11-26 | **Order.** WP-11 (drills and runbooks) runs before WP-09. The order of the week is WP-01 to WP-08, WP-11, WP-09, WP-10, WP-12. The table of §6 keeps the original numbers | WP-11 needs only WP-07 (§6). The drills and `make demo` need the demo stack, the live run of WP-09 holds the working stack for up to 26 hours **[E]**, and both stacks publish the same ports (§3.3). The three drills test a crash, a corrupt file and a rerun on the fixtures: drills which pass before the backfill lower the risk of the 30-day run. The runbook which WP-09 names exists before the backfill starts |
| C5 | 2026-11-26 | **Action J3 is not taken.** Docker Desktop stays at 7.65 GiB, and the reports name the 7.65 GiB machine. A change to 9 GB has to come before WP-09 starts, not "before WP-10" | The AWS host of phase P2 has 8 GB (BP §9.1), so the 7.65 GiB machine is the closer test. Benchmark B2 reads the durations of the backfill of WP-09, so a change between WP-09 and WP-10 would mix two machine sizes |
| C8 | 2026-11-26 | **Stretch items of §6.** One stretch item is taken when the recorded working time at the end of WP-10 is below the planned hours: the events mart over the 30 days. The other three stretch items are not taken in week 2 | The AWS half needs a sign-in of Jay (J4). The `plan` job in CI is work of P2. A Dependabot configuration opens pull requests on a schedule of GitHub, and Jay has not asked for the configuration |
| C9 | 2026-11-26 | **Hours.** The hours of §6 are planned hours of a person. Each pull request records the start and the end of the work package in UTC. The working time of a work package excludes the waits for the GitHub checks, for the backfill and for the live run. The 150% rule of §6 and the signal of §10 ("more than 37 hours used before WP-08 starts") apply to the recorded working time | The rules of §6 and §10 need one measured number for each work package |

---

## 4. Verified starting state

### 4.1 Repository, stack and laptop [V]

| Item | State on 2026-11-26 |
|---|---|
| Repository | `/Users/jaymercado/Jay/Projects/gbmi-platform`; `main` = `origin/main` = `b0e91ef`; clean working tree; annotated tag `p0` on `989ccfb` |
| GitHub | Pull requests #1, #2 and #3 are merged. The four workflows passed on `main` at `b0e91ef`: `ci` 30 s, `terraform` 57 s, `dbt` 3 min 17 s, `e2e` 8 min 59 s |
| Tests | `tests/unit` and `tests/transforms` hold 95 tests |
| Local stack | Compose project `gbmi`: 7 containers, all healthy — PostgreSQL 17.12, ClickHouse 26.8.46.6, four Airflow 3.4.0 processes, Metabase |
| PostgreSQL database `gbmi` | Only the schema `public`; no table. Roles: `postgres`, `airflow`, `gbmi`, `metabase` |
| Lake of the laptop | `data/` holds 1.5 MB: the demo hour of phase P0 |
| Docker Desktop | Engine 28.2.2, Compose v2.36.2; the virtual machine has 8,217,751,552 bytes (7.65 GiB) and 12 CPUs. BP §9.1 asks for 9 GB |
| Other containers | Eight Supabase containers of another project run in the same Docker Desktop. Week 2 does not touch the Supabase containers |
| Disk | 456 GB free |
| Tools | uv 0.13.10, Terraform 1.17.2, AWS CLI 2.42.3, gh 2.86.0, GNU Make 3.81 |
| AWS | Not read for the plan. Week 2 changes no AWS resource |

### 4.2 Version pins [V]

| Component | Pin of phase P0 | Found on 2026-11-26 |
|---|---|---|
| Apache Airflow; astronomer-cosmos | 3.4.0; 1.16.0 | Both are still the newest release |
| DuckDB | 2.0.1 | 2.0.1 |
| PyIceberg; PyArrow; SQLAlchemy | 0.12.0; 26.0.1; 2.1.9 | No newer release |
| dbt Core; dbt-clickhouse; sqlfluff | 1.12.7; 1.10.4; 4.5.0 | No newer release |
| ClickHouse; PostgreSQL | 26.8.46.6; 17.12 | The running containers report the pinned versions. The image tags were not compared with Docker Hub for the plan; WP-01 compares the tags |
| psycopg; clickhouse-connect | 3.3.9; 1.13.0 | No newer release |
| pydantic; pydantic-settings; httpx; tenacity; structlog | 2.14.4; 2.16.3; 0.28.1; 9.2.1; 26.1.0 | No newer release |
| typer | 0.29.2 | **0.29.3** (2026-11-26): a patch release; WP-01 takes the release |
| uv; ruff; mypy; pytest; pytest-cov; pre-commit | 0.13.10; 0.17.6; 2.4.1; 9.2.0; 7.1.0; 4.7.0 | No newer release |
| hypothesis | Not installed | 6.170.17 (2026-11-20); new in the development group |
| Inside the Airflow image | — | apache-airflow-task-sdk 1.4.0, providers standard 1.21.0 and fab 3.10.2; no pytest |

Metabase, Terraform and the GitHub Actions were not re-checked for the plan, because week 2 does not change the three components. WP-01 re-checks every pin on the start date (BP §16.7, step 11).

---

## 5. Actions only Jay can take

| # | Action | When | Check |
|---|---|---|---|
| J1 | Start week 2, and give one go-ahead for the Git work of the week: commits on work-package branches, pushes, pull requests, merges with a merge commit, and the tag `p1` | Before WP-01 | — |
| J2 | Keep the laptop awake, on power and online during the backfill and the live run of WP-09. The live run has to hold 08:25 UTC (16:25 on the laptop clock): about 8 hours in total when WP-09 starts before 10:00 on the laptop clock, and up to 26 hours otherwise **[E]** | WP-09 | — |
| J3 | Optional: raise the memory of Docker Desktop to 9 GB (BP §9.1). The change restarts Docker Desktop and every running container, the Supabase containers included. Without the change, the memory report names the 7.65 GiB machine again | Before WP-10 | `docker info --format '{{.MemTotal}}'` prints a number above `8217751552` |
| J4 | Optional, only for the stretch item "AWS half" (§6): `aws login --remote --profile gbmi-login` | Stretch item | `aws sts get-caller-identity --profile gbmi --query Arn --output text` prints `arn:aws:iam::<account>:root` |

Everything else in the plan can be executed without Jay. Three older points stay with Jay and do not block week 2: the status of the alert email address in the Billing console (setup guide, step 4.1), the review of the gold-sheet labels by a person (before the precision gate of P3), and the contact address for the SEC user agent (before P2).

---

## 6. Execution sequence

| Order | Work package | Hours | Needs | Produces |
|---|---|---|---|---|
| 1 | WP-01 Start: checks, pins, plan in the repository | 0.5 | J1 | Baseline: `make demo` passes; hypothesis installed |
| 2 | WP-02 Operational database: migrations, the schema `ops`, two stacks | 4 | WP-01 | Migration runner; five tables; the demo stack; the throwaway database for tests; PostgreSQL in the workflow `ci` |
| 3 | WP-03 Ledger: state machine, calendar, operator commands | 5 | WP-02 | `gbmi.ledger`; property tests |
| 4 | WP-04 Ingestion: fixtures, index, verified download, gate G1, reconciliation | 7 | WP-03 | Fixtures of three feeds; pipeline PL-1 as functions and commands |
| 5 | WP-05 Parsers: Mentions and GKG, text functions, gate G2, contracts | 7 | The fixtures of WP-04 | Four typed tables from fixture files |
| 6 | WP-06 Silver build: units, gate G3, partition versions | 5.5 | WP-04, WP-05 | Pipeline PL-2 as functions and commands |
| 7 | WP-07 DAGs, DAG tests, tracer on the new path | 4.5 | WP-06 | Three DAGs; `make demo` on three feeds |
| 8 | WP-08 Operational models in dbt | 1.5 | WP-06 | Named collection; `ops_slot_status_daily` |
| 9 | WP-09 Thirty-day backfill and live run | 2.5 | WP-07, WP-08, J2 | Thirty days of silver; two reports |
| 10 | WP-10 Source profile and benchmarks B1, B2, B4 | 3.5 | WP-09 | Benchmark report v1 |
| 11 | WP-11 Drills DR2, DR4, DR8 and two runbooks | 2 | WP-07 | Three drill scripts in CI; two runbooks |
| 12 | WP-12 ADR-007, status report, close-out | 2 | All | Decision record, status report, tag `p1` |
| | **Total** | **45** | | About 68 with contingency |

Rules for the sequence:

- The order is the plan. WP-05 needs only the fixtures which WP-04 records first, and may start as soon as the fixtures exist.
- Every work package ends with `make lint` and `make test`, one pull request, the four checks and a merge with a merge commit.
- A work package which has used 150% of the planned hours stops, and the cut order of §10 applies. The review of the draft named WP-06 and WP-10 as the two packages with the tightest hours; each of the two packages names the part which drops first.
- The backfill of WP-09 starts only when `make demo` and the workflow `e2e` pass on the code of WP-07.

**Stretch items** are outside the 45 hours and are taken only when the week is ahead:

| Stretch item | Hours |
|---|---|
| AWS half: the build tests of WP-06 on Glue and S3 in the namespace `gbmi_ci_silver`, through `make integration-aws` (needs J4) | 1 |
| The events mart of phase P0 built over the 30 days (the month partitions of the window), for the mart size of benchmark B4 | 0.5 |
| A `plan` job in CI through OIDC as `gbmi-ci-plan`: the first real test of the OIDC subject (open item of P0, planned for P2) | 1 |
| Dependabot configuration and a `security` workflow beyond the secret scan (BP §9.7) | 0.5 |

---

## 7. Work packages

### WP-01 — Start: checks, pins, plan in the repository (0.5 h)

**Goal.** A verified starting point and a green baseline.

1. Run the first checks of the handoff file, section 10.1: `git status -sb`, `git log --oneline -n 3`, `git tag -n1`, `gh pr list --state all -L 5`, `gh run list --branch main -L 4`, `docker ps`.
2. Re-check §4.2 against PyPI, Docker Hub and GitHub releases. Rule of week 1: take a newer patch release of the same minor version; any other change stays on the pin and is noted for a decision record (BP §6.1, risk R7).
3. On the branch `p1-01-start`: copy the plan into the repository as `WEEK_2_IMPLEMENTATION_PLAN.md`; add `hypothesis>=6.170,<7` to the development group; take typer 0.29.3; run `uv lock`. Correct the index in `docs/adr/README.md`: the record ADR-004 says "Accepted" and the index says "Provisional" **[V]**.
4. `make up` and `make demo` as the baseline.

**Done when.** `make lint`, `make test` and `make demo` pass on the branch, and the pull request is merged.

### WP-02 — Operational database: migrations, the schema `ops`, two stacks (4 h)

**Goal.** The schema `ops` exists through migrations on every start of the stack, on the laptop and in CI (BP §7.3).

**Produces.** `src/gbmi/ledger/db.py` (one connection helper on `GBMI_PG_DSN`, psycopg 3), `src/gbmi/ledger/migrate.py`, the commands `gbmi db migrate` and `gbmi db status`, three migration files, the Compose service `ops-migrate`, the demo stack, the helper for a throwaway database, `tests/ledger/`.

1. **Runner** with the rules of §3.3. Tests on a throwaway database which the test session creates and drops: a fresh database receives every file; a second run changes nothing; a changed file is refused; a failing file leaves no trace; two runners at the same time apply each file once.
2. **`0001_ledger.sql`.** `ops.source_slot`: the illustrative DDL of BP §7.3 plus the columns of §3.3; primary key (`source`, `feed`, `slot_ts`); a check constraint on the seven states and one on the quarter hour of `slot_ts`; the partial index on pending states. `ops.slot_transition`: one row for each state change, append-only (`source`, `feed`, `slot_ts`, `from_state`, `to_state`, `reason`, `run_id`, `decided_by`, `created_at`).
3. **`0002_silver_partition.sql`.** `ops.silver_partition`: one row for each build of a unit and table (BP §7.3: table × partition × snapshot): `table_name`, `slot_date`, `unit_kind` (`hour` or `day`), `slot_hour`, `commit_id`, `snapshot_id`, `previous_snapshot_id`, the five counts of the audit, `file_count`, `fileset_hash`, `content_digest`, `run_id`, `pipeline_version`, `duration_ms`, `committed_at`. The newest row for a table and a date is the current version.
4. **`0003_runs_and_quality.sql`.** `ops.pipeline_run` (one row for each task execution: DAG, task, run id, try number, start, end, status, rows, bytes, code version, detail as `jsonb`) and `ops.dq_result` (one row for each check execution: gate, check, status, observed value, threshold, feed or table, unit, run id) — BP §7.3.
5. **Stack.** The one-shot service `ops-migrate` runs `gbmi db migrate` after PostgreSQL is healthy; the Airflow services and the toolbox wait for the service. The development image reads the migration files through a read-only bind mount of `./db`. The runtime image receives a copy (Dockerfile and `.dockerignore`), because the deploy workflow of P2 runs the migrations from the image (BP §9.7). `make smoke` gains one check: the newest migration file is applied.
6. **Two stacks** (§3.3). The Makefile passes `-p $(STACK)` to Compose and mounts the lake directory of the stack. `make demo` and `make e2e` always use the demo stack. `make reset` removes only the volumes and the lake directory of the named stack; today the target removes the whole directory `data/` **[V]**. `compose/compose.yaml` holds the line `name: gbmi` **[V]**: the first check of the step shows whether the option `-p` wins over the line; when the line wins, the line is removed.
7. **Throwaway database** (§3.3). One helper creates a database, applies the migrations and drops the database. `tests/ledger`, the build tests of `tests/integration`, the drills and the scratch runs of the benchmarks use the helper. The toolbox service receives the administrator connection string `GBMI_TEST_PG_ADMIN_DSN`.
8. **CI.** The workflow `ci` gets a `postgres:17` service container, pinned by the digest of `compose/compose.yaml`, and sets `GBMI_TEST_PG_ADMIN_DSN`. On the laptop the Makefile builds the value from `.env` for the PostgreSQL of the running stack. The workflow `e2e` sets `STACK=gbmi-demo`.

**Done when.** On the existing volumes of the working stack, `make up` creates the schema without `make reset`; `gbmi db status` lists three applied files; `make demo` passes on the demo stack and leaves the working stack unchanged; the workflow `ci` passes with the service container.

### WP-03 — Ledger: state machine, calendar, operator commands (5 h)

**Goal.** The ledger of BP §8.2 as tested code: no slot can reach a state by a path which the state diagram does not allow.

**Produces.** `src/gbmi/ledger/states.py`, `calendar.py`, `repository.py`; the command group `gbmi ledger`; unit tests and PostgreSQL tests.

1. **`states.py`, pure code.** The seven states and the twelve transitions of BP §8.2 as one table. Property tests with hypothesis: for any sequence of requested moves, only moves of the table succeed; no move leaves `loaded`; the only move out of `confirmed_missing` leads to `listed`.
2. **`calendar.py`, pure code.** The expected slots of a time range: 96 for each day and feed. Tests for the end of a month, 29 February, the turn of the year and the two dates on which clocks change in the United States (risk R26): each day has 96 slots, because every time is UTC (BP §7.11).
3. **`repository.py`.** Every transition is one guarded update (`UPDATE … WHERE state = <from>`) and one insert into `ops.slot_transition`, in one transaction (BP §8.2). A guarded update which changes no row raises `StaleStateError`. Functions: `ensure_expected` (set-based insert with `ON CONFLICT DO NOTHING`), `mark_listed`, `record_download`, `record_failed_attempt`, `quarantine`, `mark_loaded`, `confirm_missing`, `requeue`, `reopen`, `pending_downloads`, `pending_units`, `overdue`, `status`, `completeness`.
4. **Property test against PostgreSQL.** hypothesis generates sequences of repository calls. Invariants: the state of each slot equals the replay of the slot's rows in `ops.slot_transition`; no stored transition is outside the table; when two connections move the same slot, exactly one connection wins.
5. **Operator commands** (BP §8.2): `gbmi ledger status --from --to`, `gbmi ledger overdue`, `gbmi ledger completeness --from --to`, and the two commands which change states and require a range and a note: `gbmi ledger confirm-missing` and `gbmi ledger requeue`.

**Done when.** The tests pass and the coverage of `gbmi.ledger` is at least 80%.

### WP-04 — Ingestion: fixtures, index, verified download, gate G1, reconciliation (7 h)

**Goal.** Pipeline PL-1 of BP §8.3: every GDELT file lands unchanged, verified and tracked, and a missing file is noticed.

**Produces.** `src/gbmi/sources/gdelt/ingest.py` and `reconcile.py`; extensions of `index.py` and `land.py`; `src/gbmi/ops/runs.py`; the commands `gbmi gdelt ingest`, `gbmi gdelt enqueue` and `gbmi gdelt reconcile`; the fixtures of Mentions and GKG; tests.

**First: the fixtures.** `benchmarks/source_profile/record_gdelt_fixtures.py` gains the options `--feeds` and `--gkg-records`. The fixture hour 2026-11-25 08:00 to 08:45 UTC gains four Mentions files, four shortened GKG files, eight index lines and a `lastupdate.txt` (§3.3). `tests/fixtures/gdelt/ATTRIBUTION.md` describes each file and the shortening. WP-05 uses the same files.

1. **Index readers.** `lastupdate.txt` gives the three lines of the newest slot. The master-list reader takes a tail sized from the oldest wanted slot: 320 bytes for each slot (F3), times 1.5, at least 2,000,000 bytes. When the oldest slot of the tail is younger than the oldest wanted slot, the reader doubles the tail and reads again. `HttpFetcher.fetch` also returns the time of the header `Last-Modified`. `FixtureFetcher` serves a recorded `lastupdate.txt`.
2. **One ingest run** (`run_ingest(as_of, source, batch_limit, concurrency)`), the seven stages of BP §8.3:
   - Calendar: insert `expected` rows from the newest slot of the ledger to `as_of` plus 15 minutes (F1). An empty ledger starts at the newest slot of `lastupdate.txt`.
   - Index: mark the slots of `lastupdate.txt` as `listed` with URL, size and MD5. When the newest slot of `lastupdate.txt` is more than 15 minutes ahead of the newest listed slot of the ledger, the run follows a stop or a missed run: read the master-list tail and mark every slot found. A normal run costs one request of 320 bytes. A single slot which the source skipped is found by the daily reconciliation.
   - Select the `listed` slots, oldest first, up to the batch limit.
   - Download with 4 threads through `land_file`: fetch, gate G1 (size, MD5, the zip opens), write to bronze under the deterministic key. An object which bronze already holds with the expected MD5 is not fetched again. `land_file` is extended: the result carries the upload time, and a file which fails gate G1 is returned with the bytes.
   - Record `listed → downloaded → verified` in one transaction for each slot, with `bronze_uri`, `verified_at` and `published_at`.
   - Gate G1 fails: record `listed → downloaded` and `downloaded → listed` in one transaction, and add 1 to `attempts`. At the third failure, write the bad bytes to `quarantine/gdelt/<feed>/dt=…/` and record `listed → quarantined` (BP §8.3).
   - HTTP 404 for a listed file: the slot stays `listed`, and the first 404 sets `unavailable_since`. HTTP 429: the rule of §3.3.
3. **`gbmi gdelt enqueue --from --to [--reserve CLAIM]`** creates the calendar rows of a range and marks every slot of the master list as `listed` (BP §8.3, backfill). The same ingest run drains the rows.
4. **Reconciliation** over the trailing 7 days (BP §8.2, §8.3): read the master-list tail; move found `expected` slots to `listed`; add 1 to `absent_count` for each `expected` slot which the list does not hold, and set the counter to 0 for a found slot. Confirm automatically, with `decided_by = 'auto'`: an `expected` slot with `absent_count` of 2 or more and a slot time at least 48 hours old, and a `listed` slot with `unavailable_since` at least 48 hours old. Reopen a slot (`confirmed_missing → listed`) only in two cases: the slot was confirmed from `expected` and the list now holds the slot; or the list gives a size or an MD5 which differs from the stored values. A slot confirmed from `listed` or from `quarantined` stands in the list already and stays confirmed. Take a changed size or MD5 of a slot which is not yet verified from the list. Compare the bronze listing with the ledger for verified and loaded slots and report each difference; the repair queue is P4 work. Report every slot which is not `verified` six hours after the slot time.
5. **Run records.** `gbmi.ops.runs.record_run` writes one row to `ops.pipeline_run` for each task entry point (BP §8.1). Every later entry point uses the same function.
6. **Tests.** Unit tests with `httpx.MockTransport`, the pattern of `tests/unit/test_gdelt_land.py`: a normal run; a run after a gap; a wrong MD5 three times; a 404 and the 48 hours with a controlled `as_of`; a 429; a crash after the bronze write and a rerun which completes the transition without a download; a list in which one slot is absent twice; a reopened slot; a second reconciliation after a confirmation changes nothing. One integration test in the toolbox on the fixture hour: twelve slots end `verified`, and a second run writes no file.

**Done when.** One live run in the toolbox, with a throwaway database and a scratch lake, lands the newest slot of each feed; a second run downloads nothing; `gbmi ledger status` agrees with the bronze listing.

**When the work fails.** When the GDELT servers answer 429 again and again, the run continues with one download at a time and a pause of 60 seconds, and the status report records the limit (BP §17.3, assumption 2).

### WP-05 — Parsers: Mentions and GKG, text functions, gate G2, contracts (7 h)

**Goal.** Verified files of the three feeds become four typed tables in memory, with gate G2 and the derivations of BP §8.4.

**Produces.** `src/gbmi/core/positional.py`, `gdelt_mentions.py`, `gdelt_gkg.py`, `text.py`; `src/gbmi/transforms/duckdb/positional.py`, `gdelt_mention.py`, `gdelt_gkg.py`; three new schemas in `src/gbmi/lake/tables.py`; `docs/contracts/`; tests.

1. **Fixtures.** The files which WP-04 recorded; WP-05 records no file.
2. **Shared positional code.** `Kind` and `SilverColumn` move from `gdelt_export.py` to `positional.py`; the SQL generators `check_sql` and `typed_sql` take a feed definition. The Events parser gives the same output as before; the existing tests prove the equality.
3. **Mentions.** The 16 columns in the order of the GDELT codebook. `gdelt_mention` carries the key of §3.3, `slot_date`, `slot_hour`, the columns of BP §7.4 (`event_time_date`, `mention_type`, `mention_source_name`, `mention_identifier`, `confidence`, `mention_doc_tone`) and the provenance columns of BP §7.12.
4. **GKG.** The 27 columns in the order which `benchmarks/p0_memory_probe/gkg_day_probe.py` already lists. Typed fields which can fail gate G2: `GKGRECORDID` (pattern `<14 digits>-<number>`, with an optional `T`), `V2.1DATE` (14 digits, a valid time), `V2SOURCECOLLECTIONIDENTIFIER` (integer), `V2DOCUMENTIDENTIFIER` (not empty), `V1.5TONE` (exactly seven numbers: tone, positive score, negative score, polarity, activity density, self-reference density, word count).
   - `gdelt_gkg_doc`: the columns of BP §7.4. `page_title` comes from the tag `<PAGE_TITLE>` of the extras column; `themes` and `countries` follow §3.3; the country code is the fifth field from the right end of a location block (F6); `is_translated` is true when the record identifier holds `-T`.
   - `gdelt_gkg_org`: one row for each document and normalised organisation string, with `org_string_raw` (the form with the smallest offset), `org_string_norm`, `org_string_id`, `first_offset` and `mention_count` (D20). A name whose normalised form is empty is dropped and counted.
   - `V2GCAM` and the image, video and quotation columns stay in bronze only (BP §7.4).
5. **Text functions** in `gbmi.core.text`: `clean_title` (XML character references, white space), `normalise_text`, `story_id`, `org_string_id`, and the constant `TEXT_NORMALISATION_VERSION = 1` (§3.3). DuckDB selects the distinct raw strings; the Python functions map each distinct string once; DuckDB joins the result back. Tests: properties with hypothesis (a second normalisation changes nothing; the output holds only letters, digits and single spaces) and a committed list of input and output pairs, so a later change of the rule breaks a test.
6. **Gate G2** as in §3.3: the reason of each failed row; the 1% rule for a whole file as one pure function from the counts of a file to a decision.
7. **Contracts.** One YAML file for each silver table in `docs/contracts/`: column, type, required or optional, meaning, key, partition. `tests/contract/` compares each file with the schema in `gbmi.lake.tables`, and checks the recorded fixture files against the column counts 61, 16 and 27 (BP §9.6).
8. **Tests** in `tests/transforms`: for each feed a small synthetic file with one good row and one row for each failure reason; the fixture hour parses completely, and the row counts equal counts computed with separate code; repeated Mentions pairs keep the row with the highest confidence (F5); a title with character references; a location block with an extra `#`; a record with organisations only in the V1 column; output in UTC with exactly the silver schema.

**Done when.** The fixture hour of the three feeds parses into the four tables with the accounting identities of BP §5.7, and the coverage of `gbmi.core` and `gbmi.transforms.duckdb` is at least 80%.

**When the work fails.** When PyIceberg refuses a list column declared as required, the list columns stay optional in the Iceberg schema and gate G3 enforces "not NULL".

### WP-06 — Silver build: units, gate G3, partition versions (5.5 h)

**Goal.** Pipeline PL-2 of BP §8.4 as one function which builds one unit, with no dependency on Airflow.

**Produces.** `src/gbmi/transforms/build.py`, outside the package of the coverage gate, because the builder needs the lake and PostgreSQL; gate G3 at full strength in `src/gbmi/quality/audit.py`; `src/gbmi/quality/results.py`; the split of `overwrite_day_streaming` in `src/gbmi/lake/commit.py`; `src/gbmi/core/failpoint.py`; the commands `gbmi silver units`, `gbmi silver build-hour` and `gbmi silver build-day`; integration tests.

1. **`build_unit(feed, unit, as_of, run_id)`**, the eight stages of BP §8.4:
   - File set: the slots of the unit in state `verified` or `loaded` without a claim. Each file is read from bronze and the MD5 is compared with the ledger. A `verified` slot whose file has a different MD5 is left out and moves to `quarantined`. The state diagram has no move out of `loaded`: a `loaded` slot whose file has a different MD5, or whose file now fails the 1% rule, stops the build with an error, and nothing is committed.
   - Parse, gate G2, deduplicate (WP-05); write the quarantine file of the unit with `write_quarantine`.
   - Gate G3 (§3.3). A failed audit raises, commits nothing and leaves the ledger unchanged.
   - Commit to Iceberg; for GKG the document table first, then the organisation table.
   - One PostgreSQL transaction: one row in `ops.silver_partition` for each table, the three accounting columns of each slot, `verified → loaded` for the pending slots, and the rows of `ops.dq_result`.
2. **Hour unit.** In memory, through `overwrite_unit`, as in phase P0.
3. **Day unit.** `overwrite_day_streaming` is split into "stage a day file" and "commit a day file"; the existing function stays as a wrapper, and the SK1 tests keep passing. The day is parsed hour by hour, so memory follows one hour; a DuckDB table of seen keys removes duplicates across hours; one pass over the GKG files stages the files of both GKG tables.
4. **Rules of §3.3:** the empty unit; the choice between hour units and a day unit in `pending_units`; a day whose stored file-set hash differs from the hash of the loaded files counts as pending.
5. **Commit conflicts.** A `CommitFailedException` of PyIceberg is retried up to three times after a refresh of the table (BP §8.4).
6. **Failpoints** at the three places of §3.3.
7. **Tests** in the toolbox, on the fixture hour, with the local lake and the Compose PostgreSQL: two builds of an hour give the same checksum and a new snapshot; a day build after hour builds gives one file for each table and the same content; a slot which arrives after the day build causes a rebuild of the day; a failed audit changes neither silver nor the ledger; a process killed at each failpoint is repaired by the next run; a file above the 1% threshold is quarantined and the unit is built from the other files; the volume band of gate G3 with a prepared history; the operator confirms a slot which the 1% rule quarantined (`quarantined → confirmed_missing`), and the day counts as complete.

**Done when.** `gbmi silver build-day` builds the fixture day for the three feeds; the twelve slots are `loaded`; the accounting query returns twelve of twelve files.

**When the work fails.** The part which drops first is the single pass over the GKG files: when the split of `overwrite_day_streaming` takes longer than one hour, the GKG day is parsed twice, once for each table, and benchmark B2 reports the cost.

### WP-07 — DAGs, DAG tests, tracer on the new path (4.5 h)

**Goal.** The three GDELT DAGs of BP §8.1 exist as wiring, the tracer uses the new code, and a test suite guards every DAG.

**Produces.** `dags/gdelt_ingest.py`, `dags/gdelt_silver_hourly.py`, `dags/gdelt_silver_daily.py`; a changed `dags/tracer_gdelt_events.py`; `tests/dags/`; the target `make test-dags`; an extended `tests/e2e/test_demo.py`.

| DAG | Schedule (UTC) | Tasks | Notes |
|---|---|---|---|
| `gdelt_ingest` | `8,23,38,53 * * * *` | `reconcile` (does the work only when due, D18) → `ingest` → `report_overdue` | Parameters `source`, `batch_limit`, `as_of` |
| `gdelt_silver_hourly` | `11,26,41,56 * * * *` | `find_units` → `build_units` (pool `silver_writer`) | At most 48 units for each run |
| `gdelt_silver_daily` | `CronTriggerTimetable("25 8 * * *", timezone="UTC", interval=timedelta(days=1))` | `build_day` for Events, Mentions and GKG, one after the other (pool `silver_writer`) → `summary` | The date of the logical date is the day to build (F9); the target of `airflow backfill create` |

1. Every DAG file holds wiring only (BP §10). A task calls one function of `gbmi` through `@task.external_python` and returns built-in types, as the tracer does. Every DAG has `max_active_runs=1`, `catchup=False`, the default arguments of `dags/common` and an execution timeout of three times the expected duration (BP §8.1). New DAGs start paused, as the local stack sets.
2. **Tracer.** The tasks `land` and `build_silver` of `tracer_gdelt_events` call the ledger-driven functions for the hour of the parameter and for the three feeds. The tasks `pin_lake`, the dbt group and `assert_counts` stay. The entry points of phase P0 which the new functions replace (`land_export_hour`, `build_event_hour`) are removed, with the two old commands.
3. **`tests/dags`** (`unittest`, inside the image): every DAG file imports without an error; no subprocess starts during the import, which guards spike answer B3; each DAG has `max_active_runs=1` and `catchup=False`; each task has an execution timeout; each task which writes to silver uses the pool `silver_writer`; the schedules equal the table above.
4. **`make demo`** runs on the demo stack (§3.3): the tracer on the fixture hour of the three feeds. `tests/e2e/test_demo.py` gains assertions: twelve ledger slots are `loaded`; the row counts of the four silver tables equal counts computed from the fixture zips with code which shares nothing with the pipeline; the total of the chart stays 4,023; a second run downloads nothing and leaves the content checksums unchanged.
5. The workflow `e2e` runs `make test-dags` before `make integration`.

**Done when.** `make demo` passes twice in a row on the laptop, and the workflow `e2e` is green.

**When the work fails.** `airflow dags test <dag id>` is the fast inner loop.

### WP-08 — Operational models in dbt (1.5 h)

**Goal.** Completeness comes from a model over the ledger (BP §8.2), and no dbt model carries a password (open item of P0).

**Produces.** The one-shot service `postgres-roles` with `docker/postgres/roles/01_readonly.sql`; one file in `users.d` of ClickHouse; the named collection `pg_ops`; `dbt/models/staging/ops/` and `dbt/models/ops/`; one smoke check.

1. The PostgreSQL role `gbmi_ro` can log in and read the schema `ops`, and nothing else. `.env.example` gains `GBMI_RO_DB_PASSWORD`; `make env` completes an existing `.env`. The service `postgres-roles` creates the role and the grants at every start, after `ops-migrate`.
2. The administrator user `default` of ClickHouse holds no right on named collections **[V]**: in the running container `CHECK GRANT CREATE NAMED COLLECTION ON *` returns 0, and `CHECK GRANT POSTGRES ON *.*` returns 1. A mounted file in `users.d` gives the user the setting `named_collection_control`, and Compose creates the ClickHouse container again. `clickhouse-init` then creates the named collection `pg_ops` (host, port, database, user and password of the role; the password comes from the environment of the container) and grants the role `dbt_rw` the use of `pg_ops` and the source privilege `POSTGRES`.
3. `stg_ops__source_slot` is a view over `postgresql(pg_ops, schema = 'ops', table = 'source_slot')`.
4. `ops_slot_status_daily` is rebuilt in full (BP §7.6): one row for each feed and slot date with the count of slots in each state and `completeness = loaded ÷ (96 − confirmed_missing)`, and one row for each slot date over all feeds, as BP §8.2 defines.
5. Tests: dbt tests on the model; one test which searches the compiled SQL in `dbt/target` for the reader password and finds nothing; one smoke check in which the dbt user reads the ledger through `pg_ops`.

**Done when.** `make dbt-build` passes and `ops.ops_slot_status_daily` shows the fixture day.

**Time box.** 1.5 hours. When the time box ends without a passing build, cut 1 of §10 applies; the hours of the time box are then used and not freed.

### WP-09 — Thirty-day backfill and live run (2.5 h of work; 8 to 26 h of elapsed time [E])

**Goal.** Exit-gate rows 1, 2, 3, 7 and 10, on the working stack.

1. Stop the demo stack and start the working stack (`make up`). Fix the window of §3.3: when WP-09 starts on day S, the window runs from day S−31 to day S−2. `gdelt_silver_hourly` stays paused until step 6; otherwise the job would build the last days of the window a first time.
2. `make toolbox CMD="gbmi gdelt enqueue --from <first day> --to <last day>"`. Expected: 8,640 ledger rows, each `listed` (F4).
3. Unpause `gdelt_ingest`, because a run of a paused DAG stays queued (F9). Drain bronze: a small script triggers `gdelt_ingest` through the REST API of Airflow with `batch_limit` 1,500 until no slot of the window is `listed`, and records the time and the bytes of each run for benchmark B1. Expected volume: about 13 GB (F4). At the 19 MB per second of the P0 probe the download takes about 12 minutes **[E]**. From step 3 on, the scheduled ingest runs also land the slots of day S−1 and of day S.
4. Unpause `gdelt_silver_daily`. First `airflow backfill create --dag-id gdelt_silver_daily --from-date … --to-date … --max-active-runs 1 --dry-run`, and compare the printed logical dates with the 30 dates of the window. Then the same command without `--dry-run`. The duration of one day is **TBM**.
5. Evidence: `gbmi ledger status` for the window; `ops_slot_status_daily` for completeness, built in the toolbox with `dbt build --select +ops_slot_status_daily`; the accounting query; the two days of the rerun proof (one weekday and one Sunday): `gbmi silver build-day` again for the three feeds, and the content checksums of the four tables equal the stored checksums.
6. Live run: unpause `gdelt_silver_hourly`. The live run lasts at least 6 hours and holds 08:25 UTC (16:25 on the laptop clock), when the scheduled run of `gdelt_silver_daily` builds day S−1. The latency sample starts when no pending slot is left from the time before step 6. Afterwards read from the ledger, for each slot of the sample: upload to `listed`, `listed` to `verified`, `listed` to the silver commit; report median and 95th percentile. A slot without `published_at` is left out of the first measure.
7. `docs/reports/p1-quarantine-reconciliation.md`: quarantined rows by feed and reason; quarantined files; for each feed and day the ledger against the bronze listing against the partition versions (counts and bytes).

**Done when.** The five gate rows hold.

**When the work fails.** A file which the GDELT servers do not serve stays `listed`; the runbook of WP-11 gives the two ways out (wait for the automatic confirmation, or `gbmi ledger confirm-missing` with a note). A day below 99.5% is reported as measured; the cause is found before any threshold is touched.

### WP-10 — Source profile and benchmarks B1, B2, B4 (3.5 h)

**Goal.** Benchmark report v1 (BP §15.6), the source profile (BP §4.1) and the three measurements which phase P0 left open.

1. **Source profile**, scripts in `benchmarks/source_profile/`, on the bronze files of the window (no further download) and one stream of the master list: bytes by month and feed; slots for each feed; lines out of order; rows and bytes for each column of the three feeds; the share of `V2GCAM`; titles present; copies of a normalised title within a day; distinct organisation strings for each day and the share of V1-only records; the share of repeated Mentions pairs (F5); `GLOBALEVENTID` values which occur in more than one slot of a day.
2. **B1 — ingestion throughput.** One fixed window of 6 hours (72 files) into a scratch lake at 1, 4 and 8 parallel downloads, three runs each: files for each minute and megabytes for each second. Plus the duration of the 30-day drain of WP-09.
3. **B2 — routine transformation.** One day of each feed, three runs each, as rebuilds of real days of the window on the working stack (the exception of §3.3): wall-clock time, peak resident memory of the process, peak of the container, rows for each second; the first run after a start of the stack is reported apart. Thirty days: the durations of WP-09 from `ops.silver_partition`. Each run also records the CPU seconds of the process (risk R4 asks for memory and CPU). Two further measurements: an hour overwrite inside a compacted day against a rebuild of the day (D17), on a scratch copy; a GKG day build while one ingest run downloads and ClickHouse runs the aggregation script of phase P0. The cost of four builds for each hour (D16) is read from the hour builds of the live run in `ops.silver_partition`; no separate experiment runs. The part which drops first in WP-10 is the comparison for D17.
4. **B4 — storage efficiency.** One GKG day as zip, as raw text, as Parquet with ZSTD with `V2GCAM` kept (a scratch table of the benchmark), and as the two silver tables. Events and Mentions: zip, raw text, silver. The mart size is reported only when the stretch item ran.
5. **Method rules** of BP §15.6: a fixed, recorded dataset; three runs or more; cold and warm runs apart; hardware, versions and configuration recorded; scripts and raw results committed under `benchmarks/`; median and range reported; no number is quoted anywhere before the report holds the number.
6. `docs/reports/benchmark-v1.md`; the target `make bench B=<n>` (BP §9.1).

**Done when.** Exit-gate row 9 holds, and the report answers the three open questions of phase P0 with numbers.

### WP-11 — Drills DR2, DR4, DR8 and two runbooks (2 h)

**Goal.** Three scripted drills with assertions (BP §9.8), run in CI on the fixtures, and the two runbooks of BP §11.2.

1. **`drills/dr2_crash_silver.py`.** A build of the fixture hour is killed before the Iceberg commit: the content checksum of the table is unchanged. A second build is killed between the Iceberg commit and the ledger update: silver holds the new rows and the slots are still `verified`. The rerun ends with `loaded` slots, a checksum equal to an undisturbed build and no duplicate key. The mart half of DR2 is P3 work.
2. **`drills/dr4_corrupt_file.py`.** A fetcher changes one byte of one zip: gate G1 rejects the file three times; the slot is `quarantined`; the bad bytes lie in quarantine; `ops.dq_result` holds the failure. Then `gbmi ledger requeue` and one run with the good file: the slot becomes `verified` and then `loaded`.
3. **`drills/dr8_rerun_silver.py`.** A closed day is built twice: the content checksums of the four tables are equal. The mart half of DR8 is P3 work.
4. `make drill DR=<n>` (BP §9.1); the workflow `e2e` runs the three drills after `make demo`.
5. **Runbooks** in the template of BP §15.4 (Symptom — Detection — Diagnosis — Recovery steps — Verification — Prevention): `docs/runbooks/gdelt-slot-quarantined-or-overdue.md` and `docs/runbooks/silver-build-failed-or-wrong-data.md`. The first runbook covers the requeue, the automatic confirmation, and the operator confirmation of a slot which the 1% rule quarantined: a requeue lands the same verified bytes again, so the confirmation is the only way to close the day. The second runbook repairs a bad unit by a rebuild from bronze and names the snapshot before the commit (`previous_snapshot_id`); the rollback command is P4 work (D29).

**Done when.** Exit-gate row 8 holds.

### WP-12 — ADR-007, status report, close-out (2 h)

1. `docs/adr/007-ledger-state-machine-and-completeness.md` in the template of `docs/adr/README.md`: the states, the guarded transitions, the calendar, the two rules of automatic confirmation, the meaning of completeness, the alternatives (retries without state; trust in the source list), and the evidence of WP-03 and WP-09. The index of the decision records is updated.
2. `docs/reports/p1-gdelt-production.md`: the exit gate with evidence; the window and the numbers of the backfill; the pin table; the choices made during the run; the places where a measurement corrects the blueprint; the open items; the hand-off to week 3.
3. README: the status, the new commands, the limits of the slice.
4. The handoff file in the project folder and the memory notes are brought up to date.
5. After the last merge: the four workflows are green on `main`; the annotated tag `p1` marks the merge commit and is pushed.

**Done when.** §2 holds.

---

## 8. Repository at the end of the week

Only new and changed paths are listed. `(new)` marks a new path.

```text
gbmi-platform/
├── WEEK_2_IMPLEMENTATION_PLAN.md                      (new)
├── Makefile                                           STACK, test with PostgreSQL, test-dags, drill, bench
├── pyproject.toml, uv.lock                            hypothesis; typer 0.29.3
├── .env.example                                       GBMI_RO_DB_PASSWORD
├── src/gbmi/
│   ├── cli.py                                         groups db, ledger, gdelt, silver, lake
│   ├── config.py                                      batch limit, concurrency, thresholds, windows
│   ├── core/                                          positional.py, gdelt_mentions.py, gdelt_gkg.py,
│   │                                                  text.py, failpoint.py (new); gdelt_export.py
│   ├── ledger/                                        (new) db.py, migrate.py, states.py, calendar.py,
│   │                                                  repository.py
│   ├── sources/gdelt/                                 ingest.py, reconcile.py (new); index.py, land.py
│   ├── transforms/                                    build.py (new); duckdb/positional.py,
│   │                                                  duckdb/gdelt_mention.py, duckdb/gdelt_gkg.py (new)
│   ├── lake/                                          tables.py (three schemas), commit.py (stage and commit)
│   ├── quality/                                       audit.py (gate G3), results.py (new)
│   └── ops/                                           (new) runs.py
├── dags/                                              gdelt_ingest.py, gdelt_silver_hourly.py,
│                                                      gdelt_silver_daily.py (new); tracer_gdelt_events.py
├── dbt/models/                                        staging/ops/, ops/ (new)
├── db/postgres/migrations/                            (new) 0001 to 0003
├── docker/postgres/roles/                             (new) 01_readonly.sql
├── docker/airflow/Dockerfile, .dockerignore           the migrations in the runtime image
├── docker/clickhouse/, db/clickhouse/init/            users.d file; named collection pg_ops; grants for dbt_rw
├── compose/compose.yaml, compose/smoke.sh             ops-migrate, postgres-roles; two new checks
├── tests/                                             unit/, transforms/ (extended); ledger/, contract/,
│                                                      dags/ (new); integration/, e2e/ (extended);
│                                                      fixtures/gdelt/v2/{mentions,gkg}/ (new)
├── drills/                                            (new) dr2_crash_silver.py, dr4_corrupt_file.py,
│                                                      dr8_rerun_silver.py
├── benchmarks/                                        source_profile/ (extended); b1_ingest/, b2_transform/,
│                                                      b4_storage/ with results (new)
├── docs/
│   ├── adr/                                           007-…md (new); README.md (index)
│   ├── contracts/                                     (new) four YAML files
│   ├── runbooks/                                      (new) two runbooks
│   └── reports/                                       p1-gdelt-production.md, benchmark-v1.md,
│                                                      p1-quarantine-reconciliation.md (new)
└── .github/workflows/                                 ci.yml (PostgreSQL service), e2e.yml (DAG tests, drills)
```

---

## 9. Verification

| Check | Command | Expected |
|---|---|---|
| Tooling | `make setup lint test` | Clean. The coverage of `gbmi.ledger`, `gbmi.core` and `gbmi.transforms.duckdb` is at least 80% |
| Migrations | `make up`, then `make toolbox CMD="gbmi db status"` | Three applied files; a second `make up` applies nothing |
| Stack | `make smoke` | Every check passes, the two new checks included (one new check when cut 1 of §10 applies) |
| Ledger rules | `uv run pytest tests/unit tests/ledger` | The property tests pass |
| Parsers | `uv run pytest tests/transforms tests/contract` | The fixture hour of three feeds parses completely; the contracts match the schemas |
| Spikes and builds | `make integration` | The tests of phase P0 and the build tests of WP-06 pass |
| DAGs | `make test-dags` | Three GDELT DAGs and the tracer import without an error; every assertion of WP-07 holds |
| End to end | `make demo`, twice (demo stack) | Passes; the second run downloads nothing and leaves the checksums unchanged; the working stack is unchanged |
| Drills | `make drill DR=2`, `make drill DR=4`, `make drill DR=8` | Each script ends with exit code 0 |
| Operational model | `make dbt-build` (not when cut 1 of §10 applies) | Passes; `ops.ops_slot_status_daily` holds one row for each feed and day |
| Backfill | `make toolbox CMD="gbmi ledger status --from <first day> --to <last day>"` | 8,640 slots; each slot is `loaded` or `confirmed_missing` |
| Completeness and accounting | The two queries of the status report | At least 99.5% for each of the 30 days; 100% of the loaded files |
| Rerun | `make toolbox CMD="gbmi silver build-day --feed <feed> --date <day>"` for the three feeds and for two days of the window (one weekday, one Sunday) | The content checksums of the four tables equal the stored checksums |
| Live run | Three DAGs unpaused for 6 hours or more, 08:25 UTC included | No failed run; one scheduled run of `gdelt_silver_daily`; the latencies are in the status report |
| CI | Merge the last pull request | `ci`, `dbt`, `e2e` and `terraform` are green on `main` |
| From nothing | `make reset STACK=gbmi-demo && make demo` | Passes. The command removes the volumes and the lake of the demo stack only. `make reset` for the working stack deletes the 30 days and needs the agreement of Jay |

---

## 10. Risks, time boxes and cut order

| Risk | Signal | Response |
|---|---|---|
| The GDELT servers slow down or refuse the backfill | HTTP 429, or a throughput far below the 19 MB per second of phase P0 | One download at a time; a pause of 60 seconds; the backfill continues in later runs, because the ledger keeps the state |
| The laptop sleeps during the backfill or the live run | Runs missing in Airflow; the ledger shows old `expected` slots | The next ingest run reads the master-list tail and drains the gap (risk R22 of the blueprint). The live run of 6 hours starts again |
| The real GKG parser needs more memory or time than the probe of phase P0 | A day build above 1.5 GB or above 10 minutes on the laptop | Smaller batches (one slot in place of one hour); the DuckDB spill directory; the number goes into the memory report (risk R4) |
| A real day falls outside the volume band of gate G3 | A day build fails with the volume check | Read the cause first. A true small day is accepted with `--accept-volume` and a note |
| The backfill of Airflow creates other dates than expected | The dry run prints dates which differ from the window | Change `--from-date` and `--to-date` until the dry run prints the 30 dates; record the rule in the runbook |
| A property test finds a case late in the week | A hypothesis failure in CI | Keep the failing example as a fixed test; repair the rule, not the test |
| The workflow `e2e` becomes slow | Above 20 minutes | Run the drills only on pushes to `main`; keep the DAG tests on every pull request |
| The week runs long (risk R27) | More than 37 hours used before WP-08 starts (33.5 are planned) | Apply the cut order, starting with cut 1 before WP-08 begins |

**Cut order** inside the 45 hours, first to go at the top:

| Cut | Hours freed | Consequence |
|---|---|---|
| 1. WP-08, the operational models in dbt | 1.5 | Completeness comes from one SQL query over the ledger in the status report. The named collection moves to P3, where the news marts need the ledger |
| 2. Benchmark B1 at 1 and 8 parallel downloads; the Parquet variant with `V2GCAM` of B4 | 1 | Benchmark report v1 holds B1 at 4 parallel downloads and B4 without the variant; P5 completes both |
| 3. The comparison for D17 in benchmark B2 | 0.5 | The decision D17 stays on the SK1 result; P4 measures the comparison |
| 4. The failpoint before the bronze move | 0.25 | Drill DR2 covers the two silver failpoints. The cut is possible only before WP-04 ends |
| 5. The live run of 6 hours | 0 (elapsed time) | Exit-gate row 7 is recorded as moved to P2, where the host runs the schedules |

A cut frees hours only when the cut is taken before the work starts. A time box which ends without a result applies the cut and frees no hours.

**Never cut** (BP §16.5): the ledger state machine; gates G1 to G3 with the pre-commit audit; the 30-day backfill with completeness and accounting; the rerun checksum test; the DAG test suite; CI.

---

## 11. Hand-off to week 3

Phase P2 (SEC ingestion and first deployment) starts when §2 holds. The tag `p1` marks the commit.

- **P2 inherits** the migration runner and the schema `ops`, the ledger pattern (the table `ops.source_object` gives SEC files the same life cycle, BP §7.3), `record_run`, the quality results, the failpoints, the drill and benchmark targets, and three DAGs which need only a new environment to run on the host.
- **P2 must read first** benchmark report v1: the memory and the duration of a GKG day decide the host column of the memory budget (BP §9.1, risk R4). The upload times of F1 and the schedules of §3.3 apply to the host unchanged.
- **Open items carried forward:** commit latency on S3 and the memory of the stack on the host (P2); bronze on S3 — a new download on the host or a copy of the laptop files (P2 decides); a ClickHouse role on AWS without write access to the lake (P2); the PostgreSQL reader password in Parameter Store (P2); the formal drill DR1 and the mart halves of DR2 and DR8 (P3); consistent pins of `gdelt_gkg_doc` and `gdelt_gkg_org` for one mart build (P3); the webhook and email alerts, the repair queue of gate G6 and the rollback command for a silver table (P4); clean-up of unreferenced files, of staged day files which a crash left behind and of old Iceberg metadata files (P4).
- **Points which stay with Jay:** the status of the alert email address (setup guide, step 4.1); the review of the gold-sheet labels before P3; the contact address for the SEC user agent, which the first SEC request of P2 needs.
