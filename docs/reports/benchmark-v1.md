# Benchmark report v1 — the first measured numbers

| | |
|---|---|
| Subject | The first measured numbers of the platform (blueprint §15.6): the source profile, benchmark B1 (ingestion throughput), benchmark B2 (routine transformation) and benchmark B4 (storage efficiency) |
| Phase | P1, work package WP-10 of the week 2 plan. Report v2 follows in phase P5 with the benchmarks B3 and B5 to B8 |
| Measured | 2026-11-26 and 2026-11-27 (UTC), on one laptop |
| Scripts and raw results | The folder `benchmarks/`. Each number of the report names the script and the result file |

## How to read the report

- **Units.** 1 MB is 1,000,000 bytes and 1 GB is 1,000 MB. MiB and GiB (1 MiB is 1,048,576 bytes) appear only where a tool reports memory in the units.
- **Feed.** One of the three GDELT file series: Events (`export`), Mentions (`mentions`) and the Global Knowledge Graph (`gkg`, short GKG).
- **Slot.** One interval of 15 minutes. GDELT publishes one zip file for each feed and slot: 96 slots and 288 files for one day.
- **Bronze** is the folder of the lake with the downloaded zip files, unchanged. **Silver** is the set of four Apache Iceberg tables with the parsed rows: `gdelt_event`, `gdelt_mention`, `gdelt_gkg_doc` (one row for each GKG document) and `gdelt_gkg_org` (one row for each organisation name of a document).
- **Repeated runs** are reported as the median with the lowest and the highest value.
- **Method rules** (blueprint §15.6): a fixed, recorded dataset; three runs or more for a timed measurement; the first run after a start apart from the later runs; machine, versions and configuration recorded; scripts and raw results in the repository.

## Machine, versions and dataset

| | |
|---|---|
| Laptop | MacBook Pro with an Apple M2 Pro (12 cores) and 16 GB of memory; macOS 27.2.1; internal disk |
| Docker | Docker Desktop with Docker 28.2.2. The virtual machine has 8,217,751,552 bytes of memory (7.65 GiB) and 12 CPUs; kernel 6.10.14-linuxkit, aarch64 |
| Other load on the virtual machine | Containers of other projects ran during every measurement (read with `docker stats`). On 2026-11-26, during the source profile and benchmark B4: twenty containers with about 2.1 GiB of memory at 19:40 UTC and about 2.0 GiB at 21:27 UTC. On 2026-11-27, right after benchmarks B1 and B2: twelve containers with about 1.2 GiB at 18:23 UTC |
| Stack | The working stack of the repository (Compose project `gbmi`) without Metabase: Airflow 3.4.0, PostgreSQL 17, ClickHouse 26.8. The scheduler container, which also runs the task processes, has a memory limit of 2,600 MiB |
| Libraries | Python 3.12.16, DuckDB 2.0.1, PyIceberg 0.12.0, pyarrow 26.0.1, httpx 0.28.1 |
| Lake | The folder `data/lake` on the laptop disk, mounted into the containers by Docker Desktop |
| Dataset | The 30 UTC days from 2026-10-26 to 2026-11-24 of the three feeds, as the backfill of WP-09 landed the files: 8,640 files, 12.90 GB. Gate G1 compared the size and the MD5 hash of each file with the file list of GDELT |
| Benchmark day | 2026-11-04, a Wednesday: the largest GKG day of the window (540.8 MB as zip files). 2026-11-22, a Sunday, serves as the small day |

## Source profile

The source profile describes what GDELT serves. Two scripts produced the numbers. Both scripts only read.

### The master file list: the history since 2015

GDELT keeps one text file with one line for each file of the three feeds: the size, the MD5 hash and the address. The script `benchmarks/source_profile/profile_master_list.py` read the list once as a stream on 2026-11-26 at 21:26 UTC. The list had 129,867,940 bytes and 1,216,290 lines, and the stream took 10.9 seconds. Raw result: `benchmarks/source_profile/results/master-list-2026-11-26.json`, with the files and the bytes of each of the 142 months.

| Fact | Events | Mentions | GKG |
|---|---:|---:|---:|
| Lines (files) | 405,408 | 405,408 | 405,413 |
| First slot | 2015-02-18 23:00 | 2015-02-18 23:00 | 2015-02-18 23:00 |
| Last slot | 2026-11-26 21:30 | 2026-11-26 21:30 | 2026-11-26 21:30 |
| Slots of the 15-minute calendar from the first to the last slot | 412,699 | 412,699 | 412,699 |
| Calendar slots which the list does not hold | 7,291 | 7,291 | 7,286 |
| Share of the calendar slots in the list | 98.23% | 98.23% | 98.23% |
| Slots listed more than once | 0 | 0 | 0 |
| Lines out of order | 1,167 | 1,167 | 1,170 |
| Compressed bytes | 40.3 GB | 68.2 GB | 2,896.7 GB |

- **The whole history has 3,005 GB of zip files.** GKG is 96.4% of the bytes.
- **The list is not sorted.** A line is out of order when the slot of the line is older than a slot which stands before the line in the list. The newest case: the slot 2026-11-15 02:45 stands after the slot 2026-11-15 04:30. A reader of the list must not assume sorted lines; the index reader of the platform does not.
- **1.77% of the calendar slots are missing in the list**, and the missing slots come in blocks. Missing slots of the Events feed by year: 200 in 2015, 45 in 2016, 994 in 2017, 992 in 2018, 11 in 2019, 2,585 in 2020, 610 in 2021, 5 in 2022, 165 in 2023, 2 in 2024, 1,681 in 2025 and 1 in 2026 (the slot 2026-05-09 23:30). The three feeds miss the same slots, with one difference: GKG holds 5 slots which Events and Mentions lack. A day with a missing slot can never show 96 loaded slots, so the ledger needs the state `confirmed_missing` (decision record ADR-007).
- **61 lines are no file of GDELT 2.0.** Each of the 61 lines holds only the address of the folder, without a file name.

Compressed bytes by year, in GB. The script counts a file in the year of the slot. The year 2015 starts on 18 February, and the year 2026 ends on 26 November.

| Year | Files | Events | Mentions | GKG | All feeds |
|---|---:|---:|---:|---:|---:|
| 2015 | 90,420 | 4.0 | 7.3 | 273.2 | 284.4 |
| 2016 | 105,274 | 5.0 | 9.1 | 403.1 | 417.1 |
| 2017 | 102,142 | 4.5 | 8.4 | 347.8 | 360.7 |
| 2018 | 102,144 | 4.2 | 7.9 | 311.4 | 323.5 |
| 2019 | 105,087 | 3.8 | 6.7 | 259.0 | 269.6 |
| 2020 | 97,653 | 3.1 | 5.2 | 216.0 | 224.2 |
| 2021 | 103,290 | 2.6 | 4.2 | 186.6 | 193.5 |
| 2022 | 105,105 | 2.4 | 3.5 | 165.2 | 171.1 |
| 2023 | 104,625 | 3.1 | 4.8 | 214.4 | 222.2 |
| 2024 | 105,402 | 3.0 | 4.4 | 204.1 | 211.5 |
| 2025 | 100,077 | 2.6 | 3.8 | 173.2 | 179.6 |
| 2026 | 95,010 | 2.1 | 3.0 | 142.7 | 147.9 |

Compressed bytes of the last 13 months, in MB. The month 2026-11 ends on 26 November.

| Month | Slots in the list | Events | Mentions | GKG | All feeds |
|---|---:|---:|---:|---:|---:|
| 2025-11 | 2,880 | 207.8 | 290.7 | 14,160.1 | 14,658.6 |
| 2025-12 | 2,976 | 192.1 | 270.5 | 13,441.8 | 13,904.4 |
| 2026-01 | 2,976 | 209.3 | 304.5 | 14,028.3 | 14,542.1 |
| 2026-02 | 2,688 | 197.9 | 280.8 | 13,238.6 | 13,717.4 |
| 2026-03 | 2,976 | 221.0 | 321.1 | 14,481.7 | 15,023.7 |
| 2026-04 | 2,880 | 195.1 | 296.5 | 13,461.0 | 13,952.6 |
| 2026-05 | 2,975 | 198.5 | 280.4 | 13,235.9 | 13,714.8 |
| 2026-06 | 2,880 | 183.7 | 257.4 | 12,854.1 | 13,295.2 |
| 2026-07 | 2,976 | 191.6 | 269.8 | 13,104.2 | 13,565.6 |
| 2026-08 | 2,976 | 178.5 | 248.5 | 12,353.2 | 12,780.2 |
| 2026-09 | 2,880 | 190.6 | 269.2 | 12,565.3 | 13,025.1 |
| 2026-10 | 2,976 | 202.8 | 283.9 | 12,832.6 | 13,319.4 |
| 2026-11 | 2,487 | 167.3 | 232.6 | 10,549.1 | 10,949.0 |

One month of the three feeds has between 12.8 GB and 15.0 GB of zip files in the last twelve full months. The twelve full months from 2025-11 to 2026-10 have 165.5 GB together: 2.4 GB of Events, 3.4 GB of Mentions and 159.8 GB of GKG. The blueprint estimates about 160 GB of zip files for the 12 months of the bulk backfill of phase P5 (§5.3). The text size of the twelve months is not measured. With the ratios of the window (next section) the text has about 530 GB; the number is an estimate.

### The 30 days of the window: the files in bronze

The script `benchmarks/source_profile/profile_window.py` read the 8,640 zip files of the window from bronze, hour by hour, in 295 seconds, with a peak of 777 MiB of resident memory. The script ran twice, on 2026-11-26 at 21:12 UTC and on 2026-11-27 at 18:16 UTC, and both runs gave the same counts; the result file is the file of the second run. The facts about titles, stories and organisation names come from the two GKG silver tables, and the share of repeated Mentions pairs comes from the partition versions of the day builds. Raw result: `benchmarks/source_profile/results/profile-2026-10-26-to-2026-11-24.json`, with the numbers of each day and of each column.

| Fact | Events | Mentions | GKG |
|---|---:|---:|---:|
| Files | 2,880 | 2,880 | 2,880 |
| Lines | 3,102,544 | 8,387,184 | 2,905,938 |
| Zip bytes | 196.1 MB | 273.4 MB | 12,434.0 MB |
| Bytes as text | 1,263.7 MB | 1,793.5 MB | 38,586.7 MB |
| Zip bytes against text bytes | 0.155 | 0.152 | 0.322 |
| Lines of one day: lowest, median, highest | 57,162 / 112,978 / 130,634 | 138,886 / 308,176 / 356,130 | 50,459 / 108,763 / 125,779 |
| Zip bytes of one day in MB: lowest, median, highest | 3.4 / 7.3 / 8.2 | 4.4 / 10.2 / 12.0 | 216.5 / 467.6 / 540.8 |
| Median lines of a Wednesday | 124,212 | 343,841 | 119,182 |
| Median lines of a Sunday | 60,672 | 158,507 | 52,090 |
| Columns in the codebook | 61 | 16 | 27 |
| Lines with another number of columns | 0 | 0 | 0 |
| Columns which no line fills | none | `MentionDocTranslationInfo`, `Extras` | `V2.1TRANSLATIONINFO` |

The columns with the most bytes. The share is the share of all field bytes of the feed, without the tab characters. "Filled" is the share of the lines with a value in the column.

| Feed | Column | Bytes | Share | Filled |
|---|---|---:|---:|---:|
| Events | `SOURCEURL` | 327.0 MB | 30.3% | 100.0% |
| Events | `ActionGeo_FullName` | 76.9 MB | 7.1% | 97.3% |
| Events | `Actor1Geo_FullName` | 69.1 MB | 6.4% | 88.1% |
| Mentions | `MentionIdentifier` | 926.8 MB | 55.6% | 100.0% |
| Mentions | `MentionDocTone` | 134.9 MB | 8.1% | 100.0% |
| Mentions | `MentionSourceName` | 123.3 MB | 7.4% | 100.0% |
| GKG | `V2GCAM` | 26,611.0 MB | 69.1% | 100.0% |
| GKG | `V2ENHANCEDTHEMES` | 3,442.6 MB | 8.9% | 89.2% |
| GKG | `V1THEMES` | 1,626.6 MB | 4.2% | 89.2% |
| GKG | `V2ENHANCEDLOCATIONS` | 1,406.4 MB | 3.7% | 78.8% |
| GKG | `V2EXTRASXML` | 1,402.1 MB | 3.6% | 100.0% |

The facts which the week 2 plan asks for (WP-10, step 1):

| Question | Result over the 30 days |
|---|---|
| The share of `V2GCAM` | 69.1% of the field bytes of GKG: 26.6 GB of 38.5 GB. Silver does not keep the column |
| Titles present | Every one of the 2,905,938 GKG lines has the tag `<PAGE_TITLE>`. 2,900,503 documents (99.81%) have a title which is not empty after the cleaning; the share of one day lies between 99.75% and 99.86% |
| Copies of a normalised title within a day | 1,023,706 of the 2,900,503 documents with a title (35.3%) repeat the normalised title of another document of the same day. The share of one day lies between 32.4% and 37.3% |
| Documents with organisation names | 2,226,672 documents (76.6%) name one organisation or more: 6,608,297 rows in `gdelt_gkg_org`, 2.97 for each such document |
| Distinct organisation strings for each day | Median 63,470 distinct normalised strings in one day; lowest 29,944, highest 69,895 |
| Records with organisation names in the V1 column only | 82,254 documents, 3.69% of the documents with organisation names; the share of one day lies between 3.1% and 5.0%. The count from the bronze lines and the count from silver agree |
| Repeated pairs of event and article in Mentions | 151,649 of 8,387,184 rows (1.81%) repeat a pair inside one file. The share of one day lies between 1.43% and 2.36%, median 1.77% |
| `GLOBALEVENTID` values in more than one slot of a day | None in the 30 days |

Four single-file findings of the week 2 plan now have a number over 30 days: `V2GCAM` has 69.1% of the bytes (one file: 69.9%, finding F6); 3.69% of the records with organisation names fill only the V1 column (one file: 21 of 783, which is 2.7%, finding F6); 1.81% of the Mentions rows repeat a pair (one file: 25 of 2,880, which is 0.87%, finding F5); every record has a title tag (finding F7).

## B4 — storage efficiency

**Question.** How many bytes does one day need in each form, and what does the column `V2GCAM` cost?

**Method.** The script `benchmarks/b4_storage/storage.py` (`make bench B=4`) measures one day. The zip bytes are the sizes of the files in bronze. The text bytes are the uncompressed sizes which each zip records. The silver bytes are the sizes of the Parquet data files of the day in each Iceberg table. For GKG the script also writes four scratch Parquet files with ZSTD, one row group for each hour, and removes the files at the end. A size is exact and does not change between runs, so each day was measured once. Raw results: `benchmarks/b4_storage/results/storage-<day>.json`.

Sizes in MB. The number in brackets is the size against the zip files of the same day.

| Form of one GKG day | 2026-11-04 (Wed) | 2026-11-18 (Wed) | 2026-11-22 (Sun) |
|---|---:|---:|---:|
| Zip files in bronze, as GDELT serves the files | 540.8 (1.000) | 500.7 (1.000) | 223.9 (1.000) |
| The same files as text | 1,680.5 (3.107) | 1,551.4 (3.099) | 698.8 (3.121) |
| Parquet with ZSTD: the 27 source columns as text | 429.3 (0.794) | 399.5 (0.798) | 169.4 (0.756) |
| Parquet with ZSTD: the 26 source columns without `V2GCAM` | 115.7 (0.214) | 104.1 (0.208) | 45.7 (0.204) |
| Parquet with ZSTD: the silver documents with the text of `V2GCAM` beside each row | 332.8 (0.615) | 313.2 (0.626) | 132.2 (0.591) |
| Silver table `gdelt_gkg_doc` | 19.1 (0.035) | 18.1 (0.036) | 8.5 (0.038) |
| Silver table `gdelt_gkg_org` | 6.6 (0.012) | 6.4 (0.013) | 2.7 (0.012) |
| Silver, both tables | 25.7 (0.048) | 24.5 (0.049) | 11.2 (0.050) |

| Feed and day | Zip files | As text | Silver table | Silver against zip |
|---|---:|---:|---:|---:|
| Events, 2026-11-04 (Wed) | 8.1 | 51.2 | 4.0 | 0.491 |
| Events, 2026-11-18 (Wed) | 8.2 | 52.6 | 4.0 | 0.488 |
| Events, 2026-11-22 (Sun) | 3.5 | 24.3 | 2.0 | 0.577 |
| Mentions, 2026-11-04 (Wed) | 12.0 | 76.0 | 4.6 | 0.383 |
| Mentions, 2026-11-18 (Wed) | 11.7 | 75.6 | 4.5 | 0.381 |
| Mentions, 2026-11-22 (Sun) | 4.5 | 32.3 | 1.9 | 0.432 |

The whole window, from `benchmarks/b4_storage/window_sizes.py` (raw result: `benchmarks/b4_storage/results/window-sizes-2026-10-26-to-2026-11-24.json`):

| Feed | Zip files of 30 days | Silver of 30 days | Silver against zip | Silver of one day: lowest, median, highest |
|---|---:|---:|---:|---|
| Events | 196.1 MB | 99.7 MB | 0.508 | 2.0 / 3.6 / 4.0 MB |
| Mentions | 273.4 MB | 107.8 MB | 0.394 | 1.9 / 4.0 / 4.6 MB |
| GKG, table `gdelt_gkg_doc` | 12,434.0 MB | 451.1 MB | 0.036 | 8.3 / 16.9 / 19.1 MB |
| GKG, table `gdelt_gkg_org` | (the same files) | 156.2 MB | 0.013 | 2.6 / 5.9 / 6.6 MB |
| All feeds | 12,903.5 MB | 814.8 MB | 0.063 | |

**Findings.**

- **Silver of the 30 days has 0.81 GB; bronze has 12.90 GB.** Each silver table holds one data file for each day.
- **The column `V2GCAM` decides the size of GKG.** With the same 27 source columns as text, Parquet with ZSTD needs 76% to 80% of the zip bytes on the three days. Without `V2GCAM` the same file needs 20% to 21%. `V2GCAM` is 73% to 74% of the Parquet file of the source columns.
- **One GKG day in silver is 20 to 21 times smaller than the zip files and 63 to 65 times smaller than the text.** Silver keeps the columns which the marts need and drops `V2GCAM` (blueprint §8.4). The silver documents with the text of `V2GCAM` beside each row would need 333 MB for the benchmark day in place of 19 MB.
- **The blueprint estimated 2.5 to 4 GB for one month of GKG in silver** (the probe of phase P0 gave 103 MB for one day, with every column except `V2GCAM` kept as text). The measured month has 0.61 GB.
- **The events mart is small.** The stretch item of the week 2 plan (change C8) was taken. The script `benchmarks/b4_storage/events_mart.py` pinned `lake.gdelt_event` in ClickHouse (3,383,053 rows: the 30 days and the days from 2026-11-25 to 2026-11-27) and built the mart `marts.fct_gdelt_event_daily` for the month partitions 202610 and 202611 with dbt, in 5.2 seconds; 11 of 11 steps passed. The mart holds 336,028 rows in 5.34 MB on disk (43.8 MB before compression): 63,662 rows and 1.02 MB for October, 272,366 rows and 4.32 MB for November. For each of the two months the events of the mart equal the rows of the pinned silver table: 656,819 and 2,726,234. Raw result: `benchmarks/b4_storage/results/events-mart-202610-202611.json`. The large marts of phase P3 do not exist yet.

## B1 — ingestion throughput

**Question.** How many files and megabytes does the ingest code land in one minute, and what does the number of parallel downloads change?

**Dataset.** The 72 files of the three feeds for the six hours from 12:00 to 17:45 UTC of 2026-11-04, as the ledger lists the files: 155.2 MB (GKG 149.6 MB, Mentions 3.3 MB, Events 2.2 MB). The result file names each file with the size and the MD5 hash.

**Method.** The script `benchmarks/b1_ingest/throughput.py` (`make bench B=1`) downloads the 72 files from the servers of GDELT through the code of the ingest job: download, gate G1, write to bronze, ledger update. Each run has a new scratch lake, a new throwaway ledger and new connections. The numbers of parallel downloads took turns (1, 4, 8, and again), with 5 seconds between two runs, so a drift of the network touches each number alike. Nine runs, on 2026-11-27 from 17:46 to 17:50 UTC, with the three GDELT DAGs paused. Raw result: `benchmarks/b1_ingest/results/throughput-2026-11-04-h12-6h.json`.

Each cell gives the median of three runs, with the lowest and the highest value in brackets.

| Parallel downloads | Seconds for the 72 files | MB for each second | Files for each minute |
|---:|---|---|---|
| 1 | 39.6 (14.8 to 40.5) | 3.9 (3.8 to 10.5) | 109 (107 to 292) |
| 4 | 6.0 (5.7 to 8.4) | 26.0 (18.5 to 27.0) | 724 (514 to 752) |
| 8 | 4.5 (4.3 to 4.6) | 34.8 (33.6 to 36.3) | 970 (936 to 1,011) |

The nine runs in the order of the measurement: 1 parallel: 39.6 s; 4 parallel: 8.4 s; 8 parallel: 4.3 s; 1 parallel: 14.8 s; 4 parallel: 5.7 s; 8 parallel: 4.6 s; 1 parallel: 40.5 s; 4 parallel: 6.0 s; 8 parallel: 4.5 s. Each run verified 72 of 72 files, and no request was answered with HTTP 429.

**The 30-day drain of WP-09.** The same code ran inside the DAG `gdelt_ingest`, with 4 parallel downloads and batches of 1,500 files (`benchmarks/b1_ingest/drain_bronze.py`; raw result `drain-2026-10-26-to-2026-11-24.jsonl`):

| Fact | Value |
|---|---|
| Files and bytes | 8,977 files, 13.37 GB, in six runs |
| Task time of the six runs | 813.9 seconds |
| MB for each second | 16.4 over the six runs; 14.1 to 18.7 for one run |
| Files for each minute | 662 over the six runs; 593 to 789 for one run |
| From the first start to the last end | 14 minutes and 34 seconds |

**Findings.**

- **Four parallel downloads are 6.6 times as fast as one** (6.0 seconds against 39.6 seconds in the median). Eight are 1.3 times as fast as four. The default of the ingest job stays at 4; the blueprint allows 4 to 8 (§8.3).
- **One download at a time varies most:** one of the three runs took 14.8 seconds, the two other runs took 40 seconds. The cause was not examined.
- **The drain reached 16.4 MB for each second, the window of 72 files 26.0 MB for each second, both with 4 parallel downloads.** The two numbers come from different measurements: the drain ran as a task of Airflow in the scheduler container over 13.4 GB, the window ran in the toolbox container over 155 MB. The reason for the difference was not examined.
- **A routine run is far below both numbers.** One run of `gdelt_ingest` lands three files, about 4 MB, every 15 minutes.

## B2 — routine transformation

**Question.** How long does the build of one day take, how much memory and CPU does the build need, and what did the 30 days cost?

**Dataset.** The day 2026-11-04, a Wednesday, the largest day of the window, with 96 files for each feed. Events: 8.1 MB as zip files, 125,229 lines. Mentions: 12.0 MB, 353,999 lines. GKG: 540.8 MB, 125,779 lines, which give 125,779 document rows and 291,651 organisation rows.

**Method.** The script `benchmarks/b2_transform/rebuild_day.py` (`make bench B=2`) runs the operator command `gbmi silver build-day --day 2026-11-04 --feed <feed>` in a child process. The command does the whole build: read the 96 files from bronze, gate G2, the audit of gate G3, the Iceberg commit and the ledger update. The script records:

- the wall-clock seconds of the command;
- the CPU seconds and the peak resident memory of the child process, from the system call `wait4`;
- the memory of the container, read every 0.1 seconds from the control group. A container has two numbers: the total, which includes the page cache of the files which the build read, and the anonymous memory, which the kernel cannot give back.

The measuring script itself holds 106 MiB when the script starts a child, so the peak of a child is the peak of the build. A rebuild adds one partition version for each table (the exception of the week 2 plan, §3.3). Each of the 18 rebuilds ended with exit code 0 and gave the content checksum of the version before.

The series `benchmarks/b2_transform/run_b2.sh` ran on 2026-11-27 from 18:07 to 18:13 UTC, with the three GDELT DAGs paused:

1. `make down`, `make up`, then one run for each feed: the first run after a start of the stack. The file cache of the Docker machine and of macOS was not emptied, so "first" means new containers and new processes, not cold disks.
2. Three more runs for each feed.
3. Three runs of the GKG day inside the scheduler container, where a task process of Airflow runs. The container has a memory limit of 2,600 MiB.
4. The same three runs under load. ClickHouse ran three aggregations over the two GKG silver tables in a loop (`benchmarks/b2_transform/clickhouse_load.py`), and a second process downloaded the 288 files of the day from GDELT again and again, with 4 parallel downloads.

Raw results, under `benchmarks/b2_transform/results/`: `rebuild-2026-11-04-*.json`, `stats-2026-11-04-*.json` (`docker stats`, read each second), `clickhouse-load-2026-11-04.json` and `download-load-2026-11-04-*.json`.

**One day, in the toolbox container.** The container has no memory limit. A cell with three runs gives the median, with the lowest and the highest value in brackets.

| Feed | Runs | Wall-clock seconds | CPU seconds | Peak resident memory of the process | Peak anonymous memory of the container | Raw lines for each second |
|---|---|---|---|---|---|---|
| Events | The first run after a start | 3.72 | 4.64 | 330 MiB | 278 MiB | 33,666 |
| Events | Three more runs | 3.32 (3.31 to 3.47) | 4.22 (4.08 to 4.56) | 337 (328 to 339) MiB | 281 (281 to 285) MiB | 37,678 (36,063 to 37,827) |
| Mentions | The first run after a start | 4.08 | 5.82 | 803 MiB | 748 MiB | 86,776 |
| Mentions | Three more runs | 4.13 (4.07 to 4.13) | 5.67 (5.21 to 5.94) | 781 (767 to 808) MiB | 702 (681 to 736) MiB | 85,768 (85,707 to 87,025) |
| GKG | The first run after a start | 20.24 | 23.31 | 1,023 MiB | 976 MiB | 6,216 |
| GKG | Three more runs | 19.92 (19.38 to 20.28) | 22.40 (22.19 to 22.78) | 991 (988 to 1,000) MiB | 946 (922 to 951) MiB | 6,314 (6,202 to 6,492) |

**The GKG day in the scheduler container.**

| GKG day in the scheduler container | Wall-clock seconds | CPU seconds | Peak resident memory of the process | Peak memory of the container, page cache included | Peak anonymous memory of the container |
|---|---|---|---|---|---|
| Alone, three runs | 20.51 (20.38 to 20.62) | 23.44 (23.38 to 23.47) | 1,008 (978 to 1,014) MiB | 1,878 (1,800 to 1,906) MiB | 1,232 (1,232 to 1,261) MiB |
| Under load, three runs | 34.39 (33.23 to 34.53) | 36.95 (35.31 to 37.59) | 1,049 (1,037 to 1,074) MiB | 1,698 (1,620 to 1,827) MiB | 1,311 (1,298 to 1,330) MiB |

The anonymous memory of the scheduler container includes the processes of the Airflow scheduler. During the runs under load, ClickHouse finished 69 aggregations in 117 seconds (the slowest took 3.8 seconds, the largest used 317 MiB), and the download process landed the 288 files (560.9 MB) five times, at 28 to 40 MB for each second. The highest memory of the two containers since the start of the stack, page cache included: 1,914 MiB of 2,600 MiB for the scheduler container and 1,833 MiB of 2,500 MiB for the ClickHouse container. The counter `oom_kill` of both control groups stayed at 0.

**Thirty days.** The day builds of the backfill of WP-09, read from `ops.silver_partition` (`benchmarks/b2_transform/ledger_durations.py`; raw result `ledger-durations-2026-10-26-to-2026-11-24.json`). A duration is the time inside the build, without the start of the process.

| Feed | Build of one day in seconds | All 30 days in seconds | Raw lines for each second | Zipped MB for each second | Slowest day | Fastest day |
|---|---|---:|---|---|---|---|
| Events | 2.58 (1.93 to 2.84) | 75.1 | 43,434 (29,273 to 47,769) | 2.76 (1.73 to 3.09) | 2026-11-17 (Tue): 2.84 s, 127,432 lines | 2026-11-08 (Sun): 1.93 s, 57,162 lines |
| Mentions | 3.04 (1.94 to 3.41) | 87.6 | 99,448 (67,441 to 106,787) | 3.25 (2.15 to 3.52) | 2026-11-04 (Wed): 3.41 s, 353,999 lines | 2026-11-15 (Sun): 1.94 s, 138,886 lines |
| GKG | 16.88 (8.68 to 19.77) | 462.0 | 6,358 (5,399 to 6,524) | 27.13 (23.53 to 27.85) | 2026-11-04 (Wed): 19.77 s, 125,779 lines | 2026-11-15 (Sun): 8.68 s, 50,459 lines |
| All feeds | | 624.7 | | | | |

The 90 builds took 624.7 seconds together. The backfill took 834 seconds from the first start to the last end, with the task starts of Airflow.

**Four builds for each hour (difference D16 of the week 2 plan).** The hourly job runs every 15 minutes and builds an hour again when the next file of the hour arrives, so an hour is built up to four times. The table reads the hour builds of the last period of the live run (2026-11-27, 11:26 to 17:45 UTC) from `ops.silver_partition`: the six hours from 12:00 to 17:45, each with four builds.

| Feed | Hours with four builds | Seconds of the four builds together | Seconds of the last build, with four files | Four builds against the last build |
|---|---:|---|---|---|
| Events | 6 | 1.37 (1.34 to 6.55) | 0.39 (0.33 to 3.64) | 3.5 (1.8 to 4.7) times |
| Mentions | 6 | 0.93 (0.91 to 1.45) | 0.27 (0.25 to 0.44) | 3.5 (3.2 to 3.7) times |
| GKG | 6 | 3.80 (3.46 to 7.97) | 1.29 (1.19 to 3.90) | 2.9 (2.0 to 3.2) times |

The 25 runs of the hourly job in the period took 49.8 seconds together: 1.6 seconds in the median and 8.1 seconds at most.

**Findings.**

- **The largest GKG day builds in 20 seconds with a peak of about 1.0 GiB.** The process peaked at 991 MiB in the toolbox container and at 1,008 MiB in the scheduler container (medians). The task budget of the blueprint is 2 GB (§9.1). The two signals of the week 2 plan for a build which is too large, 1.5 GB and 10 minutes (§10), are not reached.
- **Events and Mentions build in 3 to 4 seconds.** A Mentions day needs 781 MiB, more than twice the memory of an Events day (337 MiB), with 2.8 times the lines.
- **The first run after a start is not slower in a way which matters:** 20.2 seconds against 19.9 seconds for GKG, 3.7 against 3.3 for Events.
- **Under load the GKG day takes 1.7 times as long:** 34.4 seconds against 20.5 seconds. The memory of the process stays at about 1.0 GiB (1,049 MiB).
- **The 30 days of the three feeds cost 10.4 minutes of build time.** GKG is 74% of the time. One GKG day took between 8.7 seconds (a Sunday) and 19.8 seconds (the benchmark day).
- **Four builds of an hour cost about 3 to 3.5 times one build of the complete hour:** 3.8 seconds against 1.3 seconds for a GKG hour. The hourly job worked for 50 seconds in 6.3 hours.

## The three questions which phase P0 left open

The status report of phase P0 handed three measurements to phase P1.

**1. How much memory does the real GKG parser need?** The probe of phase P0 built a GKG day without parsing: 535 MiB and 6.9 seconds. The real build, with the titles, the tone values, the themes, the countries, the organisation rows, the duplicate check and the audit, needs about 1.0 GiB and 20 seconds for the largest day of the window (benchmark B2). A daily build fits the task budget of 2 GB with a factor of two.

**2. Does a silver build work beside other load?** Yes, on the laptop: the GKG day built in 34.4 seconds while ClickHouse aggregated the GKG tables and a second process downloaded the day. No process was killed, and both containers stayed below the limits (benchmark B2). Not measured: the host of phase P2, which has 2 CPUs where the laptop has 12.

**3. What does an hour overwrite inside a finalised day cost (difference D17)?** A day build finalises a day: each table then holds one data file for the day. When a file of such a day arrives late, the platform builds the whole day again (D17). The other way is an overwrite of the one hour. The script `benchmarks/b2_transform/hour_overwrite.py` measured both ways on a scratch copy of the benchmark day, three times each, for the hour from 12:00 to 12:45 (raw result `hour-overwrite-2026-11-04.json`). Each step ran in a child process under a small wrapper process, so the peak of a step does not include memory of the measuring script.

| Feed | Step | Wall-clock seconds | CPU seconds | Peak resident memory of the process | Data files of the day after the step |
|---|---|---|---|---|---|
| Events | Build of the whole day | 3.42 (3.26 to 3.43) | 4.17 (4.08 to 4.22) | 302 MiB (280 to 318) | 1 in `gdelt_event` |
| Events | Overwrite of the one hour | 0.99 (0.93 to 1.22) | 1.78 (1.37 to 3.11) | 486 MiB (467 to 521) | 2 in `gdelt_event` |
| GKG | Build of the whole day | 21.20 (20.76 to 21.66) | 24.30 (24.12 to 24.96) | 1,014 MiB (986 to 1,052) | 1 in `gdelt_gkg_doc`, 1 in `gdelt_gkg_org` |
| GKG | Overwrite of the one hour | 2.54 (2.44 to 2.95) | 4.90 (4.80 to 5.57) | 1,066 MiB (1,045 to 1,074) | 2 in `gdelt_gkg_doc`, 2 in `gdelt_gkg_org` |

- **The hour overwrite saves time:** 2.5 seconds against 21.2 seconds for GKG, 1.0 second against 3.4 seconds for Events.
- **The hour overwrite saves no memory.** Iceberg writes the data file of the day again without the rows of the hour, so the process holds the rows of the day: 1,066 MiB against 1,014 MiB for GKG, and 486 MiB against 302 MiB for Events.
- **The hour overwrite leaves two data files for the day,** one with the other 23 hours and one with the hour. After each overwrite the day held the same rows as before: the content checksum did not change.
- **The decision D17 stays.** A late file for a finalised day rebuilds the day. The price is about 19 seconds for a GKG day, the memory is the same, and each finalised day keeps one file and one build path. No such file arrived in the 30 days.

## Not measured in report v1

| Open | Phase |
|---|---|
| The same builds on the host of phase P2 (2 CPUs, 8 GB), and the commit latency on S3 | P2 |
| The large marts and the partition replacement at volume | P3 |
| Cold disks: no run started with an empty file cache | P5 |
| The memory of a small day, and a day larger than 541 MB of GKG zip files | P5 |
| Engine comparison (B3), query latency (B5), freshness over the soak (B6), rebuild and recovery (B7), mining (B8) | P5 and P6 |

## How to repeat the measurements

Each command runs in the repository folder, on the working stack (`make up`). Benchmarks B1 and B2 need the three GDELT DAGs paused. The results depend on the days which the stack holds.

```bash
# Source profile: one stream of the master list; the bronze files and silver tables of a window
make toolbox CMD="python -m benchmarks.source_profile.profile_master_list"
make toolbox CMD="python -m benchmarks.source_profile.profile_window --from 2026-10-26 --to 2026-11-24"

make bench B=1                                      # ingestion throughput, 72 files, 9 runs
make bench B=2                                      # three rebuilds of one day for each feed
bash benchmarks/b2_transform/run_b2.sh 2026-11-04   # the whole series of B2; restarts the stack once
make toolbox CMD="python -m benchmarks.b2_transform.hour_overwrite --day 2026-11-04"
make toolbox CMD="python -m benchmarks.b2_transform.ledger_durations --from 2026-10-26 --to 2026-11-24"
make bench B=4                                      # storage of one day
make toolbox CMD="python -m benchmarks.b4_storage.window_sizes --from 2026-10-26 --to 2026-11-24"
make toolbox CMD="python -m benchmarks.b4_storage.events_mart --months 202610 202611"
```

`make bench` uses the day 2026-11-04; `BENCH_DAY=<day>` names another day.
