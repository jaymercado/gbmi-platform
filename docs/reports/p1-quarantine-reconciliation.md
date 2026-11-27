# P1 report — quarantine and reconciliation of the 30 days

| | |
|---|---|
| Work package | WP-09, step 7 of the week 2 plan; exit-gate row 10 |
| Window | The 30 UTC days from 2026-10-26 to 2026-11-24: 8,640 expected files |
| Evaluated | 2026-11-26 at 21:01:39 UTC, 16 minutes after the end of the backfill, on the working stack of the laptop |
| Script | `benchmarks/p1_backfill/quarantine_reconciliation.py`. The script only reads |
| Raw result | `benchmarks/p1_backfill/results/quarantine-reconciliation-2026-10-26-to-2026-11-24.json` |

## Terms

- **Feed:** one of the three GDELT file series: Events (`export`), Mentions (`mentions`) and the Global Knowledge Graph (`gkg`, short GKG).
- **Slot:** one interval of 15 minutes. GDELT publishes one file for each feed and slot, so one day has 96 slots and 288 files.
- **Ledger:** the PostgreSQL table `ops.source_slot`, with one row for each expected file and the state of the file.
- **Bronze:** the folder of the lake with the downloaded zip files, unchanged.
- **Silver:** the four Iceberg tables with the parsed rows.
- **Quarantine:** the place for data which failed a quality gate. A quarantined *row* is one line of a file which failed gate G2, the check of each row; the line goes to a quarantine file with the reason. A quarantined *file* is a whole file which failed gate G1 (size, MD5 hash, readable zip) three times, or a file with more than 1% failed rows.
- **Partition version:** one row of `ops.silver_partition` for each build of a unit, where a unit is one day or one hour of one table. The row holds the number of files, the row counts and the checksum of the build.

## Result

- No row of the window is in quarantine: 14,395,666 raw lines gave 14,395,666 parsed rows and 0 quarantined rows.
- No file of the window is in quarantine: no slot has the state `quarantined`, and the quarantine area of the lake holds no file.
- For each of the 90 pairs of feed and day, three sources agree: the ledger (loaded files and bytes), the listing of the bronze folder (files and bytes) and the newest day-level partition version (files). Each pair has 96 files.

## Quarantined rows

| Feed | Raw lines | Parsed rows | Quarantined rows in the ledger | Rows in quarantine files | Reasons |
|---|---:|---:|---:|---:|---|
| Events | 3,102,544 | 3,102,544 | 0 | 0 | none |
| Mentions | 8,387,184 | 8,387,184 | 0 | 0 | none |
| GKG | 2,905,938 | 2,905,938 | 0 | 0 | none |
| Total | 14,395,666 | 14,395,666 | 0 | 0 | |

The two counts of quarantined rows come from two places and agree. The accounting columns of the ledger count each rejected line once. The quarantine files of the units hold the rejected lines. Both counts are 0.

A zero needs a second look: a gate which rejects nothing could be a gate which checks nothing. Three facts speak against a silent gate.

1. The parser tests feed broken lines to gate G2: a wrong number of columns, a missing key, a text in a number column, a number which is not finite. The gate rejects each line with the reason (`tests/transforms`, `tests/contract`).
2. Drill DR4 corrupts a recorded file, and the pipeline quarantines the file (`make drill DR=4`).
3. A separate script, the source profile, read the same 8,640 files and found no line with another number of columns than the codebook gives: 61 for Events, 16 for Mentions, 27 for GKG (`benchmarks/source_profile/results/profile-2026-10-26-to-2026-11-24.json`).

A parsed row is not always a silver row. The Mentions feed repeats pairs of event and article inside one file (finding F5 of the week 2 plan). The build keeps one row of each repeated pair and counts the other rows as duplicates. A duplicate is counted in the partition version and is not quarantined.

| Table | Parsed rows | Duplicates | Rows in silver |
|---|---:|---:|---:|
| Events (`gdelt_event`) | 3,102,544 | 0 | 3,102,544 |
| Mentions (`gdelt_mention`) | 8,387,184 | 151,649 (1.81%) | 8,235,535 |
| GKG documents (`gdelt_gkg_doc`) | 2,905,938 | 0 | 2,905,938 |
| GKG organisations (`gdelt_gkg_org`) | — | — | 6,608,297 |

The organisation table has one row for each normalised organisation name of a document. Gate G3 compares the number of rows with the sum of the column `org_count` over the documents, for every build.

## Quarantined files

None. Gate G1 passed for every download: until 21:08 UTC, the table `ops.dq_result` held 9,186 results of the check `landing`, each with the status `passed`. The 9,186 files are the 8,640 files of the window and 546 files of the two days after the window. No file needed a second attempt: the column `attempts` is 0 in every row of the ledger.

## Reconciliation by feed

| Feed | Loaded files in the ledger | Files in bronze | Files in the newest day versions | Bytes in the ledger | Bytes in bronze |
|---|---:|---:|---:|---:|---:|
| Events | 2,880 | 2,880 | 2,880 | 196,149,481 | 196,149,481 |
| Mentions | 2,880 | 2,880 | 2,880 | 273,380,213 | 273,380,213 |
| GKG | 2,880 | 2,880 | 2,880 | 12,434,008,419 | 12,434,008,419 |
| Total | 8,640 | 8,640 | 8,640 | 12,903,538,113 | 12,903,538,113 |

The bytes of the ledger are the sizes in the file list of GDELT. The bytes of bronze are the sizes of the files on disk.

## Reconciliation by day

Each cell "files" gives three numbers: loaded files in the ledger / files in bronze / files in the newest day-level partition version. The bytes of the ledger and the bytes of bronze are equal in every row, so the table shows one number.

| Day | Weekday | Events files | Events bytes | Mentions files | Mentions bytes | GKG files | GKG bytes | All agree |
|---|---|---|---:|---|---:|---|---:|---|
| 2026-10-26 | Mon | 96 / 96 / 96 | 6,811,874 | 96 / 96 / 96 | 9,057,810 | 96 / 96 / 96 | 432,769,973 | yes |
| 2026-10-27 | Tue | 96 / 96 / 96 | 7,704,403 | 96 / 96 / 96 | 10,766,759 | 96 / 96 / 96 | 503,846,895 | yes |
| 2026-10-28 | Wed | 96 / 96 / 96 | 7,962,885 | 96 / 96 / 96 | 11,098,410 | 96 / 96 / 96 | 514,980,749 | yes |
| 2026-10-29 | Thu | 96 / 96 / 96 | 7,707,146 | 96 / 96 / 96 | 10,558,836 | 96 / 96 / 96 | 515,306,720 | yes |
| 2026-10-30 | Fri | 96 / 96 / 96 | 7,344,295 | 96 / 96 / 96 | 10,223,706 | 96 / 96 / 96 | 467,945,496 | yes |
| 2026-10-31 | Sat | 96 / 96 / 96 | 4,455,680 | 96 / 96 / 96 | 6,238,783 | 96 / 96 / 96 | 271,561,864 | yes |
| 2026-11-01 | Sun | 96 / 96 / 96 | 3,785,014 | 96 / 96 / 96 | 5,151,836 | 96 / 96 / 96 | 236,983,255 | yes |
| 2026-11-02 | Mon | 96 / 96 / 96 | 6,630,254 | 96 / 96 / 96 | 9,263,301 | 96 / 96 / 96 | 439,075,137 | yes |
| 2026-11-03 | Tue | 96 / 96 / 96 | 7,668,814 | 96 / 96 / 96 | 10,727,412 | 96 / 96 / 96 | 487,216,867 | yes |
| 2026-11-04 | Wed | 96 / 96 / 96 | 8,052,450 | 96 / 96 / 96 | 12,004,306 | 96 / 96 / 96 | 540,822,235 | yes |
| 2026-11-05 | Thu | 96 / 96 / 96 | 7,546,794 | 96 / 96 / 96 | 10,810,077 | 96 / 96 / 96 | 506,281,300 | yes |
| 2026-11-06 | Fri | 96 / 96 / 96 | 7,282,781 | 96 / 96 / 96 | 10,245,160 | 96 / 96 / 96 | 483,441,370 | yes |
| 2026-11-07 | Sat | 96 / 96 / 96 | 4,358,684 | 96 / 96 / 96 | 6,152,488 | 96 / 96 / 96 | 277,707,296 | yes |
| 2026-11-08 | Sun | 96 / 96 / 96 | 3,422,996 | 96 / 96 / 96 | 4,969,217 | 96 / 96 / 96 | 227,347,063 | yes |
| 2026-11-09 | Mon | 96 / 96 / 96 | 6,514,268 | 96 / 96 / 96 | 9,540,987 | 96 / 96 / 96 | 419,139,753 | yes |
| 2026-11-10 | Tue | 96 / 96 / 96 | 7,886,632 | 96 / 96 / 96 | 11,209,317 | 96 / 96 / 96 | 489,725,865 | yes |
| 2026-11-11 | Wed | 96 / 96 / 96 | 7,599,316 | 96 / 96 / 96 | 10,734,225 | 96 / 96 / 96 | 469,769,570 | yes |
| 2026-11-12 | Thu | 96 / 96 / 96 | 7,731,424 | 96 / 96 / 96 | 11,101,991 | 96 / 96 / 96 | 467,195,238 | yes |
| 2026-11-13 | Fri | 96 / 96 / 96 | 7,089,535 | 96 / 96 / 96 | 10,054,148 | 96 / 96 / 96 | 444,689,095 | yes |
| 2026-11-14 | Sat | 96 / 96 / 96 | 4,462,484 | 96 / 96 / 96 | 5,697,127 | 96 / 96 / 96 | 263,990,219 | yes |
| 2026-11-15 | Sun | 96 / 96 / 96 | 3,627,566 | 96 / 96 / 96 | 4,351,539 | 96 / 96 / 96 | 216,523,329 | yes |
| 2026-11-16 | Mon | 96 / 96 / 96 | 6,790,467 | 96 / 96 / 96 | 8,752,748 | 96 / 96 / 96 | 407,305,510 | yes |
| 2026-11-17 | Tue | 96 / 96 / 96 | 8,105,190 | 96 / 96 / 96 | 11,463,113 | 96 / 96 / 96 | 488,460,168 | yes |
| 2026-11-18 | Wed | 96 / 96 / 96 | 8,232,392 | 96 / 96 / 96 | 11,697,837 | 96 / 96 / 96 | 500,681,961 | yes |
| 2026-11-19 | Thu | 96 / 96 / 96 | 8,249,502 | 96 / 96 / 96 | 11,731,788 | 96 / 96 / 96 | 506,060,994 | yes |
| 2026-11-20 | Fri | 96 / 96 / 96 | 7,540,390 | 96 / 96 / 96 | 10,633,814 | 96 / 96 / 96 | 473,453,743 | yes |
| 2026-11-21 | Sat | 96 / 96 / 96 | 4,441,744 | 96 / 96 / 96 | 5,628,052 | 96 / 96 / 96 | 268,771,561 | yes |
| 2026-11-22 | Sun | 96 / 96 / 96 | 3,524,909 | 96 / 96 / 96 | 4,501,702 | 96 / 96 / 96 | 223,947,678 | yes |
| 2026-11-23 | Mon | 96 / 96 / 96 | 6,375,169 | 96 / 96 / 96 | 8,750,998 | 96 / 96 / 96 | 419,883,519 | yes |
| 2026-11-24 | Tue | 96 / 96 / 96 | 7,244,423 | 96 / 96 / 96 | 10,262,726 | 96 / 96 / 96 | 469,123,996 | yes |

## The two days after the window

The scheduled runs of `gdelt_silver_daily` built the two days which follow the window: 2026-11-25 on 2026-11-26, and 2026-11-26 on 2026-11-27 (status report, sections on the backfill and on the live run). The same script compared the two days on 2026-11-27 at 11:32 UTC. Raw result: `benchmarks/p1_backfill/results/quarantine-reconciliation-2026-11-25-to-2026-11-26.json`.

| Day | Feed | Files: ledger / bronze / day version | Bytes in the ledger and in bronze | Raw lines | Quarantined rows |
|---|---|---|---:|---:|---:|
| 2026-11-25 (Wed) | Events | 96 / 96 / 96 | 7,502,531 | 116,928 | 0 |
| 2026-11-25 (Wed) | Mentions | 96 / 96 / 96 | 10,087,462 | 291,859 | 0 |
| 2026-11-25 (Wed) | GKG | 96 / 96 / 96 | 476,465,708 | 112,593 | 0 |
| 2026-11-26 (Thu) | Events | 96 / 96 / 96 | 5,966,112 | 95,311 | 0 |
| 2026-11-26 (Thu) | Mentions | 96 / 96 / 96 | 7,582,551 | 230,139 | 0 |
| 2026-11-26 (Thu) | GKG | 96 / 96 / 96 | 371,588,765 | 88,072 | 0 |

The six pairs of feed and day agree. No slot of the two days is quarantined, and no quarantine file exists.

## The daily reconciliation job

The pipeline runs a separate reconciliation, apart from the script of the report. The task `reconcile` of the DAG `gdelt_ingest` runs once in each UTC day from 08:00 UTC. The task reads the master list of GDELT for the trailing seven days, lists each expected slot which the master list holds, counts each slot which the master list does not hold, and compares the bronze listing with the ledger. The comparison is recorded as the check `ledger_against_bronze` of gate G6 in `ops.dq_result`.

| Reconciliation (UTC) | Slots from … to | Files in the master list | Expected slots which were listed | Absent slots | Changed listings | Slots confirmed missing | Differences between bronze and the ledger |
|---|---|---:|---:|---:|---:|---:|---:|
| 2026-11-26 20:16, in the first run of the drain | 2026-11-19 20:15 to 2026-11-26 20:30 | 2,019 | 0 | 0 | 0 | 0 | 0 |
| 2026-11-27 08:08, scheduled | 2026-11-20 08:00 to 2026-11-27 08:15 | 2,022 | 27 | 0 | 0 | 0 | 0 |

Both checks of gate G6 passed with 0 differences. The 27 slots which the second reconciliation listed are files which the laptop missed while the laptop slept (status report, section on the live run); the ingest task of the same run landed the files.

## Limits of the report

- The comparison works on counts and bytes. The content of each file was checked once, at the download: gate G1 compares the size and the MD5 hash of the file with the file list of GDELT and opens the zip.
- The tables of the window show one moment: 2026-11-26 at 21:01 UTC. The reconciliation job of the pipeline reads the trailing seven days only. For the 23 older days of the window, the script of the report is the only comparison of bronze with the ledger.
