# The watchlist, version 0

The **watchlist** is the list of companies whose filings and financial facts the platform follows closely: the filings pipeline loads the submissions document of each company of the list each day, and the facts pipeline loads the financial facts of each company (blueprint §4.6; week 3 plan §3.3).

The list is one file, `dbt/seeds/watchlist.csv`. dbt reads the file as a seed, and the SEC pipelines read the first `GBMI_WATCHLIST_SIZE` rows of the same file: 50 on the working stack and on the AWS host, 5 on the demo stack.

| Column | Meaning |
|---|---|
| `rank` | The position in the list, from 1 without a gap |
| `cik` | The Central Index Key: the number which the SEC gives the company |
| `name_at_selection` | The name of the company in the ticker file of the SEC on the day of the draw |
| `reason` | Why the company is in the list |
| `added_on` | The day on which the company joined the list |

## How version 0 was drawn

`benchmarks/source_profile/watchlist_candidates.py` drew the list on 2026-11-28 from data which the platform held. The script sent no request. The raw result is `benchmarks/source_profile/results/watchlist-v0.json`.

| Ranks | Companies | Rule |
|---|---|---|
| 1 to 5 | 5 | The five demo companies of the week 3 plan. The recorded SEC files of the demo hold the documents of exactly the five |
| 6 to 18 | 13 | The other companies of the gold sheet `config/labelling/gold_sheet_v0.csv` with the label `company_in_universe`, in the order of the documents of the sheet |
| 19 to 50 | 32 | The companies of the universe whose name, without its legal form, is exactly an organisation string of the GDELT GKG files, by the number of documents from 2026-10-26 to 2026-11-24 |

The universe of the draw is the snapshot of 2026-11-28: 7,988 companies. 1,913 names of the universe stand as an organisation string in the GKG files of the 30 days.

Two more rules:

- **A company needs a periodic report.** A company joins the list only with an annual report (form 10-K, 20-F or 40-F) among the filings of its submissions document, or with a periodic report in a loaded index file. A company without such a report has no financial facts. All 50 companies of version 0 have an annual report in their document, and the live load of 2026-11-28 found a facts document for each of the 50.
- **No name which is a common word.** The count of news documents is an exact comparison of names. For a name which is also a common word, the count says little about the company.

## What was left out

| Company or name | CIK | GKG documents | Why |
|---|---|---|---|
| easyJet plc | 1470419 | 519 | no periodic report is known |
| SPACE EXPLORATION TECHNOLOGIES CORP | 1181412 | 0 | no periodic report is known |
| DEUTSCHE LUFTHANSA A G /FI | 1049724 | 0 | no periodic report is known |
| `shell` | 1306965 | 5988 | The name is a common word; the gold sheet labels the string 'shell' as ambiguous |
| `visa` | 1403161 | 5679 | The name is a common word: a visa is also a travel document |
| `intel` | 50863 | 4412 | The name is a common short form of the word intelligence |
| `oracle` | 1341439 | 3451 | The name is a common word |

## The limits of version 0

- The draw is a profile, not a matcher. Entity resolution arrives in phase P3. A company whose name in the news differs from its name at the SEC has no documents in the count: the name "Meta Platforms" stands in no organisation string, and the news says "Meta", "Facebook" or "Instagram".
- "The name is exactly an organisation string" prefers companies with a short, unique name.
- The list of common words is four names which were read in the result of a dry run. No dictionary was used.

## How to change the list

Change `dbt/seeds/watchlist.csv` by pull request.

1. Keep ranks 1 to 5: the demo and the tests read the five rows.
2. Keep the ranks from 1 without a gap, and each CIK once. `tests/unit/test_sec_watchlist.py` and the reader `gbmi.sources.sec.watchlist` refuse a file which breaks one of the two rules.
3. Take the CIK and the name from the newest snapshot of the universe (`sec_company_snapshot`). A CIK outside the ticker file of the SEC has no row in the snapshot.
4. After the merge and the next deployment, the filings pipeline loads the document of a new company on its next run, and the facts pipeline loads the facts of the company on its next run. A company without a facts document shows as `not_published` in `gbmi sec objects`: replace such a company.

A new draw with the script replaces the whole file: `make toolbox CMD="python -m benchmarks.source_profile.watchlist_candidates --write"`. A company which stays in the list keeps its `added_on`.
