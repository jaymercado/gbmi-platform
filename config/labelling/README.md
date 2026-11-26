# Labelling sheets for the entity-resolution gold set

Entity resolution is the centrepiece of the platform, and its precision is only as credible as the labelled sample it is measured on (blueprint §8.8). The first sheet was drawn and labelled in week 1, so that 200 labels exist when phase P3 needs them (risk R25).

## `gold_sheet_v0.csv`

| | |
|---|---|
| Drawn by | `benchmarks/source_profile/sample_org_strings.py`. A new draw, written to another path with `--out`, reproduces the first eight columns byte for byte. The script does not overwrite a sheet with labels |
| Source | Eight GDELT 2.0 GKG slots, 2026-11-25 08:00–09:45 UTC, each verified against the size and MD5 in the master file list |
| Population | 7,549 records; 5,612 (74.3%) name at least one organisation; 7,650 distinct organisation strings |
| Sample | 200 strings: the 100 with the most documents (stratum `top`, 20 to 461 documents each) and 100 drawn at random from the rest with seed 20261125 (stratum `random`) |
| Shown to the labeller | The string exactly as GDELT gives it, the number of documents that mention it, and up to five article titles |
| Labelled | All 200 strings, on 2026-11-26 — see [The labels of 2026-11-26](#the-labels-of-2026-11-26) |

The sheet is **blind by construction**: no matching system exists yet, so nothing but the string and the titles can influence a label. The strata of blueprint §8.8 (ambiguous short names, subsidiaries and brands, non-companies) are drawn in phase P3 by the real `gbmi er sample-gold` command; these 200 labels count towards the MVP's 200.

## How to label

Fill in three columns. Leave the others untouched.

| Column | Value |
|---|---|
| `label` | One of the four labels below |
| `cik` | The SEC Central Index Key, digits only, when the label is `company_in_universe`; the most likely candidate when it is `ambiguous`; otherwise empty |
| `note` | Free text: anything that made the decision hard |

| Label | Use it when |
|---|---|
| `company_in_universe` | The string names one company of the universe: a company which the SEC ticker list holds, with a CIK and at least one ticker. Most universe companies are listed on a US exchange |
| `company_not_in_universe` | The string names a company which the SEC ticker list does not hold: private, listed only abroad, or defunct |
| `not_a_company` | Anything else: a country, government body, court, university, charity, sports team, place, person or parsing error |
| `ambiguous` | The string could be a universe company or something else depending on the article — typically a short name that is also a common word |

Rules:

1. **Judge the string, using the titles only as context.** The label applies to the string as a whole, across all the documents that use it.
2. **Look up the CIK by hand** in EDGAR's company search (<https://www.sec.gov/edgar/searchedgar/companysearch>). No automated SEC request is made in week 1.
3. **Brands, products and subsidiaries of a listed company:** label `company_in_universe`, give the *parent's* CIK, and start the note with `brand:` or `subsidiary:`. Whether such strings should count as matches is decided in phase P3 (ADR-008); the note keeps both choices open without relabelling.
4. **If you cannot decide in about a minute**, choose `ambiguous` and say why in the note. An honest "ambiguous" is worth more than a guessed label.
5. **A foreign company with only an over-the-counter ticker** in the SEC ticker list belongs to the universe. Label the string `company_in_universe` and write `otc:` in the note, after `brand:` or `subsidiary:` when rule 3 applies too. Phase P1 decides how the universe treats such companies; the marker keeps both choices open without relabelling.
6. **What counts as a company:** an organisation whose main activity is to sell goods or services, whoever owns the organisation. A cooperative, a law firm, a hotel and a not-for-profit news agency count. A public body, a state news agency, a university, a hospital, a charity, an association, a political party, a church, a club, a venue and an airport are `not_a_company`.
7. **Cut and joined strings.** GDELT cuts some names and joins others. A cut name gets the label of the organisation which the cut name belongs to, and the note starts with `truncated:` (after `brand:` when rule 3 applies). A string which joins several names, or which is no name, is `not_a_company`; the note starts with `parsing error:` or `generic:`.

## The labels of 2026-11-26

| | |
|---|---|
| Labeller | Claude, an AI model made by Anthropic, working in Claude Code. Jay handed the labelling over on 2026-11-26 (week 1 plan, D14). No person has reviewed the labels |
| Scope | All 200 strings, in one session |
| Blind | Yes: no matching system existed. The labeller read the string, the document count and the titles |
| Universe and CIKs | A copy of the SEC ticker list: the file `company_tickers.parquet` of the open-source library [edgartools](https://github.com/dgunning/edgartools), refreshed from the SEC on 2026-10-22, with 10,443 tickers and 8,029 CIKs. Each of the 200 strings was searched in the copy, and each CIK was read from the copy. No request went to the SEC (week 1 plan, D7), so the search in the copy took the place of rule 2 |
| Web searches | Twelve names, on 2026-11-26: the owners of CNN, Twitter, ESPN and the Eastern Daily Press, and the identity of Amazon Autos, Global Airlines, Rembrandt Holdings, Scandesor Development, Getaway Negril Hotel, Foundation For Operational Forecasting, Green Ideas Technology and Origin Sciences |

| Label | `top` | `random` | Both |
|---|---|---|---|
| `company_in_universe` | 23 | 2 | 25 |
| `company_not_in_universe` | 7 | 18 | 25 |
| `not_a_company` | 70 | 79 | 149 |
| `ambiguous` | 0 | 1 | 1 |

**A label states the facts of the labelling day.** CNN belongs to Skydance Corp only since 2026-10-06, and Twitter belongs to a listed company only since SpaceX went public in 2026. Whoever uses the gold set compares each CIK with the universe of the day of use.

**Limits**

- **One labeller, and the labeller is an AI model.** The sheet has no second labeller, so the agreement between two labellers cannot be measured. A review by a person is open: the 26 rows with a CIK and a random sample of the other rows, before the precision gate of phase P3 is treated as binding.
- **The copy of the SEC ticker list is 35 days older than the labels.** Phase P1 loads the SEC reference data. The CIKs of the sheet have to be compared with the SEC reference data then.
- **The line between `company_not_in_universe` and `not_a_company` is a judgement** for news agencies, hotels and hospitals (rule 6). The line does not change precision or recall, because neither label is a match.

## What the labels show

| Finding | Numbers | Matters for |
|---|---|---|
| Most strings do not name a company | 149 of 200. In the `random` stratum, 3 of 100 strings can belong to a universe company | The matcher rejects far more strings than the matcher accepts (P3) |
| Half of the universe strings are brands or subsidiaries | 13 of 25, for example Instagram, YouTube, CNN, Reuters and Cessna. The company names in SEC data do not contain the brand names | Curated aliases and ADR-008 (P3) |
| The SEC ticker list holds companies with only an over-the-counter ticker | 1,795 of the 8,029 CIKs in the copy. easyJet and Lufthansa are in the list; Hyundai, Subaru and Fujitsu are not | The definition of the universe (P1) |
| Owners change | Two of the 25 universe strings changed owner or listing in 2026: CNN and Twitter | Aliases and labels need a date (P3) |
| A few syndicated articles fill the `top` stratum | 30 of the 100 `top` strings show one title only. Seven articles with many copies produce the 30 strings, and one list of college campuses produces 19 of the 30 | Story collapsing before document counts are trusted (P3) |
| GDELT cuts names and joins names | 23 strings are cut names, 16 are joined names or page text, and 16 are generic words | Normalisation and the negative list (P3) |

## Attribution

Organisation strings and article titles come from [The GDELT Project](https://www.gdeltproject.org/). Titles are metadata; no article text is stored. Company names, tickers and CIKs come from the [U.S. Securities and Exchange Commission](https://www.sec.gov/) (EDGAR), through the copy named above.
