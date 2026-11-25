# Labelling sheets for the entity-resolution gold set

Entity resolution is the centrepiece of the platform, and its precision is only as credible as the labelled sample it is measured on (blueprint §8.8). Labelling starts in week 1 — about 20 strings a day — so that 200 labels exist when phase P3 needs them (risk R25).

## `gold_sheet_v0.csv`

| | |
|---|---|
| Drawn by | `benchmarks/source_profile/sample_org_strings.py` (rerunning it reproduces the file byte for byte) |
| Source | Eight GDELT 2.0 GKG slots, 2026-11-25 08:00–09:45 UTC, each verified against the size and MD5 in the master file list |
| Population | 7,549 records; 5,612 (74.3%) name at least one organisation; 7,650 distinct organisation strings |
| Sample | 200 strings: the 100 with the most documents (stratum `top`, 20 to 461 documents each) and 100 drawn at random from the rest with seed 20261125 (stratum `random`) |
| Shown to the labeller | The string exactly as GDELT gives it, the number of documents that mention it, and up to five article titles |

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
| `company_in_universe` | The string names one company that is a US-listed SEC registrant with a ticker |
| `company_not_in_universe` | The string names a company that is not in that universe: private, listed only abroad, or defunct |
| `not_a_company` | Anything else: a country, government body, court, university, charity, sports team, place, person or parsing error |
| `ambiguous` | The string could be a universe company or something else depending on the article — typically a short name that is also a common word |

Rules:

1. **Judge the string, using the titles only as context.** The label applies to the string as a whole, across all the documents that use it.
2. **Look up the CIK by hand** in EDGAR's company search (<https://www.sec.gov/edgar/searchedgar/companysearch>). No automated SEC request is made in week 1.
3. **Brands, products and subsidiaries of a listed company:** label `company_in_universe`, give the *parent's* CIK, and start the note with `brand:` or `subsidiary:`. Whether such strings should count as matches is decided in phase P3 (ADR-008); the note keeps both choices open without relabelling.
4. **If you cannot decide in about a minute**, choose `ambiguous` and say why in the note. An honest "ambiguous" is worth more than a guessed label.

## Attribution

Organisation strings and article titles come from [The GDELT Project](https://www.gdeltproject.org/). Titles are metadata; no article text is stored.
