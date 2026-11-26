# GDELT fixtures

The files under `v2/` are the twelve files of one hour of the GDELT 2.0 feeds (English): the four 15-minute slots from 2026-11-25 08:00 to 08:45 UTC, for the three feeds Events (`export`), Mentions (`mentions`) and Global Knowledge Graph (`gkg`).

**Source and terms.** The GDELT Project, <https://www.gdeltproject.org/>. GDELT makes its data available for unlimited and unrestricted use, and permits redistribution, provided that the GDELT Project is cited and linked (<https://www.gdeltproject.org/about.html#termsofuse>).

## Events: `v2/export/`, unchanged

The four files are exactly as the GDELT servers delivered them on 2026-11-25.

| File | Bytes | MD5 | Rows |
|---|---|---|---|
| `20261125080000.export.CSV.zip` | 81,069 | `975f7f000c2774fc12108a1d06d34c7a` | 1,212 |
| `20261125081500.export.CSV.zip` | 47,061 | `93b152dd0eca74bf2ec82a4697d18451` | 742 |
| `20261125083000.export.CSV.zip` | 58,856 | `d9aeb7b4cda9a6c69505e04305ffbb30` | 913 |
| `20261125084500.export.CSV.zip` | 76,194 | `ef823dc41a6cd740f4ccdb42cf144b26` | 1,156 |

Content: event records only, with coded actors, CAMEO action codes, locations, tone and the URL of one source article. No article text is included.

## Mentions: `v2/mentions/`, unchanged

The four files are exactly as the GDELT servers delivered them on 2026-11-26.

| File | Bytes | MD5 | Rows |
|---|---|---|---|
| `20261125080000.mentions.CSV.zip` | 80,526 | `22fe3f8483c5700de99aba701b3f9432` | 2,091 |
| `20261125081500.mentions.CSV.zip` | 63,095 | `9a46a052f1ecdf79b9d9f5ac767262ca` | 1,898 |
| `20261125083000.mentions.CSV.zip` | 71,167 | `faa8000b31dba4f9a856dcfbe1b46e9a` | 1,970 |
| `20261125084500.mentions.CSV.zip` | 73,457 | `3d815841060b9f69455eb00b9fec0581` | 1,923 |

Content: one row for each mention of an event in an article, with the name of the source, the URL of the article, character offsets, a confidence value and the tone of the document. No article text is included.

## Global Knowledge Graph: `v2/gkg/`, shortened

**The four GKG files are not the files which GDELT serves.** A real GKG file of the hour has between 3 and 4 MB. Each fixture file holds the first 60 records of the real file, unchanged byte for byte, in a new zip with the same member name. The script checked the size and the MD5 of each real file against the master file list of GDELT before the script shortened the file.

| File | Bytes | MD5 of the shortened file | Records |
|---|---|---|---|
| `20261125080000.gkg.csv.zip` | 259,082 | `24a93eb01bb466955e7c831a8953f044` | 60 |
| `20261125081500.gkg.csv.zip` | 224,377 | `bb57c3f5cad78b9e8134f2756b88fe5c` | 60 |
| `20261125083000.gkg.csv.zip` | 244,128 | `a94f68f0ec71df4fa88dc9fa321e8627` | 60 |
| `20261125084500.gkg.csv.zip` | 211,392 | `191d5baa3b99461388e500e50ea52dc2` | 60 |

Content: one record for each article, with the names of organisations, persons, themes and locations which GDELT found, tone values, the URL and the title of the article. No article text is included.

## The two file lists

| File | Content |
|---|---|
| `v2/masterfilelist.txt` | One line for each of the twelve files, in the form and in the order of the master file list of GDELT: size in bytes, MD5 and URL. The eight lines for Events and Mentions are the lines of GDELT, verbatim. The four lines for GKG give the size and the MD5 of the shortened files, so the fixtures pass the same check as real files |
| `v2/lastupdate.txt` | The three lines of the newest slot of the hour (08:45), as `lastupdate.txt` of GDELT shows the newest slot |

## How the files were recorded

Events on 2026-11-25, Mentions and GKG on 2026-11-26, with

```bash
uv run python benchmarks/source_profile/record_gdelt_fixtures.py --hour 2026-11-25T08 --feeds mentions,gkg --gkg-records 60
```

The script fetches each file from the live source, checks the file with the gate of the pipeline (size, MD5, the zip opens) and writes the files and the two lists.
