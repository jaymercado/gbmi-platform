# GDELT fixtures

The files under `v2/export/` are four slots of the GDELT 2.0 Events feed (English), exactly as the GDELT servers delivered them on 2026-11-25:

| File | Bytes | MD5 | Rows |
|---|---|---|---|
| `20261125080000.export.CSV.zip` | 81,069 | `975f7f000c2774fc12108a1d06d34c7a` | 1,212 |
| `20261125081500.export.CSV.zip` | 47,061 | `93b152dd0eca74bf2ec82a4697d18451` | 742 |
| `20261125083000.export.CSV.zip` | 58,856 | `d9aeb7b4cda9a6c69505e04305ffbb30` | 913 |
| `20261125084500.export.CSV.zip` | 76,194 | `ef823dc41a6cd740f4ccdb42cf144b26` | 1,156 |

`v2/masterfilelist.txt` holds their four lines of GDELT's master file list, verbatim; sizes and checksums above are taken from it and are verified whenever the files are landed.

**Source and terms.** The GDELT Project, <https://www.gdeltproject.org/>. GDELT makes its data available for unlimited and unrestricted use, and permits redistribution, provided that the GDELT Project is cited and linked (<https://www.gdeltproject.org/about.html#termsofuse>).

**Content.** Event records only: coded actors, CAMEO action codes, locations, tone and the URL of one source article. No article text is included.

**How they were recorded.** `uv run python benchmarks/source_profile/record_gdelt_fixtures.py --hour 2026-11-25T08`, which lands the slots from the live source with the pipeline's own code and copies the verified bytes here.
