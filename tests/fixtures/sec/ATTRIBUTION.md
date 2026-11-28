# Recorded SEC files

The files in the folder are shortened copies of public files of the EDGAR system of the U.S.
Securities and Exchange Commission (https://www.sec.gov/). The project is not affiliated
with, endorsed by or sponsored by the SEC. `make demo`, the tests and CI read the files in
place of the live source (week 3 plan, section 3.3), so no test sends a request to the SEC.

## How the files were recorded

The step `fetch` of `benchmarks/source_profile/record_sec_fixtures.py` sent
18 requests through the fair-access client
`gbmi.sources.sec.client.SecClient` on 2026-11-28, from 04:37:51 to
04:37:58 UTC, with the declared user agent of the owner of the project. The request
log of the run (`ops.source_request`, run `record-sec-fixtures-20261128T043750Z`) shows at most 4
requests in any window of one second by the wall clock, and at most 4 by the monotonic
clock of the client. The outcomes of the requests: 2 `missing`, 16 `ok`.

The raw answers are not part of the repository. The step `shorten` of the same script wrote
the files of the folder from the raw answers, without a network.

| Requested address | Status | Bytes of the raw answer | MD5 of the raw answer |
|---|---|---|---|
| `https://www.sec.gov/files/company_tickers_exchange.json` | 200 | 521,932 | `753091670980ca331f680fa8c0fe6c54` |
| `https://data.sec.gov/submissions/CIK0001326801.json` | 200 | 157,103 | `e9531bbc7c9421c54d9b2615672b0224` |
| `https://data.sec.gov/submissions/CIK0001045810.json` | 200 | 160,578 | `c222af3d9f08f972c5753c3b902f4887` |
| `https://data.sec.gov/submissions/CIK0000789019.json` | 200 | 185,316 | `47e8afb028f0815fb6fb153d08e75297` |
| `https://data.sec.gov/submissions/CIK0000063276.json` | 200 | 164,775 | `61e05402528cfe4958d350ec6bbfbc71` |
| `https://data.sec.gov/submissions/CIK0001075124.json` | 200 | 160,643 | `e7d66477a24aae328b9309b9b5c931a4` |
| `https://data.sec.gov/submissions/CIK0001470419.json` | 200 | 2,585 | `d8bc592afa706fdf3fb7ec36cc2e88c1` |
| `https://data.sec.gov/submissions/CIK0001652044.json` | 200 | 152,430 | `a88f50f7e1b8ba682f260f71b18148d9` |
| `https://data.sec.gov/submissions/CIK0000046080.json` | 200 | 163,706 | `f02f57aaafbac8a1a9fc3d65944a81a7` |
| `https://data.sec.gov/api/xbrl/companyfacts/CIK0001326801.json` | 200 | 2,810,958 | `8d510f765257eb36aa45da662322ba9f` |
| `https://data.sec.gov/api/xbrl/companyfacts/CIK0001045810.json` | 200 | 4,092,966 | `4734230cdbf4e1c2e69488e5bd8298c2` |
| `https://data.sec.gov/api/xbrl/companyfacts/CIK0000789019.json` | 200 | 4,938,072 | `1aabf8c8880bab032bb98a933f916418` |
| `https://data.sec.gov/api/xbrl/companyfacts/CIK0000063276.json` | 200 | 4,217,915 | `cf80c465ce0059e5801ffe01f035ba8f` |
| `https://data.sec.gov/api/xbrl/companyfacts/CIK0001075124.json` | 200 | 1,232,579 | `d7566c7fa0a976c71f96f6ae3849b60f` |
| `https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/index.json` | 200 | 26,141 | `04ecb216cf6b8c404d4ef8e7e86b6b1e` |
| `https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/form.20261125.idx` | 200 | 1,145,702 | `ebb9958dd98b9c056aeb10b5f90ad911` |
| `https://data.sec.gov/api/xbrl/companyfacts/CIK0001470419.json` | 404 | 331 | `ec6b9875779f50c02c66759f6c1ff647` |
| `https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/form.20261126.idx` | 403 | 263 | `dd998bbeaeb1ca992d5b2c66273f18c4` |

## How the files were shortened

- **Ticker file:** the rows of ten CIKs: the five demo companies (Meta Platforms, Nvidia, Microsoft, Mattel, Thomson Reuters), three more
  companies with a submissions document (easyJet, Alphabet, Hasbro), and two companies without any recorded
  document (Amazon, Netflix).
- **Submissions documents** (eight): every top-level field as recorded. Of `filings.recent`:
  the 40 newest filings, every filing of the forms 10-K, 10-Q, 40-F, 8-K of the two years
  before 2026-11-25, and every filing which the recorded index file names.
- **Facts documents** (five): the concepts of `dbt/seeds/metric_concept_map.csv` and
  5 more concepts for each company (table below), each with every fact.
- **Listing of the quarter:** the entries of 2026-11-25: the recorded index file
  and the other files of the day, which the platform does not read.
- **Index file `form.20261125.idx`:** the header and about 300 rows: the rows of the ten
  companies, three filings with several filers, every row with an earlier date, two repeated
  pairs of accession number and CIK, the five rows with the longest form names, and rows in
  file order up to the total.
- **`submissions.zip`:** built from the eight shortened submissions documents.
- **`refusals/`:** the bodies of the two "no such object" answers: HTTP 404 on
  `data.sec.gov` (no facts document) and HTTP 403 below `/Archives/` (no index file for a US
  holiday). The request identifier and the host identifier of each body are replaced by the
  letter X in the same length: the two values name one request of the recording.

| Facts document | The 5 concepts beside the concepts of the metric seed |
|---|---|
| `CIK0001326801.json` | `ecd:CoSelectedMeasureAmt`, `us-gaap:CostsAndExpenses`, `us-gaap:EarningsPerShareBasic`, `us-gaap:EarningsPerShareDiluted`, `us-gaap:IncomeTaxExpenseBenefit` |
| `CIK0001045810.json` | `ecd:CoSelectedMeasureAmt`, `us-gaap:CostOfRevenue`, `us-gaap:EarningsPerShareBasic`, `us-gaap:EarningsPerShareDiluted`, `us-gaap:GrossProfit` |
| `CIK0000789019.json` | `us-gaap:CommonStockDividendsPerShareDeclared`, `us-gaap:DividendsCommonStockCash`, `us-gaap:EarningsPerShareBasic`, `us-gaap:EarningsPerShareDiluted`, `us-gaap:GrossProfit` |
| `CIK0000063276.json` | `ecd:CoSelectedMeasureAmt`, `us-gaap:EarningsPerShareBasic`, `us-gaap:EarningsPerShareDiluted`, `us-gaap:GrossProfit`, `us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding` |
| `CIK0001075124.json` | `ifrs-full:AdjustedWeightedAverageShares`, `ifrs-full:AdjustmentsForDecreaseIncreaseInOtherCurrentAssets`, `ifrs-full:AdjustmentsForDecreaseIncreaseInTradeAndOtherReceivables`, `ifrs-full:AdjustmentsForDeferredTaxExpense`, `ifrs-full:AdjustmentsForDepreciationExpense` |

## The files of the folder

| File | Bytes | MD5 |
|---|---|---|
| `www.sec.gov/files/company_tickers_exchange.json` | 751 | `7537e1fc271aa8aa08097df18c616040` |
| `data.sec.gov/submissions/CIK0001326801.json` | 12,385 | `54340eb7d7b8af62ab55c0fe8f728093` |
| `data.sec.gov/submissions/CIK0001045810.json` | 11,525 | `ad3dcdc45f8d124ecd9cd565c99ae5d9` |
| `data.sec.gov/submissions/CIK0000789019.json` | 12,482 | `fd5eb196c944e58ebdbe4b7b6200c04e` |
| `data.sec.gov/submissions/CIK0000063276.json` | 10,826 | `72c9df3d2e47a9c0f5952b5b74363257` |
| `data.sec.gov/submissions/CIK0001075124.json` | 8,056 | `39a8e641a4f3d737c15b69d537abcecf` |
| `data.sec.gov/submissions/CIK0001470419.json` | 2,585 | `d8bc592afa706fdf3fb7ec36cc2e88c1` |
| `data.sec.gov/submissions/CIK0001652044.json` | 13,163 | `396b69c7cd3216d95c0a65f3aef1bbc8` |
| `data.sec.gov/submissions/CIK0000046080.json` | 11,003 | `8956bda1b615326b14acb191323ce140` |
| `data.sec.gov/api/xbrl/companyfacts/CIK0001326801.json` | 299,131 | `cce136051fe3ae3727b5adbc6b308960` |
| `data.sec.gov/api/xbrl/companyfacts/CIK0001045810.json` | 402,166 | `f967f6cd082425e74ff4c90d3dfb75ff` |
| `data.sec.gov/api/xbrl/companyfacts/CIK0000789019.json` | 520,299 | `4ab1ca4b3a22d40bcba338dd3a85bfce` |
| `data.sec.gov/api/xbrl/companyfacts/CIK0000063276.json` | 461,096 | `3f9e202e0395c11bf9b9f0a76c8708b6` |
| `data.sec.gov/api/xbrl/companyfacts/CIK0001075124.json` | 89,170 | `570327d0c5bc8b97b4ed6c24ad03d056` |
| `www.sec.gov/Archives/edgar/daily-index/2026/QTR4/index.json` | 735 | `52bb3ec635e780340686dfb101fd0d64` |
| `www.sec.gov/Archives/edgar/daily-index/2026/QTR4/form.20261125.idx` | 58,903 | `801233317f1d3737c68c171802423bcd` |
| `www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip` | 21,226 | `5f2a98c2b2f09b1bc42a8239ff18a93d` |
| `refusals/data.sec.gov-404.xml` | 331 | `3a740206647665907c51cae6e365f297` |
| `refusals/www.sec.gov-archives-403.xml` | 263 | `67c843383f11d9ab04860038a2b1b5c3` |
