# Data dictionary

The data dictionary of the platform is the dbt documentation site:

**<https://jaymercado.github.io/gbmi-platform/>**

The address is the one which the GitHub API returns for the Pages site of the repository (`gh api repos/jaymercado/gbmi-platform/pages --jq .html_url`).

| | |
|---|---|
| Built from | The dbt project in `dbt/`, with `make docs` on the demo stack (`dbt docs generate --static`: one file) |
| Published by | The job `pages` of the workflow `e2e`, on each push to the branch `main` (week 3 plan, WP-14) |
| What the site shows | Each model, source and seed with its description; each column with its type and its description; the tests of a model; the SQL of a model as written and as compiled; the lineage graph |
| The data behind the site | The recorded files of the demo. The column types of the catalog come from the ClickHouse of the demo stack in CI, not from the AWS host |
| Secrets | The site is public. `make docs` searches the site file for every secret of the stack and fails on a hit (`tools/site_check.py`) |

## What the site holds

19 models, 2 seeds and 6 sources on 2026-11-28.

| Layer | Models |
|---|---|
| Sources (the Iceberg tables of the lake, pinned in ClickHouse) | `lake.gdelt_event`, `lake.sec_company_snapshot`, `lake.sec_company_former_name`, `lake.sec_filing_index`, `lake.sec_filing_detail`, `lake.sec_xbrl_fact` |
| Seeds | `metric_concept_map`, `watchlist` |
| Staging | `stg_gdelt__event`, `stg_ops__source_slot`, `stg_sec__company_snapshot`, `stg_sec__company_former_name`, `stg_sec__filing_index`, `stg_sec__filing_detail`, `stg_sec__xbrl_fact` |
| Intermediate | `int_company__watchlist`, `int_company__snapshot_versions`, `int_company__former_name_versions`, `int_financials__metric_facts`, `int_financials__quarters` |
| Marts | `dim_company`, `fct_filing`, `fct_filing_item`, `fct_financial_fact`, `fct_financial_metric`, `fct_gdelt_event_daily` |
| Operations | `ops_slot_status_daily` |

The five marts of phase P2 are `dim_company`, `fct_filing`, `fct_filing_item`, `fct_financial_fact` and `fct_financial_metric`. The contract of each silver table behind the sources is a file in `docs/contracts/`.

## Build the site on the laptop

```bash
make demo    # builds the models on the demo stack
make docs    # writes site/index.html and searches the file for secrets
open site/index.html
```
