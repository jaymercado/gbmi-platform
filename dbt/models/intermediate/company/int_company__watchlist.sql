{#-
    The watchlist of the stack: the first rows of the seed, in the order of the rank. The SEC
    pipelines read the same file and the same size (gbmi.sources.sec.watchlist), so the models
    and the pipelines follow the same companies.
-#}
select
    rank,
    cik,
    name_at_selection,
    reason,
    added_on
from {{ ref('watchlist') }}
where rank <= {{ watchlist_size() }}
