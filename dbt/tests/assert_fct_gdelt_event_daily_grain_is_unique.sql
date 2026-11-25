-- The declared grain of fct_gdelt_event_daily holds: no combination of day, country, root
-- code and actor type appears twice. The test fails if this query returns a row.
select
    date,
    action_geo_country_code,
    event_root_code,
    actor1_type1_code,
    count() as occurrences
from {{ ref('fct_gdelt_event_daily') }}
group by
    date,
    action_geo_country_code,
    event_root_code,
    actor1_type1_code
having occurrences > 1
