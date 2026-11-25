{#-
    One row per GDELT event, as silver holds it: renamed and cast, nothing else.
    slot_date becomes a Date (Iceberg dates arrive as Date32), which keeps mart partition
    names readable and the column two bytes wide.
-#}
select
    global_event_id,
    toDate(slot_date) as slot_date,
    slot_hour,
    slot_ts,
    event_date,
    actor1_code,
    actor1_name,
    actor1_country_code,
    actor1_type1_code,
    actor2_code,
    actor2_name,
    actor2_country_code,
    actor2_type1_code,
    is_root_event,
    event_code,
    event_base_code,
    event_root_code,
    quad_class,
    goldstein_scale,
    num_mentions,
    num_sources,
    num_articles,
    avg_tone,
    action_geo_type,
    action_geo_full_name,
    action_geo_country_code,
    date_added,
    source_url,
    _source_file,
    _slot_ts,
    _ingested_at,
    _run_id,
    _pipeline_version
from {{ source('lake', 'gdelt_event') }}
