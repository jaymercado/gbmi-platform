{#-
    The names of a company before the first snapshot, from the former names of EDGAR
    (blueprint 7.10; week 3 plan, WP-08 step 4).

    The SEC gives each former name two dates: the first and the last use of the name. The rule:

      1. The former names of a company are ordered by the first date.
      2. A former name starts on its first date and ends on the first date of the next name.
      3. The last former name ends on its own last date.
      4. The name of the first snapshot (the "current" name) starts on the last date of the
         last former name and ends on the first snapshot date.

    The end date itself is not part of an interval, so the intervals of a company follow each
    other without a gap and without an overlap, and the last one ends where the snapshot
    intervals start. Every range of the rule is one row. range_status says whether the range is
    a validity interval:

      kept                  the range holds one day or more before the first snapshot date
      ends_before_start     the end of the range lies before the start (dates of the SEC)
      empty                 the end is the start: the range holds no day
      after_first_snapshot  the range holds no day before the first snapshot date; the
                            snapshots describe the days from the first snapshot date on

    dim_company takes the rows with the status kept. The other rows stay in the model, so one
    query counts the dropped ranges.
-#}
with first_snapshots as (
    select
        cik,
        min(valid_from) as first_snapshot_date,
        argMin(name, valid_from) as first_snapshot_name
    from {{ ref('int_company__snapshot_versions') }}
    group by cik
),

former_names as (
    select
        names.cik,
        names.former_name as name,
        names.name_from,
        -- A former name without a last date is taken to hold until the first snapshot date.
        coalesce(names.name_to, first_snapshots.first_snapshot_date) as name_to,
        first_snapshots.first_snapshot_date,
        first_snapshots.first_snapshot_name
    from {{ ref('stg_sec__company_former_name') }} as names
    inner join first_snapshots on names.cik = first_snapshots.cik
),

ordered as (
    select
        cik,
        name,
        name_from,
        name_to,
        first_snapshot_date,
        first_snapshot_name,
        row_number() over company_names as position,
        count() over (partition by cik) as names_of_company,
        leadInFrame(name_from, 1, name_from) over company_names as next_name_from
    from former_names
    window company_names as (
        partition by cik
        order by name_from, name_to, name
        rows between unbounded preceding and unbounded following
    )
),

ranges as (
    -- Rules 2 and 3: one range for each former name.
    select
        cik,
        name,
        'former' as name_kind,
        name_from as range_start,
        if(position = names_of_company, name_to, next_name_from) as range_end,
        first_snapshot_date
    from ordered

    union all

    -- Rule 4: the name of the first snapshot, from the end of the last former name on. When
    -- the last former name ends before its start, the current name starts on that start, so
    -- the ranges of a company never overlap.
    select
        cik,
        first_snapshot_name as name,
        'current' as name_kind,
        greatest(name_to, name_from) as range_start,
        first_snapshot_date as range_end,
        first_snapshot_date
    from ordered
    where position = names_of_company
)

select
    cik,
    name,
    name_kind,
    range_start as valid_from,
    -- A kept range ends on the first snapshot date at the latest.
    least(range_end, first_snapshot_date) as valid_to,
    range_end,
    first_snapshot_date,
    multiIf(
        range_end < range_start and name_kind = 'former', 'ends_before_start',
        range_end = range_start and name_kind = 'former', 'empty',
        least(range_end, first_snapshot_date) <= range_start, 'after_first_snapshot',
        'kept'
    ) as range_status
from ranges
