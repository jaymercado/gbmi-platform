"""ClickHouse dialect checks (blueprint 17.3, assumption 16; week 1 plan, WP-07).

The blueprint's illustrative SQL was written before any ClickHouse was running. Three of its
statements are run here on ClickHouse 26.8 with inline data, so that the syntax the marts will
rely on in later phases is known to work - or the corrected form is recorded:

* the type-2 interval query with ``lagInFrame`` and ``leadInFrame`` (blueprint 7.10)
* the as-of query with ``argMax(value, (known_at, accession_number))`` (blueprint 7.10)
* the array expressions of the data mining layer (blueprint 8.12)
"""

from __future__ import annotations

from datetime import date

import pytest
from clickhouse_connect.driver.client import Client

pytestmark = pytest.mark.integration

# One company whose attributes go A, A, B, B, A, A over six daily snapshots.
SNAPSHOTS = """
    select 320193 as cik, toDate32(snapshot_date) as snapshot_date, attr_hash, name
    from values(
        'snapshot_date String, attr_hash String, name String',
        ('2026-01-01', 'A', 'Alpha Inc'), ('2026-01-02', 'A', 'Alpha Inc'),
        ('2026-01-03', 'B', 'Beta Inc'), ('2026-01-04', 'B', 'Beta Inc'),
        ('2026-01-05', 'A', 'Alpha Inc'), ('2026-01-06', 'A', 'Alpha Inc')
    )
"""

TYPE_2 = """
    with snapshots as ({snapshots}),
    changes as (
        select cik, snapshot_date, attr_hash, name,
               lagInFrame(attr_hash, 1, '') over w as prev_hash
        from snapshots
        window w as ({window})
    ),
    versions as (
        select * from changes where prev_hash = '' or prev_hash != attr_hash
    )
    select cik, name,
           snapshot_date as valid_from,
           leadInFrame(snapshot_date, 1, {open_end}) over w as valid_to
    from versions
    window w as ({window})
    order by valid_from
"""

EXPLICIT_FRAME = (
    "partition by cik order by snapshot_date "
    + "rows between unbounded preceding and unbounded following"
)
DEFAULT_FRAME = "partition by cik order by snapshot_date"
#: The corrected open-ended sentinel: Iceberg dates arrive as Date32, which holds 9999-12-31.
OPEN_END = "toDate32('9999-12-31')"


def test_type_2_intervals_a_b_a_give_three_intervals(clickhouse: Client) -> None:
    """A, B and back to A must give three validity intervals, not two (blueprint 7.10)."""
    query = TYPE_2.format(snapshots=SNAPSHOTS, window=EXPLICIT_FRAME, open_end=OPEN_END)

    rows = clickhouse.query(query).result_rows

    assert rows == [
        (320193, "Alpha Inc", date(2026, 1, 1), date(2026, 1, 3)),
        (320193, "Beta Inc", date(2026, 1, 3), date(2026, 1, 5)),
        (320193, "Alpha Inc", date(2026, 1, 5), date(9999, 12, 31)),
    ]


def test_type_2_default_frame_is_wrong(clickhouse: Client) -> None:
    """The blueprint's warning holds: with the default frame, leadInFrame sees no later row."""
    query = TYPE_2.format(snapshots=SNAPSHOTS, window=DEFAULT_FRAME, open_end=OPEN_END)

    rows = clickhouse.query(query).result_rows

    assert [row[3] for row in rows] == [date(9999, 12, 31)] * 3


def test_open_ended_sentinel_needs_date32(clickhouse: Client) -> None:
    """Correction to the blueprint's SQL: toDate('9999-12-31') silently becomes 2149-06-06.

    A ClickHouse Date ends at 2149-06-06 and saturates without an error. A Date32 - the type
    Iceberg dates arrive as - holds 9999-12-31 on ClickHouse 26.8, so the open end of a
    validity interval is written as toDate32('9999-12-31').
    """
    ((as_date, as_date32),) = clickhouse.query(
        "select toDate('9999-12-31'), toDate32('9999-12-31')"
    ).result_rows

    assert as_date == date(2149, 6, 6)
    assert as_date32 == date(9999, 12, 31)


# One reporting period. The value 100 was filed first; it was restated to 110 later; a third
# filing accepted at the same instant as the restatement carries a higher accession number.
FACTS = """
    select 320193 as cik, 'Revenues' as concept,
           toDate('2025-10-01') as period_start, toDate('2025-12-31') as period_end,
           value, toDateTime(known_at, 'UTC') as known_at, accession_number
    from values(
        'value Float64, known_at String, accession_number String',
        (100, '2026-01-30 21:05:00', '0000320193-26-000010'),
        (110, '2026-05-02 20:30:00', '0000320193-26-000041'),
        (111, '2026-05-02 20:30:00', '0000320193-26-000042')
    )
"""

AS_OF = """
    with facts as ({facts})
    select period_start, period_end,
           argMax(value, (known_at, accession_number)) as value_known,
           max(known_at)                               as known_since
    from facts
    where cik = 320193
      and concept = 'Revenues'
      and known_at < toDateTime('{as_of}', 'UTC')
    group by period_start, period_end
    order by period_end, period_start
"""


@pytest.mark.parametrize(
    ("as_of", "expected"),
    [
        ("2026-01-01 00:00:00", None),  # nothing was known yet
        ("2026-04-01 00:00:00", 100.0),  # the first-reported value
        ("2026-06-01 00:00:00", 111.0),  # the restatement; the tie goes to the higher accession
    ],
)
def test_as_of_query_with_tuple_argmax(
    clickhouse: Client, as_of: str, expected: float | None
) -> None:
    rows = clickhouse.query(AS_OF.format(facts=FACTS, as_of=as_of)).result_rows

    if expected is None:
        assert rows == []
    else:
        ((period_start, period_end, value_known, _),) = rows
        assert (period_start, period_end) == (date(2025, 10, 1), date(2025, 12, 31))
        assert value_known == expected


def test_margin_to_next_centroid(clickhouse: Client) -> None:
    """arraySort(groupArray(distance))[2] - min(distance): the cluster-margin expression of DM-3."""
    rows = clickhouse.query(
        """
        select cik,
               argMin(cluster_id, distance)                       as cluster_id,
               min(distance)                                      as distance_to_centroid,
               arraySort(groupArray(distance))[2] - min(distance) as margin_to_next
        from values('cik UInt32, cluster_id UInt8, distance Float64',
                    (1, 0, 2.5), (1, 1, 0.5), (1, 2, 1.75), (2, 0, 3.0))
        group by cik
        order by cik
        """
    ).result_rows

    assert rows[0] == (1, 1, 0.5, 1.25)
    # With a single centroid there is no second element: ClickHouse returns the type's default,
    # 0, so the margin is negative. A model with one cluster must not use this expression.
    assert rows[1] == (2, 0, 3.0, -3.0)


def test_seasonal_robust_z_score_arrays(clickhouse: Client) -> None:
    """arrayReduce, arrayMap over an alias and length() in WHERE: the anomaly expression of DM-5."""
    rows = clickhouse.query(
        """
        with history as (
            select cik, stories, past
            from values('cik UInt32, stories Float64, past Array(Float64)',
                        (1, 30, [4, 6, 5, 7, 5, 6, 4, 5]),
                        (2, 30, [4, 6, 5]))
        )
        select cik, stories,
               arrayReduce('medianExact', past)                                      as median_past,
               arrayReduce('medianExact', arrayMap(x -> abs(x - median_past), past)) as mad_past,
               (stories - median_past) / greatest(1.4826 * mad_past, 1)              as seasonal_z
        from history
        where length(past) >= 6
        order by cik
        """
    ).result_rows

    # Company 2 has fewer than six comparable days and is left out.
    ((cik, stories, median_past, mad_past, seasonal_z),) = rows
    assert (cik, stories, median_past, mad_past) == (1, 30.0, 5.0, 1.0)
    assert seasonal_z == pytest.approx((30 - 5) / 1.4826)
