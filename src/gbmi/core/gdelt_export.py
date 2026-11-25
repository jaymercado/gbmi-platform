"""The positional schema of the GDELT 2.0 ``export`` (Events) feed.

An export file is tab-delimited text with no header and exactly 61 columns, in the order the
GDELT Event Database Codebook V2.0 gives them. The schema is positional and fixed: a row with
any other column count is rejected, never guessed at (gate G2, blueprint 5.7).

``EVENT_COLUMNS`` says which of the 61 source columns become which column of the silver table
``gdelt_event`` (blueprint 7.4) and how each is converted. Every engine that parses the feed
is generated from this one definition.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

EXPORT_COLUMNS: tuple[str, ...] = (
    # event id and date
    "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year",
    "FractionDate",
    # actor 1
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code",
    "Actor1Type2Code", "Actor1Type3Code",
    # actor 2
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code",
    "Actor2Type2Code", "Actor2Type3Code",
    # event action
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone",
    # actor 1 geography
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code",
    "Actor1Geo_ADM2Code", "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    # actor 2 geography
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code",
    "Actor2Geo_ADM2Code", "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    # action geography
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
    "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    # data management
    "DATEADDED", "SOURCEURL",
)  # fmt: skip

EXPORT_COLUMN_COUNT = 61
if len(EXPORT_COLUMNS) != EXPORT_COLUMN_COUNT:  # a typo here would shift every later column
    raise AssertionError(
        f"the export schema has {EXPORT_COLUMN_COUNT} columns, not {len(EXPORT_COLUMNS)}"
    )


class Kind(StrEnum):
    """How a source field becomes a typed value. An empty field is NULL, except for a key."""

    KEY = "key"  # integer, required
    INTEGER = "integer"
    DOUBLE = "double"
    TEXT = "text"
    FLAG = "flag"  # 0 or 1, stored as a boolean
    DATE8 = "date8"  # YYYYMMDD
    TIMESTAMP14 = "timestamp14"  # YYYYMMDDHHMMSS, UTC


@dataclass(frozen=True)
class SilverColumn:
    """One column of ``gdelt_event`` that comes straight from an export field."""

    name: str
    source: str
    kind: Kind

    @property
    def position(self) -> int:
        """One-based position of the source field in an export row."""
        return EXPORT_COLUMNS.index(self.source) + 1


EVENT_KEY = "global_event_id"

EVENT_COLUMNS: tuple[SilverColumn, ...] = (
    SilverColumn(EVENT_KEY, "GLOBALEVENTID", Kind.KEY),
    SilverColumn("event_date", "SQLDATE", Kind.DATE8),
    SilverColumn("actor1_code", "Actor1Code", Kind.TEXT),
    SilverColumn("actor1_name", "Actor1Name", Kind.TEXT),
    SilverColumn("actor1_country_code", "Actor1CountryCode", Kind.TEXT),
    SilverColumn("actor1_type1_code", "Actor1Type1Code", Kind.TEXT),
    SilverColumn("actor2_code", "Actor2Code", Kind.TEXT),
    SilverColumn("actor2_name", "Actor2Name", Kind.TEXT),
    SilverColumn("actor2_country_code", "Actor2CountryCode", Kind.TEXT),
    SilverColumn("actor2_type1_code", "Actor2Type1Code", Kind.TEXT),
    SilverColumn("is_root_event", "IsRootEvent", Kind.FLAG),
    SilverColumn("event_code", "EventCode", Kind.TEXT),
    SilverColumn("event_base_code", "EventBaseCode", Kind.TEXT),
    SilverColumn("event_root_code", "EventRootCode", Kind.TEXT),
    SilverColumn("quad_class", "QuadClass", Kind.INTEGER),
    SilverColumn("goldstein_scale", "GoldsteinScale", Kind.DOUBLE),
    SilverColumn("num_mentions", "NumMentions", Kind.INTEGER),
    SilverColumn("num_sources", "NumSources", Kind.INTEGER),
    SilverColumn("num_articles", "NumArticles", Kind.INTEGER),
    SilverColumn("avg_tone", "AvgTone", Kind.DOUBLE),
    SilverColumn("action_geo_type", "ActionGeo_Type", Kind.INTEGER),
    SilverColumn("action_geo_full_name", "ActionGeo_FullName", Kind.TEXT),
    SilverColumn("action_geo_country_code", "ActionGeo_CountryCode", Kind.TEXT),
    SilverColumn("date_added", "DATEADDED", Kind.TIMESTAMP14),
    SilverColumn("source_url", "SOURCEURL", Kind.TEXT),
)
