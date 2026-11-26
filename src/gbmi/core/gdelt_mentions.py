"""The positional schema of the GDELT 2.0 ``mentions`` feed.

A mentions file has exactly 16 tab-delimited columns, in the order of the GDELT Event Database
Codebook V2.0 (Mentions table). One row is one mention of one event in one article.

The silver table ``gdelt_mention`` (blueprint 7.4) keeps the logical key and six columns. The
batch time of a row (``MentionTimeDate``) equals the time in the file name for every row of a
file (week 2 plan, F8); the table stores it as ``slot_ts``.
"""

from __future__ import annotations

from gbmi.core.positional import Kind, feed_schema

MENTIONS_COLUMNS: tuple[str, ...] = (
    "GLOBALEVENTID", "EventTimeDate", "MentionTimeDate", "MentionType",
    "MentionSourceName", "MentionIdentifier", "SentenceID", "Actor1CharOffset",
    "Actor2CharOffset", "ActionCharOffset", "InRawText", "Confidence",
    "MentionDocLen", "MentionDocTone", "MentionDocTranslationInfo", "Extras",
)  # fmt: skip

MENTIONS_COLUMN_COUNT = 16
#: The logical key of ``gdelt_mention``, together with the slot (blueprint 7.4).
MENTION_KEY = ("global_event_id", "mention_identifier", "slot_ts")

MENTIONS = feed_schema(
    "mentions",
    MENTIONS_COLUMNS,
    MENTIONS_COLUMN_COUNT,
    (
        ("global_event_id", "GLOBALEVENTID", Kind.KEY),
        ("mention_identifier", "MentionIdentifier", Kind.REQUIRED_TEXT),
        ("event_time_date", "EventTimeDate", Kind.TIMESTAMP14),
        ("mention_type", "MentionType", Kind.INTEGER),
        ("mention_source_name", "MentionSourceName", Kind.TEXT),
        ("confidence", "Confidence", Kind.INTEGER),
        ("mention_doc_tone", "MentionDocTone", Kind.DOUBLE),
    ),
)

MENTION_COLUMNS = MENTIONS.columns
