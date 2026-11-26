"""Table definitions shared by every writer (blueprint 7.4).

Silver tables are Iceberg format version 2 with ZSTD-compressed Parquet. GDELT tables are
partitioned by ``slot_date`` with the identity transform, and the partition spec never evolves
(blueprint 7.8).
"""

from __future__ import annotations

from collections.abc import Sequence

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from gbmi.core.gdelt import Feed
from gbmi.core.gdelt_export import EVENT_COLUMNS, EVENT_KEY
from gbmi.core.gdelt_gkg import DOC_COLUMNS, DOC_KEY, ORG_KEY, TONE_FIELDS
from gbmi.core.gdelt_mentions import MENTION_COLUMNS, MENTION_KEY
from gbmi.core.positional import Kind, SilverColumn

#: Properties of every silver table.
TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
}

PARTITION_COLUMN = "slot_date"

#: A column of a table definition: name, type and whether a value is required.
Column = tuple[str, IcebergType, bool]
#: Marks a list of strings in a table definition; the builder gives the element its field id.
STRING_LIST = ListType(element_id=0, element_type=StringType(), element_required=True)


def slot_date_partition(schema: Schema) -> PartitionSpec:
    """Identity partition on ``slot_date``: one partition is exactly one UTC day."""
    field = schema.find_field(PARTITION_COLUMN)
    return PartitionSpec(
        PartitionField(
            source_id=field.field_id, field_id=1000, transform=IdentityTransform(), name="slot_date"
        )
    )


def create_table_if_missing(catalog: Catalog, namespace: str, name: str, schema: Schema) -> Table:
    """Load a slot-date-partitioned silver table, creating it on first use."""
    identifier = (namespace, name)
    try:
        return catalog.load_table(identifier)
    except NoSuchTableError:
        return catalog.create_table(
            identifier,
            schema=schema,
            partition_spec=slot_date_partition(schema),
            properties=TABLE_PROPERTIES,
        )


#: Provenance columns carried by every silver table (blueprint 7.12).
PROVENANCE_COLUMNS: tuple[tuple[str, IcebergType], ...] = (
    ("_source_file", StringType()),
    ("_source_md5", StringType()),
    ("_slot_ts", TimestamptzType()),
    ("_ingested_at", TimestamptzType()),
    ("_run_id", StringType()),
    ("_pipeline_version", StringType()),
)
#: The three provenance columns which a run fills in, not the file.
RUN_COLUMNS = ("_ingested_at", "_run_id", "_pipeline_version")

_KIND_TYPE: dict[Kind, IcebergType] = {
    Kind.KEY: LongType(),
    Kind.INTEGER: IntegerType(),
    Kind.DOUBLE: DoubleType(),
    Kind.TEXT: StringType(),
    Kind.REQUIRED_TEXT: StringType(),
    Kind.FLAG: BooleanType(),
    Kind.DATE8: DateType(),
    Kind.TIMESTAMP14: TimestamptzType(),
    Kind.REQUIRED_TIMESTAMP14: TimestamptzType(),
}
_REQUIRED_KINDS = frozenset({Kind.KEY, Kind.REQUIRED_TEXT, Kind.REQUIRED_TIMESTAMP14})

#: The slot columns of every GDELT table: the partition, the hour and the slot of the file.
_SLOT_COLUMNS: tuple[Column, ...] = (
    ("slot_date", DateType(), True),
    ("slot_hour", IntegerType(), True),
    ("slot_ts", TimestamptzType(), True),
)


def _from_feed(columns: Sequence[SilverColumn], *, skip: Sequence[str] = ()) -> list[Column]:
    """The table columns which come straight from the fields of a feed."""
    return [
        (column.name, _KIND_TYPE[column.kind], column.kind in _REQUIRED_KINDS)
        for column in columns
        if column.name not in skip
    ]


def _schema(columns: Sequence[Column]) -> Schema:
    """Columns in order, then provenance. Field ids count from 1; list elements follow."""
    complete = [*columns, *((name, kind, False) for name, kind in PROVENANCE_COLUMNS)]
    element_id = len(complete)
    fields: list[NestedField] = []
    for field_id, (name, kind, required) in enumerate(complete, start=1):
        if isinstance(kind, ListType):
            element_id += 1
            kind = ListType(
                element_id=element_id,
                element_type=kind.element_type,
                element_required=kind.element_required,
            )
        fields.append(NestedField(field_id, name, kind, required=required))
    return Schema(*fields)


# --- gdelt_event ------------------------------------------------------------------------------

GDELT_EVENT = "gdelt_event"

#: One row per coded event; logical key ``global_event_id``; partitioned by ``slot_date``.
GDELT_EVENT_SCHEMA = _schema(
    [(EVENT_KEY, LongType(), True), *_SLOT_COLUMNS, *_from_feed(EVENT_COLUMNS, skip=[EVENT_KEY])]
)

# --- gdelt_mention ----------------------------------------------------------------------------

GDELT_MENTION = "gdelt_mention"

#: One row per mention of an event in an article; logical key (``global_event_id``,
#: ``mention_identifier``, ``slot_ts``).
GDELT_MENTION_SCHEMA = _schema(
    [
        ("global_event_id", LongType(), True),
        ("mention_identifier", StringType(), True),
        *_SLOT_COLUMNS,
        *_from_feed(MENTION_COLUMNS, skip=["global_event_id", "mention_identifier"]),
    ]
)

# --- gdelt_gkg_doc and gdelt_gkg_org ----------------------------------------------------------

GDELT_GKG_DOC = "gdelt_gkg_doc"
GDELT_GKG_ORG = "gdelt_gkg_org"

#: One row per article of the Global Knowledge Graph; logical key ``gkg_record_id``.
GDELT_GKG_DOC_SCHEMA = _schema(
    [
        (DOC_KEY, StringType(), True),
        *_SLOT_COLUMNS,
        *_from_feed(DOC_COLUMNS, skip=[DOC_KEY]),
        ("page_title", StringType(), False),
        ("story_id", StringType(), True),
        *((name, DoubleType(), False) for name in TONE_FIELDS if name != "word_count"),
        ("word_count", IntegerType(), False),
        # A list is never NULL: a document without values has an empty list (week 2 plan 3.3).
        ("themes", STRING_LIST, True),
        ("countries", STRING_LIST, True),
        ("org_count", IntegerType(), True),
        ("is_translated", BooleanType(), True),
    ]
)

#: One row per article and normalised organisation string; logical key (``gkg_record_id``,
#: ``org_string_id``).
GDELT_GKG_ORG_SCHEMA = _schema(
    [
        (DOC_KEY, StringType(), True),
        ("org_string_id", StringType(), True),
        *_SLOT_COLUMNS,
        ("org_string_raw", StringType(), True),
        ("org_string_norm", StringType(), True),
        ("first_offset", IntegerType(), False),
        ("mention_count", IntegerType(), True),
    ]
)

# --- the four silver tables of GDELT ----------------------------------------------------------

SILVER_SCHEMAS: dict[str, Schema] = {
    GDELT_EVENT: GDELT_EVENT_SCHEMA,
    GDELT_MENTION: GDELT_MENTION_SCHEMA,
    GDELT_GKG_DOC: GDELT_GKG_DOC_SCHEMA,
    GDELT_GKG_ORG: GDELT_GKG_ORG_SCHEMA,
}
#: The logical key of each table (blueprint 7.4).
TABLE_KEYS: dict[str, tuple[str, ...]] = {
    GDELT_EVENT: (EVENT_KEY,),
    GDELT_MENTION: MENTION_KEY,
    GDELT_GKG_DOC: (DOC_KEY,),
    GDELT_GKG_ORG: ORG_KEY,
}
#: The tables which one feed fills, in the order of the commits.
FEED_TABLES: dict[Feed, tuple[str, ...]] = {
    Feed.EXPORT: (GDELT_EVENT,),
    Feed.MENTIONS: (GDELT_MENTION,),
    Feed.GKG: (GDELT_GKG_DOC, GDELT_GKG_ORG),
}


def business_names(schema: Schema) -> list[str]:
    """The columns of a table which a parser fills from the file, in table order."""
    return [field.name for field in schema.fields if field.name not in RUN_COLUMNS]
