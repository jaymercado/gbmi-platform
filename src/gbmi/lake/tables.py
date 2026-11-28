"""Table definitions shared by every writer (blueprint 7.4).

Silver tables are Iceberg format version 2 with ZSTD-compressed Parquet. GDELT tables are
partitioned by ``slot_date`` with the identity transform, and the partition spec never evolves
(blueprint 7.8).

The five SEC tables (week 3 plan 3.3) are source-shaped: one table for each kind of source
file. Two are partitioned by a date, one by the company, two have no partition. Their rows
carry the provenance columns without ``_slot_ts``, which belongs to GDELT.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
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


def identity_partition(schema: Schema, column: str | None) -> PartitionSpec:
    """Identity partition on one column; no partition when ``column`` is None."""
    if column is None:
        return UNPARTITIONED_PARTITION_SPEC
    field = schema.find_field(column)
    return PartitionSpec(
        PartitionField(
            source_id=field.field_id, field_id=1000, transform=IdentityTransform(), name=column
        )
    )


def slot_date_partition(schema: Schema) -> PartitionSpec:
    """Identity partition on ``slot_date``: one partition is exactly one UTC day."""
    return identity_partition(schema, PARTITION_COLUMN)


def create_table_if_missing(
    catalog: Catalog,
    namespace: str,
    name: str,
    schema: Schema,
    *,
    partition_column: str | None = PARTITION_COLUMN,
) -> Table:
    """Load a silver table, creating it on first use.

    The table is partitioned by ``partition_column`` with the identity transform: ``slot_date``
    for a GDELT table. None creates a table without a partition.
    """
    identifier = (namespace, name)
    try:
        return catalog.load_table(identifier)
    except NoSuchTableError:
        return catalog.create_table(
            identifier,
            schema=schema,
            partition_spec=identity_partition(schema, partition_column),
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
#: The slot of the source file: a provenance column of the GDELT tables only (blueprint 7.12).
SLOT_PROVENANCE = "_slot_ts"
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


def provenance_names(*, slot_ts: bool = True) -> list[str]:
    """The provenance columns of a table, in table order; without ``_slot_ts`` for an SEC table."""
    return [name for name, _ in PROVENANCE_COLUMNS if slot_ts or name != SLOT_PROVENANCE]


def _schema(columns: Sequence[Column], *, slot_ts: bool = True) -> Schema:
    """Columns in order, then provenance. Field ids count from 1; list elements follow."""
    provenance = [
        (name, kind, False)
        for name, kind in PROVENANCE_COLUMNS
        if slot_ts or name != SLOT_PROVENANCE
    ]
    complete = [*columns, *provenance]
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


# --- the five silver tables of the SEC (week 3 plan 3.3) --------------------------------------

SEC_COMPANY_SNAPSHOT = "sec_company_snapshot"
SEC_COMPANY_FORMER_NAME = "sec_company_former_name"
SEC_FILING_INDEX = "sec_filing_index"
SEC_FILING_DETAIL = "sec_filing_detail"
SEC_XBRL_FACT = "sec_xbrl_fact"

#: The value of a fact. 38 digits hold every amount; six decimals hold a per-share value and a
#: ratio. ``value_text`` keeps the number exactly as the SEC wrote it.
FACT_VALUE = DecimalType(38, 6)

#: One row for each company of the universe and snapshot date; key (``snapshot_date``, ``cik``);
#: partitioned by ``snapshot_date``; one date is the unit.
SEC_COMPANY_SNAPSHOT_SCHEMA = _schema(
    [
        ("snapshot_date", DateType(), True),
        ("cik", LongType(), True),
        ("name", StringType(), True),
        # A list is never NULL: a company without a value has an empty list.
        ("tickers", STRING_LIST, True),
        ("exchanges", STRING_LIST, True),
        ("primary_ticker", StringType(), False),
        ("primary_exchange", StringType(), False),
        ("sic", StringType(), False),
        ("sic_description", StringType(), False),
        ("state_of_incorporation", StringType(), False),
        ("fiscal_year_end", StringType(), False),
        ("category", StringType(), False),
        ("entity_type", StringType(), False),
        ("lei", StringType(), False),
        ("attr_hash", StringType(), True),
        ("attributes_as_of", DateType(), False),
        ("is_stale", BooleanType(), True),
        ("is_inactive", BooleanType(), True),
    ],
    slot_ts=False,
)

#: One row for each former name of a company; key (``cik``, ``former_name``, ``valid_from``);
#: no partition; the refreshed companies are the unit.
SEC_COMPANY_FORMER_NAME_SCHEMA = _schema(
    [
        ("cik", LongType(), True),
        ("former_name", StringType(), True),
        ("valid_from", DateType(), True),
        ("valid_to", DateType(), False),
    ],
    slot_ts=False,
)

#: One row for each filing and filer of one daily index file; key (``index_date``,
#: ``accession_number``, ``cik``); partitioned by ``index_date``; one index file is the unit.
SEC_FILING_INDEX_SCHEMA = _schema(
    [
        ("index_date", DateType(), True),
        ("accession_number", StringType(), True),
        ("cik", LongType(), True),
        ("form", StringType(), True),
        ("company_name", StringType(), True),
        ("filing_date", DateType(), True),
        ("file_name", StringType(), True),
        ("line_number", IntegerType(), True),
    ],
    slot_ts=False,
)

#: One row for each filing of a watchlist company, from the submissions document; key (``cik``,
#: ``accession_number``); no partition; the refreshed companies are the unit.
SEC_FILING_DETAIL_SCHEMA = _schema(
    [
        ("cik", LongType(), True),
        ("accession_number", StringType(), True),
        ("form", StringType(), True),
        ("filing_date", DateType(), True),
        ("report_date", DateType(), False),
        ("acceptance_ts", TimestamptzType(), False),
        ("act", StringType(), False),
        ("file_number", StringType(), False),
        ("film_number", StringType(), False),
        ("items", STRING_LIST, True),
        ("core_type", StringType(), False),
        ("size_bytes", LongType(), False),
        ("is_xbrl", BooleanType(), False),
        ("is_inline_xbrl", BooleanType(), False),
        ("is_xbrl_numeric", BooleanType(), False),
        ("primary_document", StringType(), False),
        ("primary_doc_description", StringType(), False),
    ],
    slot_ts=False,
)

#: One row for each financial fact of a watchlist company; the key of blueprint 7.4; partitioned
#: by ``cik`` (week 3 plan, change C15); the refreshed companies are the unit.
SEC_XBRL_FACT_SCHEMA = _schema(
    [
        ("cik", LongType(), True),
        ("taxonomy", StringType(), True),
        ("concept", StringType(), True),
        ("unit", StringType(), True),
        ("period_start", DateType(), True),
        ("period_end", DateType(), True),
        ("accession_number", StringType(), True),
        ("is_instant", BooleanType(), True),
        ("value", FACT_VALUE, True),
        ("value_text", StringType(), True),
        ("fiscal_year", IntegerType(), False),
        ("fiscal_period", StringType(), False),
        ("form", StringType(), True),
        ("filed_date", DateType(), True),
        ("frame", StringType(), False),
    ],
    slot_ts=False,
)

#: The SEC tables stand apart from the GDELT tables: a build of a GDELT feed never sees them.
SEC_SCHEMAS: dict[str, Schema] = {
    SEC_COMPANY_SNAPSHOT: SEC_COMPANY_SNAPSHOT_SCHEMA,
    SEC_COMPANY_FORMER_NAME: SEC_COMPANY_FORMER_NAME_SCHEMA,
    SEC_FILING_INDEX: SEC_FILING_INDEX_SCHEMA,
    SEC_FILING_DETAIL: SEC_FILING_DETAIL_SCHEMA,
    SEC_XBRL_FACT: SEC_XBRL_FACT_SCHEMA,
}
SEC_TABLE_KEYS: dict[str, tuple[str, ...]] = {
    SEC_COMPANY_SNAPSHOT: ("snapshot_date", "cik"),
    SEC_COMPANY_FORMER_NAME: ("cik", "former_name", "valid_from"),
    SEC_FILING_INDEX: ("index_date", "accession_number", "cik"),
    SEC_FILING_DETAIL: ("cik", "accession_number"),
    SEC_XBRL_FACT: (
        "cik",
        "taxonomy",
        "concept",
        "unit",
        "period_start",
        "period_end",
        "accession_number",
    ),
}
#: The partition column of each SEC table; None for a table without a partition.
SEC_PARTITIONS: dict[str, str | None] = {
    SEC_COMPANY_SNAPSHOT: "snapshot_date",
    SEC_COMPANY_FORMER_NAME: None,
    SEC_FILING_INDEX: "index_date",
    SEC_FILING_DETAIL: None,
    SEC_XBRL_FACT: "cik",
}

# --- every silver table -----------------------------------------------------------------------

ALL_SCHEMAS: dict[str, Schema] = {**SILVER_SCHEMAS, **SEC_SCHEMAS}
ALL_TABLE_KEYS: dict[str, tuple[str, ...]] = {**TABLE_KEYS, **SEC_TABLE_KEYS}
PARTITIONS: dict[str, str | None] = {
    **dict.fromkeys(SILVER_SCHEMAS, PARTITION_COLUMN),
    **SEC_PARTITIONS,
}


def create_sec_table_if_missing(catalog: Catalog, namespace: str, name: str) -> Table:
    """Load one of the five SEC tables, creating it with its partition on first use."""
    return create_table_if_missing(
        catalog, namespace, name, SEC_SCHEMAS[name], partition_column=SEC_PARTITIONS[name]
    )


def business_names(schema: Schema) -> list[str]:
    """The columns of a table which a parser fills from the file, in table order."""
    return [field.name for field in schema.fields if field.name not in RUN_COLUMNS]
