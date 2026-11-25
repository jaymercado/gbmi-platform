"""Table definitions shared by every writer (blueprint 7.4).

Silver tables are Iceberg format version 2 with ZSTD-compressed Parquet. GDELT tables are
partitioned by ``slot_date`` with the identity transform, and the partition spec never evolves
(blueprint 7.8).
"""

from __future__ import annotations

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
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from gbmi.core.gdelt_export import EVENT_COLUMNS, EVENT_KEY, Kind

#: Properties of every silver table.
TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
}


def slot_date_partition(schema: Schema) -> PartitionSpec:
    """Identity partition on ``slot_date``: one partition is exactly one UTC day."""
    field = schema.find_field("slot_date")
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


# --- gdelt_event ------------------------------------------------------------------------------

GDELT_EVENT = "gdelt_event"

#: Provenance columns carried by every silver table (blueprint 7.12).
PROVENANCE_COLUMNS: tuple[tuple[str, IcebergType], ...] = (
    ("_source_file", StringType()),
    ("_source_md5", StringType()),
    ("_slot_ts", TimestamptzType()),
    ("_ingested_at", TimestamptzType()),
    ("_run_id", StringType()),
    ("_pipeline_version", StringType()),
)

_KIND_TYPE: dict[Kind, IcebergType] = {
    Kind.KEY: LongType(),
    Kind.INTEGER: IntegerType(),
    Kind.DOUBLE: DoubleType(),
    Kind.TEXT: StringType(),
    Kind.FLAG: BooleanType(),
    Kind.DATE8: DateType(),
    Kind.TIMESTAMP14: TimestamptzType(),
}


def _gdelt_event_schema() -> Schema:
    """Key, slot columns, the fields taken from the export feed, then provenance."""
    columns: list[tuple[str, IcebergType, bool]] = [
        (EVENT_KEY, LongType(), True),
        ("slot_date", DateType(), True),
        ("slot_hour", IntegerType(), True),
        ("slot_ts", TimestamptzType(), True),
    ]
    columns += [
        (column.name, _KIND_TYPE[column.kind], False)
        for column in EVENT_COLUMNS
        if column.name != EVENT_KEY
    ]
    columns += [(name, kind, False) for name, kind in PROVENANCE_COLUMNS]
    return Schema(
        *(
            NestedField(field_id, name, kind, required=required)
            for field_id, (name, kind, required) in enumerate(columns, start=1)
        )
    )


#: One row per coded event; logical key ``global_event_id``; partitioned by ``slot_date``.
GDELT_EVENT_SCHEMA = _gdelt_event_schema()
