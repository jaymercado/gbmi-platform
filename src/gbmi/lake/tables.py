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
