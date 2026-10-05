# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Create actual committed Parquet/Avro references in the owned native fixture."""
import sys
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType

catalog = load_catalog("console-inspection", type="rest", uri=sys.argv[1])
namespace = sys.argv[2]
catalog.create_namespace(namespace)
table = catalog.create_table(
    (namespace, "events"),
    schema=pa.schema([pa.field("id", pa.int64()), pa.field("message", pa.string())]),
    properties={"write.parquet.row-group-limit": "2"},
)
for first in (0, 44):
    table.append(pa.table({"id": list(range(first, first + 44)), "message": ["雪", "alpha", "beta", "gamma"] * 11}))
print("Committed two snapshots with native Parquet data and Avro manifests")
many = catalog.create_table(
    (namespace, "many_files"),
    schema=Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "message", StringType(), required=False)),
    partition_spec=PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id")),
)
many.append(pa.table({"id": list(range(101)), "message": ["file"] * 101}))
print("Committed 101 physical files for independent manifest-file pagination")
