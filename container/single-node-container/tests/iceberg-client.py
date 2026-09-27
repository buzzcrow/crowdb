import os
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField


NAMESPACE = ("crowdb-preview-e2e",)
TABLE = NAMESPACE + ("events",)


def main():
    catalog = load_catalog(
        "crowdb-preview",
        type="rest",
        uri=os.environ["CROWDB_PREVIEW_ICEBERG_URI"],
        token=os.environ["ICEBERG_TOKEN"],
    )
    if sys.argv[1] == "write":
        catalog.create_namespace(NAMESPACE, {"preview": "persisted"})
        table = catalog.create_table(
            TABLE,
            Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=True)),
        )
        table.transaction().set_properties({"preview": "persisted"}).commit_transaction()
    assert catalog.namespace_exists(NAMESPACE)
    assert NAMESPACE in catalog.list_namespaces()
    assert catalog.load_namespace_properties(NAMESPACE) == {"preview": "persisted"}
    assert TABLE in catalog.list_tables(NAMESPACE)
    assert catalog.load_table(TABLE).properties["preview"] == "persisted"


if __name__ == "__main__":
    main()
