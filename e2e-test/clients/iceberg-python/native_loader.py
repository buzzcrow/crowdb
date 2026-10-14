# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Exercise the production loader's upload, report and commit dataflow."""

from decimal import Decimal
import json
from pathlib import Path
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from crowdb_tpc_loader.backend import IcebergBackend
from crowdb_tpc_loader.loader import load_tables
from crowdb_tpc_loader.models import Options, ParquetPart, TableData
from crowdb_tpc_loader.report import RunReport
from crowdb_tpc_loader.security import Redactor


def verify(config):
    report_path = Path(config["report_dir"]) / "native-load-report.json"
    options = Options("load", "tpch", Decimal("0.01"), catalog_uri=config["endpoint"],
                      token="w" * 32, namespace=("native_loader",), report_file=report_path,
                      catalog_properties={"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"})
    backend = IcebergBackend(options, Redactor([options.token]))
    backend.connect()
    if not config.get("verify_restart"):
        backend.ensure_namespace()
        report = RunReport(options, "native-container-fixture", backend.redactor)
        report.paths.append(report_path)
        report.data["generator"] = {"implementation": "acceptance-fixture", "version": "1"}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            backend.set_staging(root)
            schema = pa.schema([pa.field("id", pa.int64())])
            path = root / "part.parquet"
            pq.write_table(pa.table({"id": [1, 2, 3]}, schema=schema), path)
            part = ParquetPart(path, 3, path.stat().st_size, schema)
            data = TableData("region", (part,), schema)
            report.generated_table(data, root)
            load_tables(backend, {data.name: data}, report, 4096, print, upload_workers=1)
        assert report.table("region")["status"] == "succeeded", report.data
    saved = json.loads(report_path.read_text())
    item = saved["tables"]["region"]["files"][0]
    assert item["state"] == "registered", item
    assert "/f/v1/" in item["remote_uri"] and "/data/" in item["upload_uri"]
    assert item["remote_uri"] != item["upload_uri"]
    table = backend.catalog.load_table((*options.namespace, "region"))
    assert [task.file.file_path for task in table.scan().plan_files()] == [item["remote_uri"]]
    selected = table.scan().to_arrow()
    with duckdb.connect() as connection:
        connection.register("selected", selected)
        assert connection.execute("SELECT count(*), sum(id) FROM selected").fetchone() == (3, 6)
    print("Production loader: selected URL report, standard commit and native SQL read passed")
