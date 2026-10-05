# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Direct REST/FileIO probe; infrastructure and unknown failures remain failures."""

import json
import os
import sys
from pathlib import Path

import duckdb


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def main():
    assert duckdb.__version__ == "1.4.3"
    connection = duckdb.connect()
    # Extensions are tied to this exact DuckDB version, not a nightly channel.
    connection.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg")
    connection.execute("CREATE SECRET ecosystem (TYPE iceberg, TOKEN " + literal(os.environ["ICEBERG_TOKEN"]) + ")")
    endpoint = literal(os.environ["CROWDB_PREVIEW_ICEBERG_URI"])
    connection.execute(f"ATTACH '' AS crowdb (TYPE iceberg, SECRET ecosystem, ENDPOINT {endpoint})")
    tables = connection.execute("SHOW ALL TABLES").fetchall()
    assert any(row[0] == "crowdb" and row[1] == "ecosystem" and row[2] == "handoff" for row in tables), tables
    # No PyIceberg conversion, static metadata path, generic S3 credentials or
    # fake replacement dataset can satisfy this catalog table query.
    stage = "selected-table-fileio"
    try:
        result = connection.execute("SELECT id, amount FROM crowdb.ecosystem.handoff ORDER BY id").fetchall()
    except duckdb.Error as error:
        message = str(error).replace(os.environ["ICEBERG_TOKEN"], "[REDACTED]")
        # CROWDB's delegated REST signing is a distinct contract. A known
        # missing remote-signing capability may be classified only after catalog
        # discovery succeeds; connection/auth/deadline/parser errors never pass.
        if "remote signing" not in message.lower() and "remote-signing" not in message.lower():
            raise
        report = {"client": "duckdb", "version": duckdb.__version__, "status": "unsupported",
                  "stage": stage, "boundary": "delegated REST FileIO signing", "diagnostic": message}
    else:
        ids = [1, 3, 4, 5, 6] if sys.argv[1:] == ["final"] else [1, 2, 3, 4]
        assert result == [(value, 10 * value) for value in ids], result
        aggregate = connection.execute("SELECT count(*), sum(amount) FROM crowdb.ecosystem.handoff").fetchone()
        assert aggregate == (len(ids), sum(ids) * 10), aggregate
        report = {"client": "duckdb", "version": duckdb.__version__, "status": "passed",
                  "operations": ["REST discovery", "delegated FileIO SELECT", "BI aggregate"]}
    Path(os.environ["CROWDB_ECOSYSTEM_RESULTS"]).joinpath("duckdb-final.json" if sys.argv[1:] == ["final"] else "duckdb.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report))


if __name__ == "__main__":
    main()
