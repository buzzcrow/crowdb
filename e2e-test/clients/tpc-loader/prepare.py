# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Prepare generator prerequisites and validate complete SF1 data before loading."""

import argparse
import importlib.metadata
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", type=Path, help="parent for a fresh owned preparation directory")
    args = parser.parse_args()
    root = args.artifacts.resolve() / ("run-" + uuid.uuid4().hex)
    root.mkdir(parents=True)
    versions = {name: importlib.metadata.version(name) for name in
                ("crowdb-tpc-loader", "pyarrow", "duckdb", "pyiceberg", "botocore")}
    record = {"versions": versions, "python": sys.executable, "phases": [], "status": "failed"}
    print(f"TPC prerequisite artifacts: {root}", flush=True)
    try:
        for benchmark in ("tpch", "tpcds"):
            started = time.monotonic()
            with (root / f"{benchmark}.log").open("w") as output:
                result = subprocess.run([sys.executable, "-m", "crowdb_tpc_loader", "generate",
                    "--benchmark", benchmark, "--sf", "1", "--output-dir", str(root / benchmark),
                    "--report-file", str(root / f"{benchmark}.json")],
                    stdout=output, stderr=subprocess.STDOUT, check=False, timeout=1800)
            print((root / f"{benchmark}.log").read_text(), end="", flush=True)
            record["phases"].append({"benchmark": benchmark, "exit_code": result.returncode,
                                     "seconds": time.monotonic() - started})
            if result.returncode:
                raise RuntimeError(f"{benchmark} SF1 prerequisite generation/validation failed")
        record["status"] = "pass"
    finally:
        (root / "preparation.json").write_text(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()
