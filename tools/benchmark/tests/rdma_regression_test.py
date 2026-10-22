# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Result and exit-status contracts, using fixture processes without an HCA."""

import csv
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class RdmaRegressionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rdma-runner-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.root = Path(__file__).resolve().parents[3]
        self.server = self.directory / "server.py"
        self.server.write_text(
            '#!/usr/bin/env python3\nimport signal\n'
            'print("listening port=1", flush=True)\nsignal.pause()\n'
        )
        self.server.chmod(0o755)
        self.client = self.directory / "client.py"
        self.client.write_text(
            '#!/usr/bin/env python3\nimport os, sys\n'
            'assert os.environ["CROWDB_RDMA_ALLOW_TCP_FALLBACK"] == "0"\n'
            'assert sys.argv[sys.argv.index("--mode") + 1] == "coroutine"\n'
            'print(os.environ["RDMA_FIXTURE_REPORT"])\n'
            'sys.exit(int(os.environ["RDMA_FIXTURE_EXIT"]))\n'
        )
        self.client.chmod(0o755)

    def run_case(self, report, client_exit):
        environment = os.environ.copy()
        environment.update({
            "RDMA_BENCH_SERVER_BIN": str(self.server),
            "RDMA_BENCH_CLI_BIN": str(self.client),
            "RDMA_BENCH_CASES": "rdma_1w_1l_1c",
            "RDMA_BENCH_LOG_ROOT": str(self.directory / "output"),
            "RDMA_FIXTURE_REPORT": report,
            "RDMA_FIXTURE_EXIT": str(client_exit),
        })
        process = subprocess.run(
            [shutil.which("pixi"), "run", "--", "bash", "tools/benchmark/bench-rdma-rpc-regression.sh"],
            cwd=self.root, env=environment, capture_output=True, text=True, timeout=20,
        )
        results = self.directory / "output/results.tsv"
        self.assertTrue(results.exists(), process.stdout + process.stderr)
        with results.open() as file:
            rows = list(csv.DictReader(file, delimiter="\t"))
        self.assertEqual(len(rows), 1, process.stdout + process.stderr)
        return process, rows[0]

    @staticmethod
    def report(errors=0):
        return json.dumps({
            "total_ops": 100,
            "duration_ms": 10000,
            "total_errors": errors,
            "by_op": {"write": {"latency_us": {"avg_us": 20.0, "p50_us": 18, "p99_us": 40}}},
        }, indent=2)

    def test_valid_report_succeeds_and_records_zero_errors(self):
        process, row = self.run_case(self.report(), 0)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(row["errors"], "0")
        self.assertEqual(row["report_errors"], "0")
        self.assertEqual(row["valid"], "1")

    def test_failed_client_preserves_reported_error_count(self):
        process, row = self.run_case(self.report(errors=7), 2)
        self.assertNotEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(row["errors"], "7")
        self.assertEqual(row["client_exit"], "2")
        self.assertEqual(row["valid"], "0")

    def test_missing_report_is_not_success_even_when_client_exits_zero(self):
        process, row = self.run_case("human report", 0)
        self.assertNotEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(row["errors"], "NA")
        self.assertEqual(row["report_errors"], "1")
        self.assertEqual(row["valid"], "0")

    def test_error_report_is_failure_even_when_client_exits_zero(self):
        process, row = self.run_case(self.report(errors=3), 0)
        self.assertNotEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(row["errors"], "3")
        self.assertEqual(row["valid"], "0")


if __name__ == "__main__":
    unittest.main()
