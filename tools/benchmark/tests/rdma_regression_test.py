# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Canonical report/readiness contracts with owned fixture processes."""

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
        temporary = tempfile.TemporaryDirectory(prefix="rdma-runner-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.root = Path(__file__).resolve().parents[3]
        self.server = self.directory / "server.py"
        self.server.write_text('''#!/usr/bin/env python3
import json, os, signal, sys, time
from pathlib import Path
assert os.environ["CROWDB_RDMA_ALLOW_TCP_FALLBACK"] == "0"
mode = os.environ.get("RDMA_FIXTURE_SERVER_MODE", "ok")
if mode == "exit": sys.exit(3)
print("listening port=19101", flush=True)
if mode == "stdout-only": signal.pause()
path = Path(sys.argv[sys.argv.index("--status_file") + 1])
status = dict(schema_version=1, process_id=os.getpid(), allow_tcp_fallback=False,
    rdma=dict(listen_active=mode != "no-rdma", ready=True, cm_port=19101))
path.write_text(json.dumps(status))
if mode == "early":
    time.sleep(0.2)
    sys.exit(4)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
signal.pause()
''')
        self.server.chmod(0o755)
        self.client = self.directory / "client.py"
        self.client.write_text('''#!/usr/bin/env python3
import os, sys, time
from pathlib import Path
assert os.environ["CROWDB_RDMA_ALLOW_TCP_FALLBACK"] == "0"
assert sys.argv[1:3] == ["bench", os.environ["RDMA_FIXTURE_VERB"]]
for flag, expected in [("--mode", "coroutine"), ("--loader-count", "4"),
    ("--connection-count", "2"), ("--cq-count", "2"), ("--wire-slot-capacity", "1024")]:
    assert sys.argv[sys.argv.index(flag) + 1] == expected
assert os.environ["CROWDB_RDMA_WIRE_SLOT_CAPACITY"] == "1024"
if os.environ.get("RDMA_FIXTURE_SERVER_MODE") == "early": time.sleep(2)
Path(sys.argv[sys.argv.index("--output") + 1]).write_text(os.environ["RDMA_FIXTURE_REPORT"])
sys.exit(int(os.environ["RDMA_FIXTURE_EXIT"]))
''')
        self.client.chmod(0o755)
        self.manifest = self.directory / "cases.json"
        self.manifest.write_text(json.dumps(dict(schema_version=1, cases=[
            dict(case_id="fixture", loader_count=4, connection_count=2, cq_count=2)])))

    def run_case(self, report, client_exit=0, server_mode="ok", transport="rdma"):
        prefix = "RDMA" if transport == "rdma" else "RPC"
        environment = os.environ | {
            f"{prefix}_BENCH_SERVER_BIN": str(self.server),
            f"{prefix}_BENCH_CLI_BIN": str(self.client),
            f"{prefix}_BENCH_LOG_ROOT": str(self.directory / "output"),
            "RDMA_FIXTURE_REPORT": report, "RDMA_FIXTURE_EXIT": str(client_exit),
            "RDMA_FIXTURE_SERVER_MODE": server_mode,
            "RDMA_FIXTURE_VERB": "rdma" if transport == "rdma" else "rpc",
        }
        script = "bench-rdma-rpc-regression.sh" if transport == "rdma" else "bench-rpc-regression.sh"
        process = subprocess.run(
            [shutil.which("pixi"), "run", "--", "bash", f"tools/benchmark/{script}",
             "--manifest", str(self.manifest), "--peak-repeats", "0", "--startup-timeout", "1"],
            cwd=self.root, env=environment, capture_output=True, text=True, timeout=20)
        results = self.directory / "output/results.tsv"
        self.assertTrue(results.exists(), process.stdout + process.stderr)
        with results.open() as stream:
            rows = list(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual(len(rows), 1, process.stdout + process.stderr)
        return process, rows[0]

    @staticmethod
    def report(errors=0, transport="rdma", duplicate_qp=False):
        row = dict(schema_version=1, case_id="fixture", valid=errors == 0, errors_total=errors,
                   actual_path=transport, allow_tcp_fallback=False, fallback_count=0,
                   loader_count=4, connection_count=2, payload_bytes=128,
                   submitted=1100, server_before=dict(rdma=dict(rpc_frames_received=100)),
                   server_after=dict(rdma=dict(rpc_frames_received=1200)),
                   completed_in_window=1000, latency_count=1000, measure_s=10, rpc_tps=100.0,
                   qpair_count=2 if transport == "rdma" else 0,
                   min_ns=1, avg_ns=20.0, p50_ns=20, p90_ns=30, p95_ns=40, p99_ns=50, p999_ns=60, max_ns=100)
        for name in ("report_errors", "setup_errors", "submit_rejects", "timeouts", "connection_errors",
                     "cq_errors", "payload_mismatches", "duplicate_completions", "retries", "histogram_overflow_count"):
            row[name] = 0
        row["client_connections"] = [dict(path=transport, open=True, info=dict(qpair_number=100 + (0 if duplicate_qp else i)))
                                     for i in range(2)]
        return json.dumps(row)

    def test_valid_report_succeeds_and_records_zero_errors(self):
        process, row = self.run_case(self.report())
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(row["errors_total"], "0")
        self.assertEqual(row["report_errors"], "0")
        self.assertEqual(row["valid"], "true")
        self.assertEqual(row["parameters.cq_depth"], "2048")
        self.assertEqual(row["server_exit"], "0")

    def test_tcp_uses_the_same_runner_and_canonical_schema(self):
        process, row = self.run_case(self.report(transport="tcp"), transport="tcp")
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(row["actual_path"], "tcp")
        self.assertEqual(row["valid"], "true")

    def test_failed_client_preserves_reported_error_count(self):
        process, row = self.run_case(self.report(errors=7), client_exit=2)
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(row["errors_total"], "7")
        self.assertEqual(row["client_exit"], "2")
        self.assertEqual(row["valid"], "false")

    def test_missing_report_is_not_success_even_when_client_exits_zero(self):
        process, row = self.run_case("human report")
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(row["errors_total"], "null")
        self.assertEqual(row["report_errors"], "1")
        self.assertEqual(row["valid"], "false")

    def test_error_report_is_failure_even_when_client_exits_zero(self):
        process, row = self.run_case(self.report(errors=3))
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(row["errors_total"], "3")
        self.assertEqual(row["valid"], "false")

    def test_duplicate_qpairs_invalidate_report(self):
        process, row = self.run_case(self.report(duplicate_qp=True))
        self.assertNotEqual(process.returncode, 0)
        self.assertNotEqual(row["report_errors"], "0")
        self.assertEqual(row["valid"], "false")

    def test_forged_success_with_errors_is_rejected(self):
        report = json.loads(self.report(errors=3))
        report["valid"] = True
        process, row = self.run_case(json.dumps(report))
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(row["errors_total"], "3")
        self.assertNotEqual(row["report_errors"], "0")

    def test_server_request_count_must_match_all_accepted_requests(self):
        report = json.loads(self.report())
        report["server_after"]["rdma"]["rpc_frames_received"] += 1
        process, row = self.run_case(json.dumps(report))
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(row["server_request_frames"], "1101")
        self.assertEqual(row["valid"], "false")

    def test_malformed_counter_and_connection_types_retain_a_failure_row(self):
        report = json.loads(self.report())
        report.update(report_errors=None, client_connections=None)
        process, row = self.run_case(json.dumps(report))
        self.assertNotEqual(process.returncode, 0)
        self.assertNotEqual(row["report_errors"], "0")
        self.assertEqual(row["valid"], "false")

    def test_failed_server_never_launches_a_successful_client(self):
        process, row = self.run_case(self.report(), server_mode="exit")
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(row["server_exit"], "3")
        self.assertEqual(row["client_exit"], "null")
        self.assertEqual(row["setup_errors"], "1")

    def test_human_listening_message_cannot_replace_rdma_readiness(self):
        process, row = self.run_case(self.report(), server_mode="no-rdma")
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(row["client_exit"], "null")
        self.assertEqual(row["setup_errors"], "1")

    def test_listening_stdout_without_status_reaches_a_bounded_failure(self):
        process, row = self.run_case(self.report(), server_mode="stdout-only")
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(row["client_exit"], "null")
        self.assertEqual(row["setup_errors"], "1")

    def test_server_exit_during_workload_invalidates_and_stops_client(self):
        process, row = self.run_case(self.report(), server_mode="early")
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(row["server_exit"], "4")
        self.assertEqual(row["valid"], "false")


if __name__ == "__main__":
    unittest.main()
