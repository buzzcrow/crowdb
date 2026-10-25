# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Measurement ownership guards; these tests do not certify OCI deployments."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "measurement_fixture", Path(__file__).resolve().parents[2] / "tools/benchmark/fixture.py")
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


class RegressionFixtureTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "console.toml"
        self.descriptor = self.state.with_suffix(".fixture.json")
        self.value = {"pid": os.getpid(), "start": fixture.process_start(os.getpid()),
                      "endpoint": "http://127.0.0.1:1", "token": "owner", "root": str(self.root)}

    def write_descriptor(self):
        self.descriptor.write_text(json.dumps(self.value))
        self.descriptor.chmod(0o600)

    def test_current_process_identity_is_required(self):
        self.write_descriptor()
        self.assertEqual(fixture.descriptor(self.state), self.value)
        self.value["start"] = "foreign-start"
        self.write_descriptor()
        with self.assertRaises(fixture.FixtureStopped):
            fixture.descriptor(self.state)

    def test_descriptor_rejects_public_file_symlink_and_oversized_file(self):
        self.write_descriptor()
        self.descriptor.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "private regular"):
            fixture.descriptor(self.state)
        self.descriptor.unlink()
        self.descriptor.symlink_to(self.state)
        with self.assertRaisesRegex(ValueError, "private regular"):
            fixture.descriptor(self.state)
        self.descriptor.unlink()
        self.write_descriptor()
        self.descriptor.write_text(" " * 65537)
        with self.assertRaisesRegex(ValueError, "bounded private"):
            fixture.descriptor(self.state)

    def test_dead_owner_cannot_spawn_a_fixture(self):
        args = argparse.Namespace(state=self.state, owner_pid=None, arguments=["--kind", "kv"])
        with patch.object(fixture.subprocess, "Popen") as spawn:
            with self.assertRaisesRegex(ValueError, "live measurement owner"):
                fixture.setup(args)
            spawn.assert_not_called()

    def test_failed_client_drops_stale_report_and_does_not_inherit_cluster_target(self):
        self.write_descriptor()
        report = self.root / "client-runtime/artifacts/cli/bench-kv/report.json"
        report.parent.mkdir(parents=True)
        report.write_text('{"total_ops": 999}')
        args = argparse.Namespace(state=self.state, action="client", arguments=["bench", "kv", "bad"])
        with patch.dict(os.environ, {"CROWDB_CLUSTER": "foreign"}), \
                patch.object(fixture.subprocess, "call", return_value=2) as call:
            self.assertEqual(fixture.run(args), 2)
            self.assertNotIn("CROWDB_CLUSTER", call.call_args.kwargs["env"])
            self.assertEqual(call.call_args.args[0][1:3],
                             ["--management-endpoint", self.value["endpoint"]])
        self.assertFalse(report.exists())
        self.assertFalse((self.root / "measurements").exists(), "failed parsing retained stale logs")

    def test_invalid_benchmark_family_cannot_delete_arbitrary_directories(self):
        self.write_descriptor()
        retained = self.root / "client-runtime/artifacts/cli/keep"
        retained.mkdir(parents=True)
        witness = retained / "witness"
        witness.write_text("preserved")
        args = argparse.Namespace(state=self.state, action="client",
                                  arguments=["bench", "kv/../keep"])
        with patch.object(fixture.subprocess, "call", return_value=2):
            self.assertEqual(fixture.run(args), 2)
        self.assertEqual(witness.read_text(), "preserved")

    def test_stop_with_stale_identity_never_sends_a_control_request(self):
        self.value["start"] = "foreign-start"
        self.write_descriptor()
        args = argparse.Namespace(state=self.state, action="stop")
        with patch.object(fixture.HTTP, "open") as request:
            self.assertEqual(fixture.run(args), 0)
            request.assert_not_called()

    def check_service_logs(self):
        script = ("source \"$1\"\n"
                  "regression_fixture_log_root() { printf '%s\\n' \"$TEST_FIXTURE_ROOT\"; }\n"
                  "regression_require_no_service_errors\n")
        common = Path(__file__).resolve().parents[2] / "tools/benchmark/bench-regression-common.sh"
        return subprocess.run(["bash", "-c", script, "log-validation", str(common)],
            env=dict(os.environ, REGRESSION_LOG_ROOT=str(self.root), TEST_FIXTURE_ROOT=str(self.root)),
            capture_output=True, text=True, timeout=5)

    def test_service_log_scan_excludes_binary_error_strings(self):
        services = self.root / "services"
        services.mkdir()
        (services / "server").write_bytes(b"\0DiskNotExist\0")
        (services / "server.log").write_text("normal lifecycle\n")
        result = self.check_service_logs()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_service_log_errors_are_printed_and_fail_validation(self):
        services = self.root / "services"
        services.mkdir()
        (services / "server.log").write_text("DiskNotExist: lost registered disk\n")
        result = self.check_service_logs()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("DiskNotExist: lost registered disk", result.stdout)

    def test_service_log_scan_failure_is_not_treated_as_no_errors(self):
        result = self.check_service_logs()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("grep status 2", result.stderr)


# Exercise the exact inline validator shipped with the benchmark script.
STREAM_SCRIPT = Path(__file__).resolve().parents[2] / "tools/benchmark/bench-chunk-stream-regression.sh"
STREAM_PROGRAM = STREAM_SCRIPT.read_text().split("<<'PY_STREAM_REGRESSION'\n", 1)[1].split("\nPY_STREAM_REGRESSION\n", 1)[0]
regression = types.ModuleType("stream_regression")
exec(compile(STREAM_PROGRAM, str(STREAM_SCRIPT), "exec"), regression.__dict__)


def stream_transcript():
    result = dict(workload="append", operations=4000, bytes=16384000, errors=0, seconds=10,
                  ops_s=400, mib_s=1.56, avg_us=2000, p50_us=1900, p99_us=3000,
                  rss_start_kib=1024, rss_end_kib=2048, rss_peak_kib=3072,
                  max_queue_requests=1, max_queue_bytes=4096, batches=4000, batch_requests=4000,
                  rollovers=0, metadata_publications=1, cache_hits=0, cache_misses=0,
                  physical_reads=0, reclaimed_bytes=0, watchdogs=0)
    phases = {name: 0 for name in regression.REQUIRED_PHASES}
    phases.update({"stream.metadata.publish.lh": 1, "stream.append.batch.lh": 4000,
                   "stream.append.mirror.lh": 4000, "chunkio.diskio.write.e2e.lh": 8000})
    return ("bench-runtime: pid=123 allowed_cpus=0-3 tokio_workers=32\n"
            + "chunk-stream: " + " ".join(f"{key}={value}" for key, value in result.items()) + "\n"
            + "\n".join(f"stream-phase: name={name} count={count} avg_us=0 p50_us=0 p99_us=0 max_us=0"
                        for name, count in sorted(phases.items())))


class StreamRegressionTest(unittest.TestCase):
    def analyze(self, text, **kwargs):
        return regression.analyze(text, "append_4k_1t", 10, **kwargs)

    def test_complete_workload_passes_and_retains_all_distributions(self):
        value = self.analyze(stream_transcript())
        self.assertTrue(value["passed"], value["checks"])
        self.assertEqual(value["phases"]["stream.metadata.publish.lh"]["count"], 1)
        self.assertIn("stream.chunk.seal_drain.lh", value["phases"])

    def test_missing_duplicate_or_nonfinite_results_cannot_pass(self):
        for text in ("", stream_transcript() + "\n" + stream_transcript(),
                     stream_transcript().replace(" p99_us=3000", ""),
                     stream_transcript().replace("ops_s=400", "ops_s=nan"),
                     stream_transcript().replace("errors=0", "errors=-1")):
            with self.subTest(text=text[:100]), self.assertRaises(ValueError):
                self.analyze(text)

    def test_missing_phase_is_not_interpreted_as_zero_checkpoint_work(self):
        text = "\n".join(line for line in stream_transcript().splitlines()
                         if "stream.checkpoint.cursor.lh" not in line)
        with self.assertRaisesRegex(ValueError, "missing phases"):
            self.analyze(text)

    def test_publication_cursor_and_required_mirror_guards_fail(self):
        for before, after, check in (
                ("metadata_publications=1", "metadata_publications=0", "publications"),
                ("name=stream.checkpoint.cursor.lh count=0", "name=stream.checkpoint.cursor.lh count=1",
                 "no_cursor_checkpoint"),
                ("name=chunkio.diskio.write.e2e.lh count=8000",
                 "name=chunkio.diskio.write.e2e.lh count=4000", "mirror_writes")):
            with self.subTest(check=check):
                value = self.analyze(stream_transcript().replace(before, after))
                self.assertFalse(value["passed"])
                self.assertFalse(value["checks"][check])

    def test_short_or_failed_runs_and_original_performance_bounds_fail(self):
        for before, after, check in (("seconds=10", "seconds=0.1", "full_duration"),
                                     ("ops_s=400", "ops_s=249", "throughput"),
                                     ("p99_us=3000", "p99_us=10001", "p99"),
                                     ("rss_peak_kib=3072", "rss_peak_kib=300000", "rss")):
            with self.subTest(check=check):
                value = self.analyze(stream_transcript().replace(before, after))
                self.assertFalse(value["passed"])
                self.assertFalse(value["checks"][check])
        self.assertFalse(self.analyze(stream_transcript(), status=124)["passed"])

    def test_trim_may_checkpoint_its_retained_boundary(self):
        text = stream_transcript().replace("workload=append", "workload=gc")
        text = text.replace("mib_s=1.56", "mib_s=2000").replace("reclaimed_bytes=0", "reclaimed_bytes=1048576")
        text = text.replace("name=stream.checkpoint.cursor.lh count=0", "name=stream.checkpoint.cursor.lh count=1")
        value = regression.analyze(text, "gc_576m", 10)
        self.assertTrue(value["passed"], value["checks"])

    def test_missing_case_receipt_cannot_disappear_from_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "conditions.json").write_text(json.dumps(dict(cases=["append_4k_1t"])))
            self.assertEqual(regression.summarize(argparse.Namespace(root=root, results=root / "results.tsv")), 1)
            self.assertFalse(json.loads((root / "results.json").read_text())["complete"])

    def test_failed_cleanup_cannot_pass_a_successful_workload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "workload.log"
            log.write_text(stream_transcript())
            (root / "cpu.json").write_text("{}")
            (root / "console.fixture-stopped.json").write_text('{"ok":false,"error":"stale owner"}')
            args = argparse.Namespace(log=log, label="append_4k_1t", duration=10, status=0)
            self.assertEqual(regression.record(args), 1)
            self.assertFalse(json.loads((root / "result.json").read_text())["checks"]["cleanup"])
