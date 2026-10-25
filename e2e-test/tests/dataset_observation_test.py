# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Pure fake-process/fake-proc checks; never launch a workload or Docker."""

import ast
import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/sdk"))
from dataset_observation import ENVIRONMENT, LINE_LIMIT, ORDER, PREFIX, SIZES, ObservationError, _Child, run_observed
from dataset_observation_resources import OwnedSources, local_delta, resource_delta

RUN, SNAPSHOT = "1" * 32, "2" * 32


class TestClock:
    def __init__(self, step=1):
        self.value, self.step = 0, step

    def __call__(self):
        self.value += self.step
        return self.value


def transport(value, pid=42):
    return dict(schema_version=1, process_id=pid, io_engines=1, io_workers=1,
                send_queue_capacity=1024, tcp_nodelay=True, tcp_quickack=False, event_write=True,
                allow_tcp_fallback=False, rdma_compiled=True, rdma=None,
                cpu_user_ns=100_000 * value, cpu_system_ns=2_000 * value,
                rpc_frames_sent=value, wire_bytes_sent=100 * value,
                writev_calls=value, writev_progress_calls=value, send_queue_rejects=0)


def local(value=1, python=False):
    topology = dict(metadata=dict(schema_version=1,
                    metadata=dict(transport=transport(value)), aggregate=dict(transport=transport(2 * value))),
                    data=transport(3 * value))
    if python:
        topology.update(catalog_age_ms=23, authoritative=False)
    return dict(started_unix_ns="1000", finished_unix_ns="2000", topology=topology,
                resources=dict(held_bytes=0, held_bytes_high_water=4096,
                               compatibility_copy_bytes=value), unavailable={})


def messages(path="native-rust"):
    for concurrency, repeat in ORDER:
        trial = dict(run_id=RUN, path=path, concurrency=concurrency, repeat=repeat, snapshot=SNAPSHOT)
        yield dict(v=1, event="ready", trial=trial, observation=local())
        yield dict(v=1, event="done", trial=trial, observation=local(2),
                   result=dict(completed_requests=concurrency * 16 * len(SIZES),
                               bytes=concurrency * 16 * sum(SIZES), errors=0))


def encoded(record):
    return PREFIX + json.dumps(record, ensure_ascii=False).encode() + b"\n"


class TestChild:
    pid = 42

    def __init__(self, records=None, code=0, tail=b"", chunks=None):
        self.chunks = list(chunks) if chunks is not None else [encoded(row) for row in (
            messages() if records is None else records)]
        self.code, self.tail, self.replies, self.closed = code, tail, [], False

    def read(self, _timeout):
        return self.chunks.pop(0) if self.chunks else b""

    def send(self, record):
        self.replies.append(record)

    def wait(self, _timeout):
        return self.code

    def close(self, log):
        self.closed = True
        log.write(self.tail)


class TestProtocol(unittest.TestCase):
    def run_child(self, child, directory, **options):
        self.captures = []

        def capture(pid):
            self.assertEqual(pid, 42)
            value = len(self.captures)
            result = dict(nodes={}, client=dict(pid=42, starttime=77, cpu_ticks=value))
            self.captures.append(result)
            return result

        def factory(command, environment):
            self.assertEqual(command, ["fake-child"])
            self.assertEqual(environment[ENVIRONMENT], RUN)
            return child

        return run_observed(["fake-child"], environment={}, output=directory, path="native-rust",
                            run_id=RUN, capture=capture, child_factory=factory,
                            clock=options.pop("clock", TestClock()), **options)

    def test_six_boundaries_preserve_original_counts_and_explicit_nonadditive_scope(self):
        child = TestChild()
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_child(child, directory)
            self.assertTrue(result["complete"])
            self.assertEqual(len(result["trials"]), 6)
            self.assertEqual(len(self.captures), 12)
            self.assertEqual([row["event"] for row in child.replies], ["go", "ack"] * 6)
            self.assertEqual(sum(row["result"]["completed_requests"] for row in result["trials"]), 1536)
            self.assertIn("not_additive", result["scope"])
            self.assertEqual(json.loads(Path(directory, "native-rust-observations.json").read_text()), result)
        self.assertTrue(child.closed)

    def test_fragmented_unicode_and_ordinary_output_are_continuously_preserved(self):
        rows = list(messages())
        rows[0]["observation"]["unavailable"]["extra"] = "雪"
        raw = b"ordinary report\n" + b"".join(encoded(row) for row in rows) + b"ordinary EOF without newline"
        child = TestChild(chunks=[raw[index:index + 17] for index in range(0, len(raw), 17)])
        with tempfile.TemporaryDirectory() as directory:
            self.run_child(child, directory)
            self.assertEqual(Path(directory, "native-rust.log").read_bytes(), raw)

    def test_bad_identity_order_schema_and_missing_snapshot_fail_instead_of_reusing_counts(self):
        for key, value in (("run_id", "3" * 32), ("path", "http"), ("repeat", 1),
                           ("snapshot", "bad"), ("concurrency", True)):
            rows = list(messages())
            rows[0]["trial"] = dict(rows[0]["trial"], **{key: value})
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(ObservationError):
                    self.run_child(TestChild(rows), directory)
        for change in (lambda row: row.update(v=True), lambda row: row.pop("observation"),
                       lambda row: row.update(event="done")):
            rows = list(messages())
            change(rows[0])
            with tempfile.TemporaryDirectory() as directory, self.assertRaises(ObservationError):
                self.run_child(TestChild(rows), directory)

    def test_incomplete_child_failure_and_cleanup_keep_last_raw_evidence(self):
        child = TestChild([next(messages())], code=7, tail=b"failure after termination\n")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ObservationError):
                self.run_child(child, directory)
            result = json.loads(Path(directory, "native-rust-observations.json").read_text())
            self.assertFalse(result["complete"])
            self.assertIn("after_failure", result["active"])
            self.assertIn(b"failure after termination", Path(directory, "native-rust.log").read_bytes())
        self.assertTrue(child.closed)

    def test_oversized_missing_newline_timeout_and_nonzero_exit_are_not_success(self):
        cases = [TestChild(chunks=[PREFIX + b"x" * LINE_LIMIT]),
                 TestChild(chunks=[encoded(next(messages())).rstrip(b"\n")]),
                 TestChild(code=7)]
        for child in cases:
            with tempfile.TemporaryDirectory() as directory, self.assertRaises(ObservationError):
                self.run_child(child, directory)
            self.assertTrue(child.closed)
        child = TestChild(chunks=[None])
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(ObservationError, "bound"):
            self.run_child(child, directory, timeout=2, clock=TestClock(1_000_000_000))
        self.assertTrue(child.closed)

    def test_changed_completions_and_future_status_are_explicit(self):
        rows = list(messages())
        rows[1]["result"]["completed_requests"] -= 1
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(ObservationError, "contract"):
            self.run_child(TestChild(rows), directory)
        first, last = local(), local(2)
        for group in ("metadata", "aggregate"):
            last["topology"]["metadata"][group]["transport"]["schema_version"] = 2
        delta = local_delta(first, last)
        self.assertIn("unavailable", delta["groups"]["metadata"])

    def test_shared_snapshot_and_observation_timestamp_shape_are_checked(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(ObservationError):
            self.run_child(TestChild(), directory, expected_snapshot="3" * 32)
        rows = list(messages())
        rows[0]["observation"]["started_unix_ns"] = 1000
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(ObservationError):
            self.run_child(TestChild(rows), directory)

    def test_launch_failure_keeps_an_artifact_and_cleanup_drains_after_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(OSError, "fake launch"):
                run_observed(["fake"], environment={}, output=directory, path="http", run_id=RUN,
                             capture=lambda _pid: None,
                             child_factory=Mock(side_effect=OSError("fake launch")))
            self.assertFalse(json.loads(Path(directory, "http-observations.json").read_text())["complete"])
        child = object.__new__(_Child)
        child.pid, child.selector = 42, SimpleNamespace(close=Mock())
        child.process = SimpleNamespace(poll=lambda: None, stdin=io.BytesIO(), stdout=io.BytesIO(),
                                        wait=Mock(side_effect=[subprocess.TimeoutExpired("fake", 5), 0]))
        child.read = Mock(side_effect=[None, b"last error\n", b""])
        log = io.BytesIO()
        with patch("dataset_observation.os.killpg") as kill:
            child.close(log)
            self.assertEqual(kill.call_count, 2)
        self.assertEqual(log.getvalue(), b"last error\n")
        self.assertTrue(child.selector.close.called)


class TestCounters(unittest.TestCase):
    def test_rust_and_python_facades_keep_selected_groups_without_summing_rusage(self):
        for python in (False, True):
            result = local_delta(local(1, python), local(3, python))
            self.assertEqual([result["groups"][key]["values"]["rpc_frames_sent"]
                              for key in ("metadata", "aggregate", "data")], [2, 4, 6])
            self.assertEqual(result["groups"]["data"]["values"]["wire_bytes_sent"], 600)
            self.assertNotIn("cpu_user_ns", json.dumps(result))
            self.assertIn("receive_bytes", result["groups"]["data"]["not_measured"])
            self.assertEqual(result["resources"]["values"], {"compatibility_copy_bytes": 2})
            self.assertIn("per_trial_held_peak", result["not_measured"])

    def test_missing_reset_and_changed_transport_configuration_never_become_zero(self):
        first, last = local(), local(2)
        last["topology"]["data"]["wire_bytes_sent"] = 0
        del last["topology"]["data"]["writev_calls"]
        last["topology"]["metadata"]["metadata"]["transport"]["io_workers"] = 2
        result = local_delta(first, last)
        self.assertIn("wire_bytes_sent", result["groups"]["data"]["unavailable"])
        self.assertIn("writev_calls", result["groups"]["data"]["unavailable"])
        self.assertIn("unavailable", result["groups"]["metadata"])
        http = dict(topology=None, resources=None, unavailable={"topology": "http_sdk_not_exposed"})
        self.assertTrue(all("unavailable" in item for item in local_delta(http, http)["groups"].values()))

    def test_owned_source_uses_real_member_rss_not_tini_and_detects_pid_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = TestFiles(Path(directory))
            source = fixture.discover()
            before = source.capture(42)
            node = before["nodes"]["owned"]
            self.assertEqual([row["rss_kib"] for row in node["processes"]], [10, 321])
            self.assertEqual(node["memory.current"]["bytes"], 800_000)
            self.assertEqual(node["processes"][1]["lifetime_peak_rss_kib"], 999)
            fixture.cpu.write_text("usage_usec 130\nuser_usec 80\nsystem_usec 50\n")
            after = source.capture(42)
            self.assertEqual(resource_delta(before, after)["nodes"]["owned"]["values"],
                             dict(usage_usec=30, user_usec=20, system_usec=10))
            fixture.process(101, 900, 10)
            self.assertIn("unavailable", resource_delta(before, source.capture(42))["nodes"]["owned"])

    def test_wrong_owner_image_name_missing_counters_and_process_limit_are_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = TestFiles(Path(directory))
            for key, value in (("Image", "wrong"), ("Name", "/other"), ("Id", ""), ("Mounts", [])):
                record = copy.deepcopy(fixture.record)
                record[key] = value
                with self.subTest(key=key), self.assertRaises(ValueError):
                    fixture.discover(record)
            record = copy.deepcopy(fixture.record)
            record["Config"]["Labels"]["org.crowdb.e2e.run"] = "wrong"
            with self.assertRaises(ValueError):
                fixture.discover(record)
            source = fixture.discover()
            first = source.capture(42)
            fixture.cpu.write_text("usage_usec 1\n")
            second = source.capture(42)
            self.assertEqual(set(resource_delta(first, second)["nodes"]["owned"]["unavailable"]),
                             {"usage_usec", "user_usec", "system_usec"})
            fixture.cpu.unlink()
            fixture.group.joinpath("cgroup.procs").write_text("1 " * 257)
            third = source.capture(42)["nodes"]["owned"]
            self.assertIn("cpu.stat", third["unavailable"])
            self.assertIn("processes", third["unavailable"])

    def test_permissions_membership_and_missing_rss_stay_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = TestFiles(Path(directory))
            source = fixture.discover()
            first = source.capture(42)
            original_read = Path.read_text

            def read(path, *args, **kwargs):
                if path == fixture.cpu:
                    raise PermissionError("fake denied")
                return original_read(path, *args, **kwargs)

            with patch.object(Path, "read_text", read):
                after = source.capture(42)
            self.assertIn("PermissionError", after["nodes"]["owned"]["unavailable"]["cpu.stat"])
            self.assertEqual(resource_delta(first, after)["nodes"]["owned"]["values"], {})
            (fixture.proc / "101/cgroup").write_text("0::/other\n")
            (fixture.proc / "202/status").write_text("Name: fixture\n")
            after = source.capture(42)
            self.assertIn("unavailable", resource_delta(first, after)["nodes"]["owned"])
            self.assertIn("rss_kib", after["nodes"]["owned"]["processes"][1]["unavailable"])


class TestFiles:
    def __init__(self, root):
        self.proc, self.groups = root / "proc", root / "cgroups"
        self.group = self.groups / "owned"
        self.group.mkdir(parents=True)
        self.cpu = self.group / "cpu.stat"
        self.cpu.write_text("usage_usec 100\nuser_usec 60\nsystem_usec 40\n")
        for name, value in (("memory.current", "800000"), ("memory.peak", "900000"),
                            ("memory.stat", "anon 700000\nfile 100000\n"), ("memory.events", "oom 0\n"),
                            ("cgroup.procs", "101 202")):
            (self.group / name).write_text(value)
        for pid, rss in ((101, 10), (202, 321), (42, 500)):
            self.process(pid, pid + 100, rss)
        self.record = dict(Id="container-id", Name="/owned", Image="image-id",
                           Config=dict(Labels={"org.crowdb.e2e.run": "fixture-run"}),
                           Mounts=[dict(Source=str(root / "data"), Destination="/opt/crowdb/data")],
                           State=dict(Pid=101, Running=True))
        self.data = root / "data"

    def process(self, pid, starttime, rss):
        root = self.proc / str(pid)
        root.mkdir(parents=True, exist_ok=True)
        fields = ["0"] * 22
        fields[0], fields[11], fields[12], fields[19] = "S", "10", "20", str(starttime)
        (root / "stat").write_text(f"{pid} (name with space) " + " ".join(fields))
        (root / "status").write_text(f"Name:\tfixture\nVmRSS:\t{rss} kB\nVmHWM:\t999 kB\n")
        (root / "cgroup").write_text("0::/owned\n")

    def discover(self, record=None):
        return OwnedSources.discover(lambda *args: json.dumps([record or self.record]), ["owned"],
                                     "fixture-run", "image-id", [self.data], proc=self.proc, groups=self.groups,
                                     clock=TestClock())


class TestRealPythonChildHelpers(unittest.TestCase):
    def helpers(self):
        source = ROOT / "lib/crowdb-sdk-native-python/tests/workload.py"
        tree = ast.parse(source.read_text())
        names = {"observation_trial", "observe", "local_observation"}
        functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        self.assertEqual({node.name for node in functions}, names)
        namespace = dict(os=os, json=json, sys=SimpleNamespace(stdin=io.StringIO(), stdout=io.StringIO()),
                         time=SimpleNamespace(time_ns=lambda: 1234))
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), "exec"), namespace)
        return namespace

    def test_standalone_never_reads_stdin_or_takes_snapshot_and_opt_in_matches_host_protocol(self):
        namespace = self.helpers()
        for value in (None, ""):
            environment = {} if value is None else {ENVIRONMENT: value}
            with patch.dict(os.environ, environment, clear=True):
                self.assertIsNone(namespace["observation_trial"](SNAPSHOT, 1, 0))
                namespace["observe"](None, "ready", "go", None)
                self.assertEqual(namespace["sys"].stdout.getvalue(), "")
        with patch.dict(os.environ, {ENVIRONMENT: RUN}, clear=True):
            trial = namespace["observation_trial"](SNAPSHOT, 1, 0)
        namespace["sys"].stdin = io.StringIO(json.dumps(dict(v=1, event="go", trial=trial)) + "\n")
        runtime = SimpleNamespace(topology=lambda **kw: json.dumps(local(python=True)["topology"]),
                                  status=lambda: json.dumps(local()["resources"]))
        namespace["observe"](trial, "ready", "go", runtime)
        record = json.loads(namespace["sys"].stdout.getvalue().encode()[len(PREFIX):])
        self.assertEqual(record["trial"], next(messages("native-python"))["trial"])
        self.assertEqual(record["observation"]["topology"]["catalog_age_ms"], 23)

    def test_snapshot_error_is_independent_and_bad_reply_fails_without_retry(self):
        namespace = self.helpers()

        def broken(**_kwargs):
            raise PermissionError("local status unavailable")

        runtime = SimpleNamespace(topology=broken, status=lambda: json.dumps(local()["resources"]))
        observation = namespace["local_observation"](runtime)
        self.assertIsNone(observation["topology"])
        self.assertIn("topology", observation["unavailable"])
        self.assertIsNotNone(observation["resources"])
        trial = next(messages("native-python"))["trial"]
        for line in ("", "{}\n", json.dumps(dict(v=1, event="ack", trial=trial)) + "\n"):
            namespace["sys"].stdin = io.StringIO(line)
            with self.assertRaises(ValueError):
                namespace["observe"](trial, "ready", "go", runtime)

    def test_actual_child_control_line_limit_rejects_before_writing_a_partial_record(self):
        namespace = self.helpers()
        trial = next(messages("native-python"))["trial"]
        runtime = SimpleNamespace(topology=lambda **_kw: json.dumps(dict(extra="x" * LINE_LIMIT)),
                                  status=lambda: json.dumps(local()["resources"]))
        with self.assertRaisesRegex(ValueError, "line limit"):
            namespace["observe"](trial, "ready", "go", runtime)
        self.assertEqual(namespace["sys"].stdout.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
