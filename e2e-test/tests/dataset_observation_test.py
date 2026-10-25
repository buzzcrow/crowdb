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
from dataset_observation_resources import OwnedSources, local_delta, resource_delta, READ_FLOW_COUNTERS, RDMA_COUNTERS
from dataset_observation_phases import summarize_phases

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


def measured_result(concurrency, path="native-rust"):
    samples = [dict(index=index, header_us=1, first_payload_us=2 if size else None,
                    verified_eof_us=3, complete_us=4, bytes=size, nonempty_deliveries=int(size != 0),
                    explicit_consumer_copy_bytes=size if path == "native-python" else 0)
               for _ in range(concurrency * 16) for index, size in enumerate(SIZES)]
    return dict(completed_requests=len(samples), bytes=concurrency * 16 * sum(SIZES), errors=0,
                latency=[dict(bytes=size, count=concurrency * 16, p50_us=4, p99_us=4) for size in SIZES],
                read_phases=dict(schema_version=1, samples=samples))


def messages(path="native-rust"):
    for concurrency, repeat in ORDER:
        trial = dict(run_id=RUN, path=path, concurrency=concurrency, repeat=repeat, snapshot=SNAPSHOT)
        yield dict(v=1, event="ready", trial=trial, observation=local())
        yield dict(v=1, event="done", trial=trial, observation=local(2),
                   result=measured_result(concurrency, path))


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


class TestPhases(unittest.TestCase):
    def test_fixed_matrix_keeps_original_completion_and_no_first_payload_for_empty(self):
        for path in ("http", "native-rust", "native-python"):
            for concurrency in (1, 3):
                result = measured_result(concurrency, path)
                summary = summarize_phases(result, path, concurrency, SIZES)
                self.assertEqual(sum(row["count"] for row in summary["sizes"]), concurrency * 128)
                empty = summary["sizes"][0]["cumulative_from_start"]["first_payload_us"]
                self.assertEqual(empty, dict(count=0, p50_us=None, p99_us=None))
                self.assertEqual(summary["explicit_consumer_copy_bytes"], result["bytes"] if path == "native-python" else 0)
                self.assertIn("not_additive", summary["timing"])

    def test_missing_phase_bad_order_copy_counts_and_boolean_timings_are_rejected(self):
        changes = [lambda result: result.pop("read_phases"),
                   lambda result: result["read_phases"].update(schema_version=True),
                   lambda result: result["read_phases"]["samples"].pop(),
                   lambda result: result["read_phases"]["samples"][1].update(first_payload_us=None),
                   lambda result: result["read_phases"]["samples"][0].update(first_payload_us=0),
                   lambda result: result["read_phases"]["samples"][1].update(verified_eof_us=0),
                   lambda result: result["read_phases"]["samples"][1].update(complete_us=2),
                   lambda result: result["read_phases"]["samples"][1].update(explicit_consumer_copy_bytes=128),
                   lambda result: result["read_phases"]["samples"][1].update(header_us=True),
                   lambda result: result["latency"][0].update(p50_us=True)]
        for change in changes:
            result = measured_result(1)
            change(result)
            with self.subTest(change=change), self.assertRaises(ValueError):
                summarize_phases(result, "native-rust", 1, SIZES)

    def test_exact_read_flow_gauges_and_rdma_deltas_do_not_become_all_copy_or_trial_peaks(self):
        first, last = local(1), local(3)
        for index, value in enumerate((first, last), 1):
            value["resources"]["read_flow"] = {key: index for key in READ_FLOW_COUNTERS}
            value["resources"].update(requests=0, producer_bytes=0, producer_bytes_high_water=4096, held_bytes=0)
            for group in ("metadata", "aggregate"):
                value["topology"]["metadata"][group]["transport"]["rdma"] = dict(
                    ready=True, device="fake-hca", gid="fake-gid", generation=1, protocol_version=1,
                    wire_layout="control_data", **{key: index * 10 for key in RDMA_COUNTERS})
        result = local_delta(first, last)
        self.assertEqual(result["read_flow"]["values"]["layout_queries"], 1)
        self.assertEqual(result["read_flow"]["values"]["stream_credit_wait_ns"], 1)
        self.assertEqual(result["groups"]["metadata"]["rdma"]["values"]["rpc_frames_received"], 10)
        self.assertEqual(result["resource_observations"]["after"]["held_bytes_high_water"], dict(value=4096))
        self.assertIn("per_trial_held_peak", result["not_measured"])
        del last["resources"]["read_flow"]["layout_queries"]
        last["resources"]["read_flow"]["stream_credit_wait_ns"] = 0
        last["topology"]["metadata"]["metadata"]["transport"]["rdma"]["generation"] = 2
        result = local_delta(first, last)
        self.assertIn("layout_queries", result["read_flow"]["unavailable"])
        self.assertIn("stream_credit_wait_ns", result["read_flow"]["unavailable"])
        self.assertIn("unavailable", result["groups"]["metadata"]["rdma"])
        self.assertIn("unavailable", result["groups"]["data"]["rdma"])

    def python_read(self, size_index, *, phases, fail_at_eof=False, corrupt=False):
        source = ROOT / "lib/crowdb-sdk-native-python/tests/workload.py"
        node = next(node for node in ast.parse(source.read_text()).body
                    if isinstance(node, ast.FunctionDef) and node.name == "read")
        clock = TestClock(1000)
        snapshot, size = bytes.fromhex(SNAPSHOT), SIZES[size_index]
        data = bytes([size_index]) * size
        class Lease:
            def __init__(self, data): self.data = data
            def __len__(self): return len(self.data)
            def to_bytes(self): return b"wrong" if corrupt else self.data
        class Batch:
            header = json.dumps(dict(snapshot=list(snapshot), fields=[{}]))
            def __init__(self): self.values = [Lease(data)] if size else []
            def next(self):
                if self.values: return self.values.pop(0)
                if fail_at_eof: raise RuntimeError("late EOF failure")
                return None
        namespace = dict(time=SimpleNamespace(perf_counter_ns=clock), json=json, SIZES=SIZES)
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec"), namespace)
        client = SimpleNamespace(read_batch=lambda *args: Batch())
        return namespace["read"](client, snapshot, size_index, phases), clock.value // 1000

    def test_actual_python_reader_opt_in_clock_points_copy_and_standalone(self):
        plain, calls = self.python_read(1, phases=False)
        self.assertIsInstance(plain, int)
        self.assertEqual(calls, 2)
        (complete, phase), calls = self.python_read(1, phases=True)
        self.assertEqual(calls, 5)
        self.assertEqual((phase["header_us"], phase["first_payload_us"], phase["verified_eof_us"], complete), (1, 2, 3, 4))
        self.assertEqual(phase["explicit_consumer_copy_bytes"], SIZES[1])
        (_, empty), calls = self.python_read(0, phases=True)
        self.assertEqual(calls, 4)
        self.assertIsNone(empty["first_payload_us"])
        with self.assertRaisesRegex(RuntimeError, "EOF"):
            self.python_read(1, phases=True, fail_at_eof=True)
        with self.assertRaises(AssertionError):
            self.python_read(1, phases=True, corrupt=True)


if __name__ == "__main__":
    unittest.main()
