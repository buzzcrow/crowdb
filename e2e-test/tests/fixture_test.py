# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Failure ownership and resource contracts without a container daemon."""

import json
import tempfile
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from fixture import LABEL, Cluster, redact, docker
from run import capacity
from allocation import start_node


class Daemon:
    def __init__(self):
        self.resources = {("container", "unrelated"): "other-run"}
        self.fail_cleanup = False

    def __call__(self, *args, **_kwargs):
        if args[:2] == ("network", "create") or args[:2] == ("volume", "create"):
            self.resources[(args[0], args[-1])] = args[args.index("--label") + 1].split("=", 1)[1]
            return args[-1]
        if args[0] == "run":
            name = args[args.index("--name") + 1]
            self.resources[("container", name)] = args[args.index("--label") + 1].split("=", 1)[1]
            raise RuntimeError("first server failed after allocation")
        if args[1] == "ls":
            owner = next(value.split("=", 2)[2] for value in args if value.startswith("label="))
            return "\n".join(name for (kind, name), label in self.resources.items() if kind == args[0] and label == owner)
        if args[1] == "rm":
            if self.fail_cleanup:
                raise RuntimeError("daemon cleanup unavailable")
            del self.resources[(args[0], args[-1])]
        return ""


class FixtureTest(unittest.TestCase):
    def test_deferred_node_records_owned_static_address_without_starting(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory)
            calls = []
            address = "10.20.30.4"
            info = [{"NetworkSettings": {"Networks": {cluster.run_id: {
                "IPAddress": "", "IPAMConfig": {"IPv4Address": address}}}}}]
            def command(*args):
                calls.append(args)
                return json.dumps(info) if args[0] == "inspect" else "created"
            cluster.command = command
            self.assertEqual(start_node(cluster, 2, Path(directory), ("server", "diskio"),
                                        address, running=False, pids_limit=512), address)
            self.assertEqual(calls[0][0], "create")
            self.assertEqual(calls[0][calls[0].index("--pids-limit") + 1], "512")
            self.assertEqual(cluster.servers, cluster.containers)
            self.assertIn(f"{LABEL}={cluster.run_id}", calls[0])
            info[0]["NetworkSettings"]["Networks"][cluster.run_id]["IPAMConfig"]["IPv4Address"] = "other"
            with self.assertRaisesRegex(AssertionError, "explicit network address"):
                start_node(cluster, 3, Path(directory), (), address, running=False)
            with self.assertRaisesRegex(ValueError, "explicit advertised address"):
                start_node(cluster, 4, Path(directory), (), running=False)

    def test_node_applies_explicit_security_options(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory)
            cluster.security_options = ("seccomp=unconfined",)
            calls = []
            info = [{"NetworkSettings": {"Networks": {cluster.run_id: {"IPAddress": "10.20.30.4"}}}}]

            def command(*args):
                calls.append(args)
                return json.dumps(info) if args[0] == "inspect" else "created"

            cluster.command = command
            start_node(cluster, 0, Path(directory), (), "10.20.30.4")
            self.assertIn(("--security-opt", "seccomp=unconfined"),
                          tuple(zip(calls[0], calls[0][1:])))

    def test_invalid_pid_budget_fails_before_allocating_a_node(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory)
            with patch.object(cluster, "command") as command:
                for limit in (0, -1, 4097, True, "512"):
                    with self.assertRaisesRegex(ValueError, "PID limit"):
                        start_node(cluster, 0, None, (), pids_limit=limit)
                command.assert_not_called()
                self.assertEqual(cluster.containers, [])

    def test_logs_keep_stderr_and_inspection_keeps_machine_readable_stdout(self):
        completed = subprocess.CompletedProcess([], 0, '{"status":"ok"}', "fatal server error")
        with patch("fixture.subprocess.run", return_value=completed):
            self.assertIn("fatal server error", docker("logs", "owned-node"))
            self.assertEqual(json.loads(docker("inspect", "owned-node")), {"status": "ok"})

    def test_partial_start_removes_allocated_resources_only(self):
        with tempfile.TemporaryDirectory() as directory:
            daemon = Daemon()
            cluster = Cluster("image", directory, daemon)
            with self.assertRaisesRegex(RuntimeError, "first server failed"), cluster:
                self.fail("failed startup must never enter test body")
            self.assertEqual(daemon.resources, {("container", "unrelated"): "other-run"})
            self.assertTrue(cluster.closed)
            self.assertTrue(list(Path(directory).rglob("*logs.log")))

    def test_cleanup_failure_preserves_the_original_error(self):
        with tempfile.TemporaryDirectory() as directory:
            daemon = Daemon()
            daemon.fail_cleanup = True
            with self.assertRaisesRegex(RuntimeError, "first server failed") as caught, Cluster("image", directory, daemon):
                pass
            self.assertIn("owned cleanup failed", " ".join(caught.exception.__notes__))

    def test_interruption_tears_down_and_keeps_other_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            daemon = Daemon()
            cluster = Cluster("image", directory, daemon)
            cluster.network_created = True
            daemon.resources[("network", cluster.run_id)] = cluster.run_id
            with patch.object(cluster, "start", return_value=cluster), self.assertRaises(KeyboardInterrupt), cluster:
                raise KeyboardInterrupt
            self.assertEqual(daemon.resources, {("container", "unrelated"): "other-run"})

    def test_close_does_not_remove_a_name_owned_by_another_run(self):
        with tempfile.TemporaryDirectory() as directory:
            daemon = Daemon()
            cluster = Cluster("image", directory, daemon)
            cluster.containers.append("unrelated")
            cluster.close()
            self.assertIn(("container", "unrelated"), daemon.resources)

    def test_collection_failure_still_cleans_and_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory, Daemon())
            with patch.object(cluster, "start", return_value=cluster), \
                 patch.object(cluster, "diagnostics", side_effect=OSError("artifact disk full")), \
                 self.assertRaisesRegex(OSError, "artifact disk full"), cluster:
                pass
            self.assertTrue(cluster.closed)

    def test_capacity_shares_cpu_but_keeps_memory_bounded(self):
        self.assertEqual(capacity(8, 4, 8192), 2)
        self.assertEqual(capacity(8, 16, 2048), 1)
        self.assertEqual(capacity(2, 4, 8192, nodes=14, node_cpus=1), 1)
        self.assertEqual(capacity(2, 4, 16384, nodes=14, node_cpus=1), 2)
        for args in ((0, 4, 4096), (1, 1, 4096), (2, 4, 1024)):
            with self.assertRaises(ValueError):
                capacity(*args)
        with self.assertRaises(ValueError):
            capacity(1, 4, 8192, node_cpus=0)

    def test_restart_retains_direct_host_and_peer_endpoints(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory)
            cluster.servers, cluster.addresses = ["server"], ["172.18.0.2"]
            cluster.origins = ["http://172.18.0.2:7000"]
            info = {"NetworkSettings": {"Networks": {cluster.run_id: {"IPAddress": "172.18.0.2"}}}}
            cluster.command = lambda *_args: json.dumps([info])
            cluster.refresh_endpoints()
            self.assertEqual(cluster.origins, ["http://172.18.0.2:7000"])
            info["NetworkSettings"]["Networks"][cluster.run_id]["IPAddress"] = "172.18.0.3"
            with self.assertRaisesRegex(AssertionError, "peer RPC address changed"):
                cluster.refresh_endpoints()

    def test_faults_target_servers_and_never_completed_clients(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = []
            cluster = Cluster("image", directory, lambda *args: calls.append(args))
            cluster.servers = ["server"]
            cluster.containers = ["server", "finished-client"]
            with patch.object(cluster, "refresh_endpoints"), patch.object(cluster, "ready"):
                cluster.crash_restart()
            self.assertEqual(calls, [("kill", "--signal", "KILL", "server"), ("start", "server")])

    def test_diagnostics_redact_credentials(self):
        result = redact({"Env": ["PASSWORD=hidden", "CROWDB_E2E_TOKEN=private"], "secret": "other"})
        for value in ("hidden", "private", "other"):
            self.assertNotIn(value, result)
        self.assertIn("<redacted>", result)
        self.assertEqual(LABEL, "org.crowdb.e2e.run")
