# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Process exit keeps its endpoint reachable and cleanup respects namespace ownership."""

import json
import tempfile
import unittest
from unittest.mock import Mock

from allocation import start_node
from fixture import Cluster, LABEL
from scenarios import Case, case_resources


class RetainedNetworkTest(unittest.TestCase):
    def test_shared_namespace_rejects_foreign_owner_image_and_address_before_start(self):
        for label, image, address in (("foreign", "release-image", "10.20.30.4"),
                                      ("owned", "other-image", "10.20.30.4"),
                                      ("owned", "release-image", "10.20.30.5")):
            with tempfile.TemporaryDirectory() as directory:
                cluster = Cluster("release-image", directory)
                cluster.servers.append("existing")
                info = {"Image": image, "Config": {"Labels": {LABEL: cluster.run_id if label == "owned" else label}},
                        "NetworkSettings": {"Networks": {cluster.run_id: {"IPAddress": address}}}}
                cluster.command = Mock(return_value=json.dumps([info]))
                with self.assertRaisesRegex(ValueError, "shared namespace ownership"):
                    start_node(cluster, 1, None, (), "10.20.30.4", network_container="existing")
                cluster.command.assert_called_once_with("inspect", "existing")
                self.assertEqual(cluster.servers, ["existing"])

    def test_storage_budget_includes_namespace_owners(self):
        self.assertEqual(case_resources(Case("chunk-client.reader", "reader", chunkdb_storage=True,
                                            diskio_process_exit=True), 0.5),
                         (6.3, 6240))
        self.assertEqual(case_resources(Case("iceberg.files", "files"), 0.5), (7.5, 7680))
        self.assertEqual(case_resources(Case("kv.management", "kv"), 0.5), (1.0, 1024))
        cpu, memory = case_resources(Case("s3.nodes", "joint", s3_node_fault=True), 0.2)
        self.assertAlmostEqual(cpu, 3.9)
        self.assertEqual(memory, 7872)
        cpu, memory = case_resources(Case("console.web.wal", "unreachable",
                                         stores=(0,) * 5, preserve_network=True), 0.25)
        self.assertAlmostEqual(cpu, 2.25)
        self.assertEqual(memory, 3664)

    def test_server_uses_retained_namespace_and_cleanup_removes_it_last(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = []
            cluster = Cluster("release-image", directory)
            address = "10.20.30.4"
            server = cluster.run_id + "-node5"
            owner = server + "-network"
            resources = set()

            def command(*args):
                calls.append(args)
                if args[0] == "run":
                    resources.add(args[args.index("--name") + 1])
                if args[0] == "inspect":
                    info = {"Id": "owned-network-id", "HostConfig": {"NetworkMode": "container:owned-network-id"},
                            "NetworkSettings": {"Networks": {cluster.run_id: {"IPAddress": address}}}}
                    return json.dumps([info])
                if args[:2] == ("container", "ls"):
                    self.assertIn(f"label={LABEL}={cluster.run_id}", args)
                    return "\n".join(resources)
                if args[:2] == ("container", "rm"):
                    if args[-1] == owner:
                        self.assertNotIn(server, resources)
                    resources.remove(args[-1])
                return ""

            cluster.command = command
            self.assertEqual(start_node(cluster, 5, None, ("server", "diskio"), address,
                                        preserve_network=True), address)
            self.assertEqual(cluster.servers, [server])
            self.assertIn("release-image", calls[0])
            self.assertIn("container:" + owner, calls[2])
            self.assertNotIn("--ip", calls[2])
            cluster.close()
            self.assertEqual(resources, set())

    def test_invalid_retention_is_rejected_before_allocating(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory, lambda *_: self.fail("allocation is forbidden"))
            for address, running in ((None, True), ("10.20.30.4", False)):
                with self.assertRaises(ValueError):
                    start_node(cluster, 0, None, (), address, running=running, preserve_network=True)
            self.assertEqual(cluster.containers, [])
