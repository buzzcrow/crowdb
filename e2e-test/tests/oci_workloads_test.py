# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from oci_reference_workloads import quiesce_chunk_allocators


class AllocatorWorkloadPreconditionTest(unittest.TestCase):
    def services(self):
        return [{"id": kind + "-1", "service_type": kind, "pid": index + 1}
                for index, kind in enumerate(("paxos-kv", "diskdb", "diskio", "chunkdb",
                                              "chunk-kv", "access-server"))]

    def test_stops_producers_in_dependency_order_and_preserves_allocator_processes(self):
        services, actions = self.services(), []
        def request(origin, path, body=None):
            self.assertEqual(origin, "http://owned:6135")
            if path == "/api/servers":
                return copy.deepcopy(services)
            service_id = path.split("/")[-2]
            self.assertEqual(path, f"/api/services/{service_id}/stop")
            self.assertEqual(body, {})
            actions.append(service_id)
            next(item for item in services if item["id"] == service_id)["pid"] = None
        with tempfile.TemporaryDirectory() as directory:
            cluster = SimpleNamespace(node_count=1, artifacts=Path(directory),
                                      endpoint=lambda _: "http://owned:6135")
            with patch("oci_reference_workloads.request", side_effect=request):
                quiesce_chunk_allocators(cluster)
            proof = json.loads((cluster.artifacts / "allocator-workload-precondition.json").read_text())
        self.assertEqual(actions, ["access-server-1", "chunk-kv-1", "chunkdb-1"])
        self.assertEqual(proof["stopped_producers"], actions)
        self.assertEqual(proof["preserved_services"], [{"id": kind + "-1", "pid": index + 1}
                         for index, kind in enumerate(("paxos-kv", "diskdb", "diskio"))])

    def test_missing_producer_rejects_before_stopping_any_service(self):
        cluster = SimpleNamespace(node_count=1, endpoint=lambda _: "http://owned:6135")
        services = [item for item in self.services() if item["service_type"] != "chunkdb"]
        with patch("oci_reference_workloads.request", return_value=services) as request:
            with self.assertRaisesRegex(AssertionError, "one owned chunkdb"):
                quiesce_chunk_allocators(cluster)
        request.assert_called_once_with("http://owned:6135", "/api/servers")

    def test_other_topologies_reject_before_contacting_the_target(self):
        with patch("oci_reference_workloads.request", new=Mock()) as request:
            with self.assertRaisesRegex(ValueError, "owned single node"):
                quiesce_chunk_allocators(SimpleNamespace(node_count=3))
        request.assert_not_called()
