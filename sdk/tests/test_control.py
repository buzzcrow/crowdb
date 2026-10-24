# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
import asyncio
import http.server
import json
import queue
import threading
import unittest
from crowdb_dataset import DatasetClient, AsyncDatasetClient, DatasetError, decimal, new_id
from crowdb_dataset.client import ConflictError


class TestControlPeer:
    def __init__(self, capabilities=(), gated=False, head_replies=()):
        self.capabilities, self.gated = list(capabilities), gated
        self.requests, self.release = queue.Queue(), threading.Event()
        self.head_replies = queue.Queue()
        for reply in head_replies: self.head_replies.put(reply)

    def __enter__(self):
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_PUT(self):
                request = json.loads(self.rfile.read(int(self.headers["content-length"])))
                owner.requests.put((self.path, request))
                status, value = owner.head_replies.get(timeout=10)
                body = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def do_GET(self):
                owner.requests.put(self.path)
                if self.path.endswith("capabilities"):
                    value = {"api_version": 2, "capabilities": owner.capabilities}
                elif "samples" in self.path:
                    value = {"entries": ["AAH/"], "after": None}
                else:
                    if owner.gated: owner.release.wait(10)
                    value = {"latest": None, "stable": None, "generation": None}
                body = json.dumps(value).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                try: self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError): pass
            def log_message(self, *_): pass
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"
        return self

    def __exit__(self, *_):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(10)


class TestControl(unittest.TestCase):
    def test_split_preparation_uses_the_resource_path_and_preserves_its_identity(self):
        snapshot, revision = new_id(), new_id()
        request = {"policy": {"overlap": "disjoint", "coverage": "allow_unassigned"}, "splits": []}
        prepared = dict(request, snapshot=snapshot, revision=revision, operation=revision, evaluator_version=1)
        error = {"code": "conflict", "message": "resource ID has different input", "request_id": "test", "retryable": False}
        with TestControlPeer(["split-preparation"], head_replies=[(409, error), (200, prepared)]) as peer, DatasetClient(peer.endpoint, "test") as owner:
            control = owner.control()
            with self.assertRaises(ConflictError) as failure: control.prepare_split_revision(snapshot, revision, request)
            self.assertEqual(failure.exception.identity, revision)
            self.assertEqual(control.prepare_split_revision(snapshot, revision, request), prepared)
            self.assertTrue(peer.requests.get(timeout=10).endswith("/capabilities"))
            path = f"/api/dataset/v2/namespaces/dataset-ns/datasets/test/snapshots/{snapshot}/split-revisions/{revision}"
            self.assertEqual(peer.requests.get(timeout=10), (path, request))
            self.assertEqual(peer.requests.get(timeout=10), (path, request))
            self.assertTrue(peer.requests.empty())

    def test_snapshot_head_conflict_uses_the_observation_without_a_head_get(self):
        snapshot = new_id()
        observed = {"latest": new_id(), "stable": None, "generation": "11" * 32}
        selected = dict(observed, latest=snapshot, generation="22" * 32)
        error = {"code": "conflict", "message": "conflict", "request_id": "test", "retryable": False, "observed_head": observed}
        with TestControlPeer(["snapshot-head-update"], head_replies=[(409, error), (200, selected)]) as peer, DatasetClient(peer.endpoint, "test") as owner:
            control = owner.control()
            request = {"snapshot": snapshot, "expected_head": None}
            with self.assertRaises(ConflictError) as failure: control.update_snapshot_head(request)
            self.assertEqual(failure.exception.identity, snapshot)
            self.assertEqual(failure.exception.observed_head, observed)
            reviewed = dict(request, expected_head=observed["generation"])
            self.assertEqual(control.update_snapshot_head(reviewed), selected)
            self.assertTrue(peer.requests.get(timeout=10).endswith("/capabilities"))
            self.assertEqual(peer.requests.get(timeout=10), ("/api/dataset/v2/namespaces/dataset-ns/datasets/test/head", request))
            self.assertEqual(peer.requests.get(timeout=10), ("/api/dataset/v2/namespaces/dataset-ns/datasets/test/head", reviewed))
            self.assertTrue(peer.requests.empty())

    def test_successful_capabilities_are_reused_across_control_wrappers(self):
        with TestControlPeer(["head-inspection"]) as peer, DatasetClient(peer.endpoint, "test") as owner:
            owner.control().head()
            owner.control().head()
            paths = [peer.requests.get(timeout=10) for _ in range(3)]
            self.assertEqual(sum(path.endswith("capabilities") for path in paths), 1)
            self.assertTrue(peer.requests.empty())
            peer.capabilities = []
            owner.control().capabilities()
            with self.assertRaises(DatasetError): owner.control().head()
            self.assertTrue(peer.requests.get(timeout=10).endswith("capabilities"))
            self.assertTrue(peer.requests.get(timeout=10).endswith("capabilities"))
            self.assertTrue(peer.requests.empty())
            peer.capabilities = ["head-inspection"]
            self.assertIsNone(owner.control().head()["latest"])
            self.assertTrue(peer.requests.get(timeout=10).endswith("capabilities"))
            self.assertTrue(peer.requests.get(timeout=10).endswith("head"))
            self.assertTrue(peer.requests.empty())

    def test_exact_integer_and_unavailable_mutation(self):
        self.assertEqual(decimal(2**64-1), "18446744073709551615")
        for value in (True, 1.0, -1, 2**64):
            with self.assertRaises(ValueError): decimal(value)
        with TestControlPeer() as peer, DatasetClient(peer.endpoint, "test") as owner:
            request = {"attempt": new_id(), "snapshot": new_id(), "expected_head": None}
            with self.assertRaises(DatasetError) as error:
                owner.control().publish_composition(new_id(), request)
            self.assertEqual(error.exception.identity, request["attempt"])
            self.assertTrue(peer.requests.get(timeout=10).endswith("capabilities"))
            self.assertTrue(peer.requests.empty())

    def test_pages_do_not_fetch_until_consumed_and_stop_at_end(self):
        with TestControlPeer(["bounded-inspection"]) as peer, DatasetClient(peer.endpoint, "a/b") as owner:
            pages = owner.control().pages("samples", snapshot=new_id(), limit=1)
            self.assertTrue(peer.requests.empty())
            self.assertEqual(next(pages)["entries"], ["AAH/"])
            with self.assertRaises(StopIteration): next(pages)
            peer.requests.get(timeout=10)
            self.assertIn("a%2Fb/snapshots", peer.requests.get(timeout=10))
            self.assertTrue(peer.requests.empty())

    def test_async_cancellation_releases_control_operation_and_preserves_gil_progress(self):
        with TestControlPeer(["head-inspection"], gated=True) as peer:
            async def run():
                async with AsyncDatasetClient(peer.endpoint, "test", max_requests=1) as owner:
                    pending = asyncio.create_task(owner.control().head())
                    await asyncio.to_thread(peer.requests.get, True, 10)
                    await asyncio.to_thread(peer.requests.get, True, 10)
                    self.assertEqual(sum(range(100)), 4950)
                    pending.cancel()
                    with self.assertRaises(asyncio.CancelledError): await pending
                    peer.release.set()
                    self.assertIsNone((await owner.control().head())["latest"])
            asyncio.run(run())
