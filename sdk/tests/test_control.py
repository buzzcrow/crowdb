# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
import asyncio
import http.server
import json
import queue
import threading
import unittest
from crowdb_dataset import DatasetClient, AsyncDatasetClient, DatasetError, decimal, new_id


class TestControlPeer:
    def __init__(self, capabilities=(), gated=False):
        self.capabilities, self.gated = list(capabilities), gated
        self.requests, self.release = queue.Queue(), threading.Event()

    def __enter__(self):
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
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
