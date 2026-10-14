# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Installed binding lifecycle tests; functional, with no throughput assertions."""
import asyncio
import multiprocessing
import unittest
from crowdb_dataset import DatasetClient, DatasetError, AsyncDatasetClient


def inherited_client(client, pipe):
    try:
        client.open()
    except RuntimeError as error:
        pipe.send(str(error))
    else:
        pipe.send("inherited client incorrectly accepted")
    client.close()
    pipe.close()


class TestNative(unittest.TestCase):
    def test_configuration_validation_and_credential_redaction(self):
        token = "PRIVATE-CREDENTIAL"
        with self.assertRaises(RuntimeError) as error:
            DatasetClient("http://PRIVATE-CREDENTIAL@localhost", "test", token)
        self.assertNotIn(token, str(error.exception))
        with self.assertRaises(RuntimeError):
            DatasetClient("http://localhost", "test", max_requests=0)

    def test_close_and_inherited_worker_handle(self):
        client = DatasetClient("http://127.0.0.1:9", "test", request_timeout=0.1)
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        worker = context.Process(target=inherited_client, args=(client, send))
        worker.start()
        send.close()
        self.assertTrue(receive.poll(10), "forked binding did not return")
        self.assertIn("worker process", receive.recv())
        worker.join(10)
        self.assertEqual(worker.exitcode, 0)
        client.close()
        with self.assertRaises(RuntimeError): client.open()

    def test_async_failure_uses_rust_operation_and_keeps_identity(self):
        async def run():
            async with AsyncDatasetClient("http://127.0.0.1:9", "test", request_timeout=0.1) as client:
                with self.assertRaises(DatasetError) as error:
                    await client.upload_status("a" * 32)
                self.assertEqual(error.exception.identity, "a" * 32)
                self.assertEqual(error.exception.code, "transport")
        asyncio.run(run())


if __name__ == "__main__": unittest.main()


class TestPeer:
    """Gated HTTP peer exercises native waits and cancellation, not server acceptance."""
    def __enter__(self):
        import http.server
        import queue
        import threading
        self.requests, self.release = queue.Queue(), threading.Event()
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.put(self.path)
                owner.release.wait(10)
                body = b'{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","namespace":"dataset-ns","name":"test","latest":null,"stable":null}'
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                try: self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError): pass
            def do_POST(self):
                owner.requests.put(self.path)
                owner.release.wait(10)
                self.send_response(503)
                body = b'{"code":"unavailable","message":"gated test","request_id":"test","retryable":false}'
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


class TestNativeConcurrency(unittest.TestCase):
    def test_blocked_native_calls_allow_python_progress_and_concurrent_calls(self):
        import concurrent.futures
        with TestPeer() as peer, DatasetClient(peer.endpoint, "test", max_requests=2) as client:
            with concurrent.futures.ThreadPoolExecutor(2) as workers:
                first, second = workers.submit(client.open), workers.submit(client.open)
                peer.requests.get(timeout=10)
                peer.requests.get(timeout=10)
                self.assertEqual(sum(range(1000)), 499500)
                with self.assertRaises(DatasetError) as error: client.open()
                self.assertEqual(error.exception.code, "capacity")
                peer.release.set()
                self.assertEqual(first.result(10)["id"], second.result(10)["id"])

    def test_async_stream_cancellation_releases_native_admission(self):
        with TestPeer() as peer:
            async def run():
                async with AsyncDatasetClient(peer.endpoint, "test", max_requests=1) as client:
                    stream = client.stream_batch({"snapshot": "a" * 32, "sample_ids": ["c2FtcGxl"], "fields": ["value"]})
                    pending = asyncio.create_task(stream.__anext__())
                    await asyncio.to_thread(peer.requests.get, True, 10)
                    pending.cancel()
                    with self.assertRaises(asyncio.CancelledError): await pending
                    await stream.close()
                    peer.release.set()
                    self.assertEqual((await client.open())["name"], "test")
            asyncio.run(run())
