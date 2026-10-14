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
