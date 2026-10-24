# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
import unittest
from crowdb_dataset import Batch, DatasetIterable, WorkerPartition


class TestStream:
    def __enter__(self): return self
    def __exit__(self, *_): pass
    def batch(self): return Batch({"next": None, "end": True}, [b"owned"])


class TestClient:
    def bind_read(self, plan):
        self.plan = plan
        return {"cursor": "old"}
    def next_batch(self, cursor):
        self.cursor = cursor
        return TestStream()


class IterableTest(unittest.TestCase):
    def test_partition_is_sent_before_payload_and_ack_is_explicit(self):
        client = TestClient()
        reader = DatasetIterable(client, {}, partition=WorkerPartition(worker=1, workers=2))
        iterator = iter(reader)
        batch = next(iterator)
        self.assertEqual(client.plan["partition"]["worker"], 1)
        self.assertFalse(hasattr(client, "progress"))
        self.assertEqual(batch.fields, [b"owned"])
        batch.acknowledge()
        self.assertEqual(reader.checkpoint, "old")
        self.assertFalse(hasattr(client, "progress"))
        self.assertEqual(list(iterator), [])
        self.assertTrue(reader.complete)

    def test_continuing_without_acknowledgement_is_rejected(self):
        iterator = iter(DatasetIterable(TestClient(), {}))
        next(iterator)
        with self.assertRaises(RuntimeError): next(iterator)


if __name__ == "__main__": unittest.main()
