# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
import unittest
from crowdb_dataset import Batch, DatasetIterable, WorkerPartition


class TestStream:
    def __enter__(self): return self
    def __exit__(self, *_): pass
    def batch(self): return Batch({"next": "issued", "end": True}, [b"owned"])


class TestClient:
    def start_read(self, plan):
        self.plan = plan
        return {"id": "session", "acknowledged": "old"}
    def next_batch(self, id): return TestStream()
    def acknowledge(self, id, expected, next):
        self.progress = (id, expected, next)
        return {"cursor": next}


class IterableTest(unittest.TestCase):
    def test_partition_is_sent_before_payload_and_ack_is_explicit(self):
        client = TestClient()
        iterator = iter(DatasetIterable(client, {}, partition=WorkerPartition(worker=1, workers=2)))
        batch = next(iterator)
        self.assertEqual(client.plan["partition"]["worker"], 1)
        self.assertFalse(hasattr(client, "progress"))
        self.assertEqual(batch.fields, [b"owned"])
        batch.acknowledge()
        self.assertEqual(client.progress, ("session", "old", "issued"))
        self.assertEqual(list(iterator), [])

    def test_continuing_without_acknowledgement_is_rejected(self):
        iterator = iter(DatasetIterable(TestClient(), {}))
        next(iterator)
        with self.assertRaises(RuntimeError): next(iterator)


if __name__ == "__main__": unittest.main()
