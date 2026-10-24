# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
import unittest
from crowdb_dataset import Batch, DatasetIterable, IntegrityError, WorkerPartition


class TestStream:
    def __enter__(self): return self
    def __exit__(self, *_): pass
    def batch(self): return Batch({"sample_ids": ["sample"], "next": None, "end": True}, [b"owned"])


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

    def test_empty_windows_advance_locally_and_nonempty_data_still_requires_ack(self):
        client = TestWindowClient([
            Batch({"sample_ids": [], "next": "empty-2", "end": False}, []),
            Batch({"sample_ids": [], "next": "data", "end": False}, []),
            Batch({"sample_ids": ["sample"], "next": None, "end": True}, [b""]),
        ])
        reader = DatasetIterable(client, {})
        iterator = iter(reader)
        batch = next(iterator)
        self.assertEqual(client.cursors, ["old", "empty-2", "data"])
        self.assertEqual(batch.fields, [b""])
        self.assertEqual(reader.checkpoint, "data")
        with self.assertRaises(RuntimeError): next(iterator)
        self.assertEqual(reader.checkpoint, "data")
        self.assertFalse(reader.complete)

    def test_empty_terminal_partition_completes_without_yield_or_rebinding(self):
        client = TestWindowClient([Batch({"sample_ids": [], "next": None, "end": True}, [])])
        reader = DatasetIterable(client, {})
        self.assertEqual(list(reader), [])
        self.assertTrue(reader.complete)
        self.assertIsNone(reader.checkpoint)
        self.assertEqual(list(reader), [])
        self.assertEqual(client.cursors, ["old"])
        self.assertEqual(client.binds, 1)

    def test_acknowledgement_then_empty_tail_reaches_eof(self):
        client = TestWindowClient([
            Batch({"sample_ids": [], "next": "data", "end": False}, []),
            Batch({"sample_ids": ["sample"], "next": "tail", "end": False}, [b"value"]),
            Batch({"sample_ids": [], "next": None, "end": True}, []),
        ])
        reader = DatasetIterable(client, {})
        iterator = iter(reader)
        batch = next(iterator)
        self.assertEqual(batch.acknowledge(), "tail")
        self.assertEqual(reader.checkpoint, "data")
        self.assertEqual(list(iterator), [])
        self.assertTrue(reader.complete)
        self.assertEqual(client.cursors, ["old", "data", "tail"])

    def test_empty_window_cannot_replay_forever_or_hide_payload(self):
        for header, fields in [
            ({"sample_ids": [], "next": "old", "end": False}, []),
            ({"sample_ids": [], "next": None, "end": False}, []),
            ({"sample_ids": [], "next": "next", "end": True}, []),
            ({"sample_ids": [], "next": None, "end": True}, [b"hidden"]),
        ]:
            with self.subTest(header=header, fields=fields):
                reader = DatasetIterable(TestWindowClient([Batch(header, fields)]), {})
                with self.assertRaises(IntegrityError): next(iter(reader))
                self.assertEqual(reader.checkpoint, "old")


class TestWindowStream:
    def __init__(self, batch): self._batch = batch
    def __enter__(self): return self
    def __exit__(self, *_): pass
    def batch(self): return self._batch


class TestWindowClient(TestClient):
    def __init__(self, windows):
        self.windows, self.cursors, self.binds = iter(windows), [], 0
    def bind_read(self, plan):
        self.binds += 1
        return super().bind_read(plan)
    def next_batch(self, cursor):
        self.cursors.append(cursor)
        return TestWindowStream(next(self.windows))


if __name__ == "__main__": unittest.main()
