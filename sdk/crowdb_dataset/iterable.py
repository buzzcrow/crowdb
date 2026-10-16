# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Server-partitioned batches with explicit application acknowledgement."""


class WorkerPartition:
    def __init__(self, worker=0, workers=1, rank=0, world_size=1):
        if not (0 <= worker < workers <= 65535 and 0 <= rank < world_size <= 65535):
            raise ValueError("invalid worker partition")
        self.worker, self.workers, self.rank, self.world_size = worker, workers, rank, world_size

    def owns(self, index):
        return index % (self.workers * self.world_size) == self.rank * self.workers + self.worker

    def as_dict(self):
        return {"worker": self.worker, "workers": self.workers,
                "rank": self.rank, "world_size": self.world_size}


class DatasetBatch:
    def __init__(self, batch, client, session, expected):
        self.header, self.fields = batch.header, batch.fields
        self._client, self._session, self._expected = client, session, expected
        self.acknowledged = False

    def acknowledge(self):
        progress = self._client.acknowledge(self._session, self._expected, self.header["next"])
        self.acknowledged = True
        return progress


class DatasetIterable:
    """Yield bounded DatasetBatch objects; acknowledge each before requesting more.

    Tensor decoding/collation belongs above this adapter. Large fields use
    next_batch streams directly. Unacknowledged batches replay on resume.
    """
    def __init__(self, client, plan, *, partition=None, prefetch=1):
        if prefetch != 1:
            raise ValueError("this acknowledged iterator supports one batch in flight")
        self.client, self.plan = client, dict(plan)
        self.plan["partition"] = (partition or WorkerPartition()).as_dict()

    def __iter__(self):
        session = self.client.start_read(self.plan)
        expected = session["acknowledged"]
        while True:
            with self.client.next_batch(session["id"]) as stream:
                batch = DatasetBatch(stream.batch(), self.client, session["id"], expected)
            yield batch
            if not batch.acknowledged:
                raise RuntimeError("acknowledge the consumed batch before requesting the next batch")
            expected = batch.header["next"]
            if batch.header["end"]:
                return
