# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Server-partitioned batches with explicit application acknowledgement."""

from .client import IntegrityError


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
    """A processed batch and its client-owned checkpoint; acknowledgement makes no RPC."""
    def __init__(self, batch):
        self.header, self.fields = batch.header, batch.fields
        self.acknowledged = False

    def acknowledge(self):
        self.acknowledged = True
        return self.header["next"]


class DatasetIterable:
    """Bounded batches with local acknowledgement. Save checkpoint after processing.

    Supplying a previous checkpoint replays the corresponding immutable batch.
    Closing/dropping an iterator needs no remote cleanup.
    """
    def __init__(self, client, plan, *, partition=None, prefetch=1, checkpoint=None):
        if prefetch != 1:
            raise ValueError("this iterator supports one batch in flight")
        self.client, self.plan = client, dict(plan)
        self.plan["partition"] = (partition or WorkerPartition()).as_dict()
        self.checkpoint = checkpoint
        self.complete = False

    def __iter__(self):
        if self.complete:
            return
        if self.checkpoint is None:
            self.checkpoint = self.client.bind_read(self.plan)["cursor"]
        while True:
            with self.client.next_batch(self.checkpoint) as stream:
                batch = DatasetBatch(stream.batch())
            following = batch.header["next"]
            if (batch.header["end"] != (following is None)
                    or following == self.checkpoint
                    or (not batch.header["sample_ids"] and batch.fields)):
                raise IntegrityError({"code": "integrity", "message": "invalid read continuation"})
            if batch.header["sample_ids"]:
                yield batch
                if not batch.acknowledged:
                    raise RuntimeError("acknowledge the consumed batch before requesting the next batch")
            self.checkpoint = batch.header["next"]
            self.complete = batch.header["end"]
            if self.complete:
                return
