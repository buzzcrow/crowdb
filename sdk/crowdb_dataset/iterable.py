"""Small framework adapter over the Dataset scan contract.

The adapter deliberately depends on a client object with ``scan(plan, cursor)``;
the Rust HTTP and direct clients can provide that bridge without duplicating
selection, shuffle, or cursor logic.
"""

from collections import deque


class WorkerPartition:
    def __init__(self, worker=0, workers=1, rank=0, world_size=1):
        if workers < 1 or world_size < 1 or worker < 0 or worker >= workers or rank < 0 or rank >= world_size:
            raise ValueError("invalid worker partition")
        self.worker = worker
        self.workers = workers
        self.rank = rank
        self.world_size = world_size

    def owns(self, index):
        return index % (self.workers * self.world_size) == self.rank * self.workers + self.worker


class DatasetIterable:
    """Bounded iterable suitable for PyTorch ``IterableDataset`` wrapping."""

    def __init__(self, client, plan, *, partition=None, prefetch=1):
        if prefetch < 1:
            raise ValueError("prefetch must be positive")
        self.client = client
        self.plan = plan
        self.partition = partition or WorkerPartition()
        self.prefetch = prefetch

    def __iter__(self):
        cursor = None
        index = 0
        queue = deque()
        while True:
            response = self.client.scan(self.plan, cursor)
            for sample in response.samples:
                if self.partition.owns(index):
                    if len(queue) >= self.prefetch:
                        yield queue.popleft()
                    queue.append(sample)
                index += 1
            cursor = response.cursor
            while queue:
                yield queue.popleft()
            if response.end:
                return
