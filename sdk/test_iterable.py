import unittest

from crowdb_dataset import DatasetIterable, WorkerPartition


class Response:
    def __init__(self, samples, cursor, end):
        self.samples, self.cursor, self.end = samples, cursor, end


class Client:
    def __init__(self):
        self.calls = 0

    def scan(self, _plan, cursor):
        self.calls += 1
        if self.calls == 1:
            return Response([0, 1, 2], "next", False)
        return Response([3, 4, 5], "done", True)


class IterableTest(unittest.TestCase):
    def test_rank_worker_partition_and_bounded_scan(self):
        values = list(DatasetIterable(Client(), object(), partition=WorkerPartition(worker=1, workers=2), prefetch=1))
        self.assertEqual(values, [1, 3, 5])


if __name__ == "__main__":
    unittest.main()
