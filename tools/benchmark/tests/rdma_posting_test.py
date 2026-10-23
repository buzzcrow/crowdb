# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import argparse
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "rdma"))
from posting import add_posting_arguments, batch_size, check_posting, posting_environment, posting_settings


class RdmaPostingTest(unittest.TestCase):
    def test_requested_policy_reaches_both_service_snapshots(self):
        parser = argparse.ArgumentParser()
        add_posting_arguments(parser)
        args = parser.parse_args(["--send-batch-size", "16", "--receive-batch-size", "8", "--aggregation-enabled"])
        settings = posting_settings(args)
        self.assertEqual(posting_environment(settings), dict(CROWDB_RDMA_SEND_BATCH_SIZE="16",
                         CROWDB_RDMA_RECEIVE_BATCH_SIZE="8", CROWDB_RDMA_AGGREGATION_ENABLED="1"))
        check_posting(settings, dict(rdma=dict(settings)), dict(rdma=None))
        with self.assertRaises(RuntimeError):
            check_posting(settings, dict(rdma=settings | dict(receive_batch_size=1)))
        with self.assertRaises(RuntimeError):
            check_posting(settings, dict(rdma=settings | dict(aggregation_enabled=1)))

    def test_batch_limits_reject_invalid_arguments_before_launching_services(self):
        for value in ("0", "257", "-1"):
            with self.assertRaises(argparse.ArgumentTypeError):
                batch_size(value)


if __name__ == "__main__":
    unittest.main()
