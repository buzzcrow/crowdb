# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Explicit posting policy shared by the real-service validation launchers."""

import argparse


def batch_size(value):
    parsed = int(value)
    if not 1 <= parsed <= 256:
        raise argparse.ArgumentTypeError("batch size must be 1..256")
    return parsed


def add_posting_arguments(parser):
    parser.add_argument("--send-batch-size", type=batch_size, default=1)
    parser.add_argument("--receive-batch-size", type=batch_size, default=1)
    parser.add_argument("--aggregation-enabled", action="store_true")


def posting_settings(args):
    return {key: getattr(args, key) for key in ("send_batch_size", "receive_batch_size", "aggregation_enabled")}


def posting_environment(settings):
    return {"CROWDB_RDMA_" + key.upper(): str(int(value)) for key, value in settings.items()}


def check_posting(settings, *snapshots):
    for snapshot in snapshots:
        if snapshot["rdma"] is not None:
            for key, expected in settings.items():
                if type(snapshot["rdma"].get(key)) is not type(expected) or snapshot["rdma"].get(key) != expected:
                    raise RuntimeError(f"requested {key}={expected}, actual {snapshot['rdma'].get(key)}")
