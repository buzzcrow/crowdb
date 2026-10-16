# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host lineage rejects unsupported aliases before container creation."""

import os
import stat
import struct
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from storage_identity import resource_identity


class StorageIdentityTest(unittest.TestCase):
    def test_regular_file_uses_exact_inode(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "disk.img"
            source.write_bytes(b"retained")
            metadata = source.stat()
            self.assertEqual(resource_identity(source, metadata),
                             f"file:{metadata.st_dev}:{metadata.st_ino}")
            self.assertEqual(source.read_bytes(), b"retained")

    def test_whole_loop_uses_kernel_backing_inode_and_read_only_descriptor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "slaves").mkdir()
            (root / "loop").mkdir()
            metadata = SimpleNamespace(st_mode=stat.S_IFBLK, st_rdev=os.makedev(7, 9))
            status = struct.pack("=5Q", 101, 202, 0, 0, 0) + bytes(192)
            with ExitStack() as stack:
                stack.enter_context(patch("storage_identity.Path", return_value=root))
                opened = stack.enter_context(patch("storage_identity.os.open", return_value=41))
                stack.enter_context(patch("storage_identity.os.fstat", return_value=metadata))
                closed = stack.enter_context(patch("storage_identity.os.close"))
                ioctl = stack.enter_context(patch("storage_identity.fcntl.ioctl", return_value=status))
                self.assertEqual(resource_identity("/dev/owned-loop", metadata), "file:101:202")
                opened.assert_called_once_with("/dev/owned-loop", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                ioctl.assert_called_once_with(41, 0x4C05, bytes(232))
                closed.assert_called_once_with(41)
                for raw_backing, offset, limit in ((0, 512, 0), (0, 0, 4096), (123, 0, 0)):
                    ioctl.return_value = struct.pack("=5Q", 101, 202, raw_backing, offset, limit) + bytes(192)
                    with self.subTest(raw_backing=raw_backing, offset=offset, limit=limit), self.assertRaisesRegex(ValueError, "complete backing file"):
                        resource_identity("/dev/owned-loop", metadata)

    def test_partitions_and_stacked_devices_are_rejected_before_open(self):
        for kind in ("partition", "stacked", "children"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "slaves").mkdir()
                if kind == "partition":
                    (root / "partition").touch()
                elif kind == "stacked":
                    (root / "slaves" / "backing").mkdir()
                else:
                    (root / "part1").mkdir()
                    (root / "part1" / "partition").touch()
                metadata = SimpleNamespace(st_mode=stat.S_IFBLK, st_rdev=123)
                with patch("storage_identity.Path", return_value=root), patch("storage_identity.os.open") as opened:
                    with self.assertRaises(ValueError):
                        resource_identity("/dev/unsupported", metadata)
                    opened.assert_not_called()

    def test_changed_opened_device_is_rejected_and_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "slaves").mkdir()
            metadata = SimpleNamespace(st_mode=stat.S_IFBLK, st_rdev=123)
            with ExitStack() as stack:
                stack.enter_context(patch("storage_identity.Path", return_value=root))
                stack.enter_context(patch("storage_identity.os.open", return_value=41))
                stack.enter_context(patch("storage_identity.os.fstat", return_value=SimpleNamespace(st_mode=stat.S_IFREG, st_rdev=123)))
                closed = stack.enter_context(patch("storage_identity.os.close"))
                with self.assertRaisesRegex(ValueError, "changed"):
                    resource_identity("/dev/changed", metadata)
                closed.assert_called_once_with(41)
