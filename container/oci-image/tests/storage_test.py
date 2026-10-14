# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Storage mapping validation and runtime launch contract."""

import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runtime import Runtime
from storage import inputs, record_inputs, runtime_arguments


class StorageTest(unittest.TestCase):
    def test_devices_are_mapped_without_mknod_permission(self):
        metadata = SimpleNamespace(st_mode=stat.S_IFBLK | 0o660, st_rdev=123)
        with patch.object(Path, "is_symlink", return_value=False), patch.object(Path, "stat", return_value=metadata):
            disks = inputs(SimpleNamespace(device=[Path("/dev/test-disk")]))
            with self.assertRaisesRegex(ValueError, "duplicate"):
                inputs(SimpleNamespace(device=[Path("/dev/test-disk"), Path("/dev/alias")]))
        self.assertIn("/dev/test-disk:/opt/crowdb/data/disks/test-disk:rw", runtime_arguments(disks))

    def test_duplicate_inode_and_target_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = root / "disk.img"
            original.write_bytes(b"existing data")
            alias = root / "alias.img"
            os.link(original, alias)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                inputs(SimpleNamespace(disk_file=[original, alias]))
            other = root / "other"
            other.mkdir()
            same_name = other / original.name
            same_name.write_bytes(b"different data")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                inputs(SimpleNamespace(disk_file=[original, same_name]))
            self.assertEqual(original.read_bytes(), b"existing data")

    def test_invalid_sources_are_not_created_or_followed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            empty = root / "empty"
            empty.touch()
            alias = root / "alias"
            alias.symlink_to(empty)
            for source in (empty, alias, root, Path("relative")):
                with self.subTest(source=source), self.assertRaises(ValueError):
                    inputs(SimpleNamespace(disk_file=[source]))
            missing = root / "absent"
            with self.assertRaises(FileNotFoundError):
                inputs(SimpleNamespace(disk_file=[missing]))
            self.assertFalse(missing.exists())

    def test_both_runtimes_receive_common_paths_and_retained_declaration(self):
        for engine in ("docker", "containerd"):
            with self.subTest(engine=engine), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / "disk.img"
                source.write_bytes(b"preserved")
                args = SimpleNamespace(disk_file=[source], device=[], data_root=root,
                                       name="node", cpus=1, memory_mib=512,
                                       network="host", startup_mode="manual",
                                       physical_host_id="host", interface="eth0",
                                       password_file=None, seed=[], ui_port=None,
                                       image="image@sha256:test")
                with patch("runtime.tool", return_value="nerdctl"):
                    runtime = Runtime(engine)
                with patch.object(runtime, "invoke", return_value="container") as invoke, patch.object(runtime, "verify"):
                    self.assertEqual(runtime.create(args), "container")
                launch = invoke.call_args_list[0].args
                self.assertIn(f"type=bind,source={source},target=/opt/crowdb/data/disks/disk.img", launch)
                declaration = next(value for value in launch if value.startswith("CROWDB_DISK_INPUTS="))
                self.assertEqual(json.loads(declaration.split("=", 1)[1]),
                                 [{"backend": "file", "path": "/opt/crowdb/data/disks/disk.img"}])
                recorded = json.loads((root / "container-storage.json").read_text())
                self.assertEqual(recorded["disks"][0]["source"], str(source))
                self.assertEqual(source.read_bytes(), b"preserved")

    def test_mapping_record_is_replaced_without_temporary_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record_inputs(root, [])
            record_inputs(root, [{"backend": "file", "source": "/host/disk", "path": "/node/disk"}])
            self.assertEqual(len(json.loads((root / "container-storage.json").read_text())["disks"]), 1)
            self.assertEqual([path.name for path in root.iterdir()], ["container-storage.json"])
