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
from runtime import OWNER, Runtime
from storage import inputs, record_inputs, runtime_arguments, validate_retained_inputs


class StorageTest(unittest.TestCase):
    def test_one_and_three_voters_use_normal_policy_and_preserve_supplied_media(self):
        for voters in (1, 3):
            with self.subTest(voters=voters), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "node"
                root.mkdir()
                source = Path(directory) / "external.img"
                source.write_bytes(b"retained")
                args = SimpleNamespace(data_root=root, disk_file=[source], device=[],
                                       name="node", cpus=1, memory_mib=512, network="host",
                                       cluster="test-cluster", initial_voters=voters,
                                       physical_host_id="host", interface="eth0",
                                       password_file=None, seed=[], ui_port=None, image="image")
                runtime = Runtime("docker")
                with patch.object(runtime, "invoke", return_value="container") as invoke, patch.object(runtime, "verify"):
                    self.assertEqual(runtime.create(args), "container")
                launch = invoke.call_args_list[0].args
                self.assertIn("CROWDB_CLUSTER=test-cluster", launch)
                self.assertIn(f"CROWDB_INITIAL_VOTERS={voters}", launch)
                self.assertFalse(any("STARTUP_MODE" in arg for arg in launch))
                self.assertTrue((root / "container-storage.json").is_file())
                self.assertEqual(source.read_bytes(), b"retained")

    def test_failed_start_preserves_logs_before_owned_cleanup(self):
        for owned in (True, False):
            with self.subTest(owned=owned), tempfile.TemporaryDirectory() as directory:
                args = SimpleNamespace(data_root=Path(directory), disk_file=[], device=[],
                                       name="node", cpus=1, memory_mib=512, network="host",
                                       cluster="test-cluster", initial_voters=1, physical_host_id="host", interface="eth0",
                                       password_file=None, seed=[], ui_port=None, image="image")
                calls = []
                token = None

                def invoke(*command):
                    nonlocal token
                    calls.append(command)
                    if command[0] == "run":
                        token = command[command.index("--label") + 1].split("=", 1)[1]
                    return "startup diagnostic"

                runtime = Runtime("docker")
                with patch.object(runtime, "invoke", side_effect=invoke), \
                        patch.object(runtime, "verify", side_effect=RuntimeError("stopped")), \
                        patch.object(runtime, "inspect", side_effect=lambda _: {
                            "Config": {"Labels": {OWNER: token if owned else "unrelated"}}}), \
                        patch("builtins.print") as output:
                    with self.assertRaisesRegex(RuntimeError, "stopped"):
                        runtime.create(args)
                self.assertEqual(calls[1:], [("logs", "node"), ("rm", "-f", "node")] if owned else [])
                self.assertEqual(output.call_count, 2 * int(owned))

    def test_replacement_preserves_original_mappings_before_runtime_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "disk.img"
            source.write_bytes(b"preserved")
            disks = inputs(SimpleNamespace(disk_file=[source]))
            record_inputs(root, disks)
            validate_retained_inputs(root, disks)
            record = (root / "container-storage.json").read_bytes()
            runtime = Runtime("docker")
            with patch.object(runtime, "invoke") as invoke:
                with self.assertRaisesRegex(ValueError, "missing or changed"):
                    runtime.create(SimpleNamespace(data_root=root, disk_file=[]))
                invoke.assert_not_called()
            changed = [dict(disks[0], backing_identity="file:other:inode")]
            with self.assertRaisesRegex(ValueError, "missing or changed"):
                validate_retained_inputs(root, changed)
            added = root / "new.img"
            added.write_bytes(b"new")
            validate_retained_inputs(root, inputs(SimpleNamespace(disk_file=[added, source])))
            self.assertEqual((root / "container-storage.json").read_bytes(), record)
            self.assertEqual(source.read_bytes(), b"preserved")

    def test_devices_are_mapped_without_mknod_permission(self):
        metadata = SimpleNamespace(st_mode=stat.S_IFBLK | 0o660, st_rdev=123)
        with patch.object(Path, "is_symlink", return_value=False), patch.object(Path, "stat", return_value=metadata), \
                patch("storage.resource_identity", return_value="device:123"):
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
                                       network="host", cluster="test-cluster", initial_voters=3,
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
                                 [{"backend": "file", "path": "/opt/crowdb/data/disks/disk.img",
                                   "backing_identity": f"file:{source.stat().st_dev}:{source.stat().st_ino}",
                                   "resource_identity": f"file:{source.stat().st_dev}:{source.stat().st_ino}"}])
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

    def test_legacy_mapping_adds_lineage_only_for_unchanged_original(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "disk.img"
            source.write_bytes(b"retained")
            disks = inputs(SimpleNamespace(disk_file=[source]))
            legacy = [{key: value for key, value in disk.items() if key != "resource_identity"}
                      for disk in disks]
            record_inputs(root, legacy)
            before = (root / "container-storage.json").read_bytes()
            validate_retained_inputs(root, disks)
            self.assertEqual((root / "container-storage.json").read_bytes(), before)
            with self.assertRaisesRegex(ValueError, "missing or changed"):
                validate_retained_inputs(root, [dict(disks[0], backing_identity="file:1:2")])

    def test_file_and_loop_alias_are_rejected_before_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "disk.img"
            source.write_bytes(b"retained")
            device = SimpleNamespace(st_mode=stat.S_IFBLK, st_rdev=123)
            original_stat = Path.stat
            def metadata(path, *args, **kwargs):
                return device if str(path) == "/dev/owned-loop" else original_stat(path, *args, **kwargs)
            with patch.object(Path, "stat", metadata), patch.object(Path, "is_symlink", return_value=False), \
                    patch("storage.resource_identity", return_value="file:10:20"):
                with self.assertRaisesRegex(ValueError, "duplicate"):
                    inputs(SimpleNamespace(disk_file=[source], device=[Path("/dev/owned-loop")]))
            self.assertEqual(source.read_bytes(), b"retained")
