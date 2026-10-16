# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from allocation import LABEL
from console_native_candidates import CAPACITY, NAMES, TAIL, TAIL_OFFSET, prepare, verify


class NativeCandidatesTest(unittest.TestCase):
    def fixture(self, root):
        data = root / "data"
        (data / "disks").mkdir(parents=True)
        info = {"Id": "immutable", "Image": "image", "Config": {"Labels": {LABEL: "owned"}},
                "Mounts": [{"Source": str(data), "Destination": "/opt/crowdb/data"}]}
        return SimpleNamespace(run_id="owned", image="image", servers=["node"], data_paths=[data],
                               artifacts=root, candidate_inputs=prepare(data), command=Mock(return_value=json.dumps([info])))

    def test_owned_inputs_keep_tail_and_expose_nonzero_only_on_rejection_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cluster = self.fixture(root)
            for index, source in enumerate(cluster.candidate_inputs):
                path = cluster.data_paths[0] / "disks" / NAMES[index]
                observed = path.stat()
                self.assertEqual(source["resource_identity"], f"file:{observed.st_dev}:{observed.st_ino}")
                self.assertEqual(observed.st_size, CAPACITY)
                with path.open("rb") as file:
                    file.seek(TAIL_OFFSET - 1)
                    self.assertEqual(file.read(1), b"X" if index == 2 else b"\0")
                    self.assertEqual(file.read(len(TAIL)), TAIL)
            verify(cluster)
            self.assertEqual(len(json.loads((root / "supplied-media-proof.json").read_text())), 3)
            with self.assertRaises(FileExistsError):
                prepare(cluster.data_paths[0])

    def test_replacement_or_modified_tail_cannot_produce_a_persistence_proof(self):
        for replace in (True, False):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                cluster = self.fixture(root)
                path = cluster.data_paths[0] / "disks" / NAMES[0]
                if replace:
                    path.rename(path.with_suffix(".retained"))
                    with path.open("wb") as file:
                        file.truncate(CAPACITY)
                else:
                    with path.open("r+b") as file:
                        file.seek(TAIL_OFFSET)
                        file.write(b"changed")
                with self.assertRaises(AssertionError):
                    verify(cluster)
                self.assertFalse((root / "supplied-media-proof.json").exists())

    def test_foreign_container_rejects_before_reading_owned_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.fixture(Path(directory))
            info = json.loads(cluster.command.return_value)
            info[0]["Config"]["Labels"][LABEL] = "foreign"
            cluster.command.return_value = json.dumps(info)
            with self.assertRaisesRegex(ValueError, "ownership"):
                verify(cluster)
