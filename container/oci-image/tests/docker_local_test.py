# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Scoped cleanup preserves data and rejects changed resource ownership."""

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from docker_local_cleanup import clean, image_garbage, selected
from docker_local_launch import ensure, verify_container
from docker_local_state import CLUSTER, OWNER, Docker, load, seal


class DockerLocalTest(unittest.TestCase):
    def setUp(self):
        self.record = {"version": 1, "cluster": "first", "owner": str(uuid.uuid4()),
                       "runtime": {"daemon": "owned", "context": "default", "endpoint": "unix:///owned"},
                       "image_id": "sha256:retained", "image": "crowdb-node:dev", "nodes": 1,
                       "port_base": 9090, "cpus": 1, "memory_mib": 1536}
        self.labels = {CLUSTER: "first", OWNER: self.record["owner"]}
        self.docker = MagicMock()
        self.docker.identity.return_value = self.record["runtime"]
        self.docker.owned.return_value = []

    def network(self, attached=False):
        return {"Id": "bridge-id", "Name": "crowdb-first", "Labels": self.labels,
                "Containers": {"foreign": {}} if attached else {}}

    def test_clean_removes_owned_empty_bridge_and_preserves_data(self):
        network = self.network()
        self.docker.owned.side_effect = lambda record, kind: [network] if kind == "network" else []
        self.docker.call.return_value = json.dumps([network])
        clean(self.docker, self.record)
        self.docker.call.assert_any_call("network", "rm", "bridge-id")
        self.assertFalse(any(call.args[:2] == ("volume", "rm") for call in self.docker.call.call_args_list))

    def test_attached_or_relabelled_bridge_is_not_removed(self):
        for attached in (True, False):
            with self.subTest(attached=attached):
                self.docker.reset_mock()
                old = self.network()
                observed = self.network(attached)
                if not attached:
                    observed["Labels"] = {CLUSTER: "other", OWNER: "foreign"}
                self.docker.owned.side_effect = lambda record, kind: [old] if kind == "network" else []
                self.docker.call.return_value = json.dumps([observed])
                with self.assertRaises(ValueError):
                    clean(self.docker, self.record)
                self.assertFalse(any(call.args[:2] == ("network", "rm") for call in self.docker.call.call_args_list))

    def test_unknown_resource_names_are_not_adopted_from_copied_labels(self):
        docker = Docker()
        owned = self.network()
        foreign = dict(owned, Name="other-network", Id="foreign")
        with patch.object(docker, "objects", return_value=[owned, foreign]):
            self.assertEqual(docker.owned(self.record, "network"), [owned])

    def test_changed_daemon_rejects_before_cleanup_effects(self):
        self.docker.identity.return_value = {"daemon": "different"}
        with self.assertRaisesRegex(ValueError, "endpoint/context"):
            clean(self.docker, self.record)
        self.docker.call.assert_not_called()

    def test_delete_data_rejects_a_volume_referenced_by_another_container(self):
        volume = {"Name": "crowdb-first-1-data", "Labels": self.labels}
        self.docker.owned.side_effect = lambda record, kind: [volume] if kind == "volume" else []
        self.docker.call.side_effect = lambda *args: json.dumps([volume]) if args[0] == "volume" else "foreign-id"
        with self.assertRaisesRegex(ValueError, "another container"):
            clean(self.docker, self.record, True)
        self.assertFalse(any(call.args[:2] == ("volume", "rm") for call in self.docker.call.call_args_list))

    def test_create_cannot_adopt_a_foreign_named_network(self):
        network = self.network()
        network["Labels"] = {}
        self.docker.objects.return_value = [network]
        with self.assertRaisesRegex(ValueError, "foreign"):
            ensure(self.docker, self.record, "network", network["Name"])
        self.docker.call.assert_not_called()

    def test_changed_container_inputs_are_rejected_before_resume(self):
        record = dict(self.record, nodes=1, port_base=9090, cpus=1, memory_mib=1536)
        environment = ["CROWDB_CLUSTER=first", "CROWDB_INITIAL_VOTERS=1"]
        for offset, name in enumerate(("S3", "ICEBERG", "DATASET"), start=1):
            environment.append(f"CROWDB_{name}_PUBLIC_URI=http://127.0.0.1:{9090+offset}")
        item = {"Name": "/crowdb-first-1", "Image": record["image_id"], "Config": {"Labels": self.labels, "Env": environment},
                "HostConfig": {"NanoCpus": 1_000_000_000, "Memory": 1536 * 1024 * 1024,
                               "NetworkMode": "crowdb-first", "PortBindings": {
                                   f"{port}/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(port)}]
                                   for port in range(9090, 9094)}},
                "Mounts": [{"Destination": "/opt/crowdb/data", "Name": "owned-volume", "Type": "volume", "RW": True}]}
        verify_container(item, record, 1, "owned-volume", "crowdb-first")
        for field, value in (("Image", "foreign"), ("Mounts", []), ("HostConfig", {})):
            with self.subTest(field=field), self.assertRaises((ValueError, KeyError)):
                verify_container(dict(item, **{field: value}), record, 1, "owned-volume", "crowdb-first")

    def test_receipts_are_immutable_scoped_and_reject_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            seal(self.record, directory)
            self.assertEqual(load("first", directory), self.record)
            self.assertEqual(selected("first", False, directory), [self.record])
            other = dict(self.record, cluster="second", owner=str(uuid.uuid4()))
            seal(other, directory)
            self.assertEqual(len(selected(None, True, directory)), 2)
            with self.assertRaises(ValueError):
                seal(dict(self.record, owner=str(uuid.uuid4())), directory)
            (directory / "foreign.json").symlink_to(directory / "first.json")
            with self.assertRaisesRegex(ValueError, "symlink"):
                load("foreign", directory)

    def test_garbage_keeps_retained_and_stopped_container_images(self):
        def call(*args):
            if args[:2] == ("image", "ls"):
                return "retained\nforeign-used\ngarbage"
            if args[:2] == ("image", "inspect"):
                return json.dumps([{"Id": "sha256:" + args[2], "RepoTags": [], "RepoDigests": [], "Config": {"Labels": {"org.opencontainers.image.source": "https://github.com/buzzcrow/crowdb"}}}])
            if args[:2] == ("ps", "-aq"):
                return "stopped-foreign" if args[-1] == "ancestor=sha256:foreign-used" else ""
            return ""
        self.docker.call.side_effect = call
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            seal(self.record, directory)
            image_garbage(self.docker, directory)
        removals = [call.args for call in self.docker.call.call_args_list if call.args[:2] == ("image", "rm")]
        self.assertEqual(removals, [("image", "rm", "sha256:garbage")])


if __name__ == "__main__":
    unittest.main()
