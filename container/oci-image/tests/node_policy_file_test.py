# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from runtime import Runtime, node_policy_file


class NodePolicyFileTest(unittest.TestCase):
    def test_readonly_policy_is_forwarded_with_the_same_contract_on_both_runtimes(self):
        for engine in ("docker", "containerd"):
            with self.subTest(engine=engine), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                policy = root / "policy.toml"
                content = 'version = 1\nmode = "production"\ninitial_voters = 3\n'
                policy.write_text(content)
                data = root / "data"
                data.mkdir()
                args = SimpleNamespace(node_policy_file=policy, data_root=data,
                    disk_file=[], device=[], name="node", cpus=1, memory_mib=512,
                    network="host", cluster="test", initial_voters=3,
                    physical_host_id="host", interface="eth0", password_file=None,
                    seed=[], ui_port=None, image="sha256:test")
                with patch("runtime.tool", return_value="nerdctl"):
                    runtime = Runtime(engine)
                with patch.object(runtime, "invoke", return_value="node") as invoke, \
                        patch.object(runtime, "verify"):
                    runtime.create(args)
                command = invoke.call_args_list[0].args
                self.assertIn(f"type=bind,source={policy},target=/run/crowdb-node-policy.toml,readonly", command)
                self.assertIn("CROWDB_NODE_POLICY_FILE=/run/crowdb-node-policy.toml", command)
                self.assertEqual(policy.read_text(), content)

    def test_invalid_file_rejects_before_any_runtime_access(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = Path(directory) / "policy.toml"
            policy.write_bytes(b'password = "must-not-appear-in-the-error\xff')
            args = SimpleNamespace(node_policy_file=policy)
            runtime = Runtime("docker")
            with patch.object(runtime, "invoke") as invoke:
                with self.assertRaisesRegex(ValueError, "not valid UTF-8 TOML") as caught:
                    runtime.create(args)
                self.assertNotIn("must-not-appear", str(caught.exception))
            invoke.assert_not_called()

    def test_unreadable_oversized_symlink_and_nonregular_files_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            policy = root / "policy.toml"
            policy.write_text('version = 1\n')
            policy.chmod(0o000)
            with self.assertRaises((PermissionError, ValueError)):
                node_policy_file(SimpleNamespace(node_policy_file=policy))
            policy.chmod(0o644)
            policy.write_bytes(b'x' * 32769)
            with self.assertRaisesRegex(ValueError, "32768"):
                node_policy_file(SimpleNamespace(node_policy_file=policy))
            alias = root / "alias"
            alias.symlink_to(policy)
            with self.assertRaisesRegex(ValueError, "non-symlink"):
                node_policy_file(SimpleNamespace(node_policy_file=alias))
            with self.assertRaisesRegex(ValueError, "regular TOML"):
                node_policy_file(SimpleNamespace(node_policy_file=root))
