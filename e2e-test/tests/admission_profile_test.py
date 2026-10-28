# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import contextlib
import io
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import run
from admission_profile import cleanup


class AdmissionProfileTest(unittest.TestCase):
    def test_required_browser_flow_cannot_succeed_with_a_skip(self):
        path = Path(__file__).resolve().parents[1] / "profiles/bridge_node.py"
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "CROWDB_CONTAINER_IMAGE":"exact","CROWDB_E2E_PROFILE_NAME":"owned",
                "CROWDB_E2E_PROFILE_DATA":directory}):
            spec = importlib.util.spec_from_file_location("test_bridge_node",path)
            bridge = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(bridge)
            output = Path(directory) / "browser/prepare"
            output.mkdir(parents=True)
            (output / "results.json").write_text(json.dumps({"stats":{
                "expected":0,"unexpected":0,"flaky":0,"skipped":1}}))
            with patch.object(bridge.subprocess,"run"):
                with self.assertRaisesRegex(AssertionError,"did not pass exactly once"):
                    bridge.browser("http://owned:6135","Docker candidates","prepare")

    def test_profile_reserves_every_node_and_host_browser_before_runtime_access(self):
        for arguments in (("--concurrency","2"), ("--concurrency","1","--cpus","1.49"),
                          ("--concurrency","1","--memory-mib","6143"), ("--concurrency","1","--layer","kv")):
            with patch.object(sys,"argv",["run.py","--profile","bridge-admission",*arguments]), \
                    patch("admission_profile.docker") as docker, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit): run.main()
                docker.assert_not_called()
        output = io.StringIO()
        with patch.object(sys,"argv",["run.py","--profile","bridge-admission","--concurrency","1",
                "--cpus","4","--memory-mib","6144","--list"]), \
                patch("admission_profile.docker") as docker, contextlib.redirect_stdout(output):
            run.main()
            docker.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())[0]["nodes"],5)

    def test_cleanup_continues_after_failed_node_removal(self):
        with patch("admission_profile.docker",side_effect=["owned-0\nowned-1",RuntimeError("remove failed"),"","owned",""]) as docker:
            with self.assertRaisesRegex(RuntimeError,"remove failed"):
                cleanup("owned")
            self.assertEqual(docker.call_args_list[2].args,("container","rm","-f","owned-1"))
            self.assertEqual(docker.call_args_list[-1].args,("network","rm","owned"))

    def test_changed_mount_or_image_and_foreign_names_are_preserved(self):
        for image, source in (("foreign", "/owned/0"),("exact", "/foreign/0")):
            node={"Image":image,"Mounts":[{"Source":source,"Destination":"/opt/crowdb/data"}]}
            with patch("admission_profile.docker",side_effect=["owned-0",json.dumps([node]),""]) as docker:
                with self.assertRaisesRegex(RuntimeError,"changed image or data ownership"):
                    cleanup("owned",Path("/owned"),"exact")
                self.assertEqual(docker.call_count,3)
        with patch("admission_profile.docker",side_effect=["foreign-node","foreign-network"]) as docker:
            with self.assertRaisesRegex(RuntimeError,"foreign labeled"):
                cleanup("owned")
            self.assertEqual(docker.call_count,2)

    def test_each_start_owns_a_policy_file_that_other_nodes_cannot_rewrite(self):
        path = Path(__file__).resolve().parents[1] / "profiles/bridge_node.py"
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "CROWDB_CONTAINER_IMAGE": "exact", "CROWDB_E2E_PROFILE_NAME": "owned",
                "CROWDB_E2E_PROFILE_DATA": directory}):
            spec = importlib.util.spec_from_file_location("test_bridge_policy", path)
            bridge = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(bridge)
            def reply(*arguments):
                if arguments[0] == "inspect":
                    return json.dumps([{"NetworkSettings": {"Networks": {"owned": {"IPAddress": "172.30.0.2"}}}}])
                return ""
            with patch.object(bridge, "docker", side_effect=reply) as docker:
                bridge.start_node("owned-0")
                bridge.start_node("owned-1")
            policies = []
            for call in docker.call_args_list:
                for argument in call.args:
                    if isinstance(argument, str) and "target=/run/bridge-policy.toml" in argument:
                        source = next(field.removeprefix("source=") for field in argument.split(",")
                                      if field.startswith("source="))
                        policies.append(Path(source))
            self.assertEqual(len(policies), 2)
            self.assertNotEqual(policies[0], policies[1])
            self.assertTrue(all(policy.parent == Path(directory) for policy in policies))
            self.assertEqual(policies[0].read_text(), policies[1].read_text())
