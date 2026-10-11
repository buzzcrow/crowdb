# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import contextlib
import io
import json
import sys
import unittest
from unittest.mock import patch
import run
from containerd_profile import cleanup


class ContainerdProfileTest(unittest.TestCase):
    def test_profile_budget_and_selection_fail_before_runtime_access(self):
        for args in (("--concurrency","2"),("--concurrency","1","--node-cpus",".75","--cpus","3.49"),
                     ("--concurrency","1","--memory-mib","4096"),("--concurrency","1","--layer","kv")):
            with patch.object(sys,"argv",["run.py","--profile","containerd-cluster",*args]), \
                    patch("containerd_profile.docker") as docker, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit): run.main()
                docker.assert_not_called()
        for profile, nodes in (("containerd-cluster",3),("containerd-host",1)):
            output=io.StringIO()
            with patch.object(sys,"argv",["run.py","--profile",profile,"--concurrency","1",
                    "--node-cpus",".75","--cpus","4","--memory-mib","6144","--list"]), \
                    patch("containerd_profile.docker") as docker, contextlib.redirect_stdout(output):
                run.main()
                docker.assert_not_called()
            self.assertEqual(json.loads(output.getvalue())[0]["nodes"],nodes)

    def test_partial_cleanup_failure_does_not_abandon_other_owned_hosts_or_network(self):
        with patch("containerd_profile.docker",side_effect=["owned-0\nowned-1",RuntimeError("first removal failed"),"","owned",""]) as docker:
            with self.assertRaisesRegex(RuntimeError,"first removal failed"):
                cleanup("owned")
            self.assertEqual(docker.call_count,5)
            self.assertEqual(docker.call_args_list[2].args,("container","rm","-f","owned-1"))
            self.assertEqual(docker.call_args_list[-1].args,("network","rm","owned"))

    def test_unexpected_labeled_resource_is_preserved(self):
        with patch("containerd_profile.docker",side_effect=["foreign-node",""]) as docker:
            with self.assertRaisesRegex(RuntimeError,"unexpected labeled resource"):
                cleanup("owned")
            self.assertEqual(docker.call_count,2)
