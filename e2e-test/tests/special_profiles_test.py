# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import contextlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import run
from special_profiles import cleanup_owned_host


class HostProfileTest(unittest.TestCase):
    def test_selection_and_resource_validation_precede_docker(self):
        for args in (("--concurrency","2"),("--concurrency","1","--cpus","2"),
                     ("--concurrency","1","--memory-mib","1024"),
                     ("--concurrency","1","--layer","kv")):
            with patch.object(sys,"argv",["run.py","--profile","host",*args]), \
                    patch("special_profiles.docker") as docker, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit): run.main()
                docker.assert_not_called()
        output = io.StringIO()
        with patch.object(sys,"argv",["run.py","--profile","host","--concurrency","1","--list"]), \
                patch("special_profiles.docker") as docker, contextlib.redirect_stdout(output):
            run.main()
            docker.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())[0]["scope"],"deployment.host")

    def test_interruption_cleanup_refuses_foreign_data_or_changed_image(self):
        for image, source in (("foreign-image","/owned/node"),("image","/foreign/node")):
            info = {"Image":image,"Mounts":[{"Destination":"/opt/crowdb/data","Source":source}]}
            with patch("special_profiles.docker",side_effect=["owned",json.dumps([info])]) as docker:
                with self.assertRaisesRegex(ValueError,"ownership"):
                    cleanup_owned_host("owned",Path("/owned"),"image")
                self.assertEqual(docker.call_count,2)
