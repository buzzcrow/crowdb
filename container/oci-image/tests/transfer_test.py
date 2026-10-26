# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Docker transport conversion preserves executable image content."""

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from transfer import verify_import


class TransferTest(unittest.TestCase):
    def setUp(self):
        self.artifact = {"config_digest": "sha256:original", "platform": "linux/amd64",
                         "config": {"Entrypoint": ["/entrypoint"], "Env": ["KEY=value"]},
                         "rootfs": {"type": "layers", "diff_ids": ["sha256:first", "sha256:second"]}}
        self.image = {"Id": "sha256:rewritten", "Os": "linux", "Architecture": "amd64",
                      "Config": copy.deepcopy(self.artifact["config"]),
                      "RootFS": {"Type": "layers", "Layers": ["sha256:first", "sha256:second"]}}

    def test_rewritten_image_id_preserves_config_and_filesystem(self):
        verify_import(self.image, self.artifact)

    def test_docker_28_explicit_defaults_preserve_oci_execution_config(self):
        self.image["Config"].update({
            "Hostname": "", "Domainname": "", "User": "", "AttachStdin": False,
            "AttachStdout": False, "AttachStderr": False, "Tty": False,
            "OpenStdin": False, "StdinOnce": False, "Cmd": None, "Image": "",
            "Volumes": None, "WorkingDir": "", "OnBuild": None, "Labels": None,
        })
        verify_import(self.image, self.artifact)

    def test_non_default_and_unknown_docker_fields_are_rejected(self):
        for key, value in (("Hostname", "changed"), ("AttachStdin", True),
                           ("WorkingDir", "/changed"), ("Cmd", ["changed"]),
                           ("OnBuild", ["RUN changed"]), ("Unexpected", None)):
            with self.subTest(field=key):
                image = copy.deepcopy(self.image)
                image["Config"][key] = value
                with self.assertRaisesRegex(RuntimeError, "image config differs"):
                    verify_import(image, self.artifact)

    def test_changed_execution_config_is_rejected(self):
        self.image["Config"]["Entrypoint"] = ["/another-entrypoint"]
        with self.assertRaisesRegex(RuntimeError, "image config differs"):
            verify_import(self.image, self.artifact)

    def test_changed_or_reordered_filesystem_is_rejected(self):
        for layers in (["sha256:changed", "sha256:second"], ["sha256:second", "sha256:first"], []):
            with self.subTest(layers=layers):
                self.image["RootFS"]["Layers"] = layers
                with self.assertRaisesRegex(RuntimeError, "filesystem differs"):
                    verify_import(self.image, self.artifact)

    def test_changed_platform_is_rejected(self):
        self.image["Architecture"] = "arm64"
        with self.assertRaisesRegex(RuntimeError, "platform differs"):
            verify_import(self.image, self.artifact)


if __name__ == "__main__":
    unittest.main()
