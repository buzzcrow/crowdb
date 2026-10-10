# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Release gates preserve the verified graph and isolate publication authority."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from publish import check_source

ROOT = Path(__file__).resolve().parents[3]


class ReleasePolicyTest(unittest.TestCase):
    def test_release_dependencies_and_credential_boundaries(self):
        workflow = yaml.load((ROOT / ".github/workflows/release-container.yml").read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(workflow["on"]), {"workflow_dispatch"})
        jobs = workflow["jobs"]
        self.assertEqual(set(jobs["publish"]["needs"]), {"construct", "verify", "containerd", "kv"})
        self.assertEqual(jobs["publish"]["environment"], "DockerHub")
        for name in ("construct", "verify", "containerd", "kv"):
            self.assertNotIn("id-token", jobs[name]["permissions"])
            self.assertNotIn("secrets.", str(jobs[name]))
            self.assertNotIn("DOCKERHUB_TOKEN", str(jobs[name]))
        self.assertIn("apt-get purge", str(jobs["construct"]))
        self.assertIn("apt-get purge", str(jobs["containerd"]))
        self.assertNotIn("build-push-action", str(jobs["publish"]))
        self.assertIn("publisher release", str(jobs["publish"]))
        self.assertIn("test-single-node-container", str(jobs["verify"]))
        self.assertIn("node-containers.py", str(jobs["verify"]))
        self.assertIn("host-node.py", str(jobs["verify"]))

    def test_missing_failed_or_stale_gate_rejects_publication(self):
        verified = {"version": "0.3.0", "revision": "revision", "image_digest": "sha256:digest"}
        env = {"GITHUB_SHA": "revision", "GITHUB_REF_NAME": "release/0.3.0",
               "DOCKER_VERIFIED_DIGEST": "sha256:digest", "CONTAINERD_VERIFIED_DIGEST": "sha256:digest", "KV_VERIFIED_DIGEST": "sha256:digest"}
        with patch.dict("os.environ", env, clear=True), patch("publish.subprocess.check_output", return_value="revision refs/heads/release/0.3.0\n"):
            check_source(verified, "0.3.0")
            for key, value in (("GITHUB_SHA", "stale"), ("GITHUB_REF_NAME", "main"),
                               ("DOCKER_VERIFIED_DIGEST", ""), ("CONTAINERD_VERIFIED_DIGEST", "different"), ("KV_VERIFIED_DIGEST", "")):
                with self.subTest(key=key), patch.dict("os.environ", {key: value}), self.assertRaises(ValueError):
                    check_source(verified, "0.3.0")
        with patch.dict("os.environ", env, clear=True), patch("publish.subprocess.check_output", return_value="new-head refs/heads/release/0.3.0\n"), self.assertRaisesRegex(ValueError, "branch moved"):
            check_source(verified, "0.3.0")

    def test_preview_never_has_publication_authority(self):
        workflow = yaml.load((ROOT / ".github/workflows/docker-preview.yml").read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(workflow["on"]), {"workflow_dispatch"})
        self.assertNotIn("secrets.", str(workflow))
        self.assertNotIn("id-token", str(workflow))


if __name__ == "__main__":
    unittest.main()
