# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Artifact rejection and cross-runtime resource contract tests."""

import hashlib
import io
import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from artifact import CONFIG, MANIFEST, inspect, receipt
from runtime import Runtime


def archive(path, bad_layer=False, bad_type=False, unsafe=False):
    files = {"oci-layout": json.dumps({"imageLayoutVersion": "1.0.0"}).encode()}

    def blob(value, media):
        data = value if isinstance(value, bytes) else json.dumps(value).encode()
        digest = hashlib.sha256(data).hexdigest()
        files["blobs/sha256/" + digest] = data
        return {"digest": "sha256:" + digest, "size": len(data), "mediaType": media}

    layer = blob(b"test filesystem", "application/vnd.oci.image.layer.v1.tar")
    config = blob({"os": "linux", "architecture": "amd64", "rootfs": {"type": "layers", "diff_ids": [layer["digest"]]},
                   "config": {"Labels": {"org.opencontainers.image.revision": "revision", "org.opencontainers.image.version": "version",
                                         "org.crowdb.runtime.sha256": "runtime"}}}, CONFIG)
    manifest = blob({"schemaVersion": 2, "mediaType": MANIFEST, "config": config, "layers": [layer]}, MANIFEST)
    if bad_type:
        manifest["mediaType"] = "application/vnd.docker.distribution.manifest.v2+json"
    if bad_layer:
        files["blobs/sha256/" + layer["digest"].split(":")[1]] = b"corrupt content"
    files["index.json"] = json.dumps({"schemaVersion": 2, "manifests": [manifest]}).encode()
    if unsafe:
        files["../escape"] = b"unsafe"
    with tarfile.open(path, "w") as output:
        for name, data in files.items():
            member = tarfile.TarInfo(name)
            member.size = len(data)
            output.addfile(member, io.BytesIO(data))


class ArtifactTest(unittest.TestCase):
    def test_archive_and_source_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar"
            archive(path)
            result = inspect(path)
            result.update(revision="revision", version="version", runtime_sha256="runtime")
            Path(str(path) + ".json").write_text(json.dumps(result))
            self.assertEqual(receipt(path)["platform"], "linux/amd64")
            result["revision"] = "another revision"
            Path(str(path) + ".json").write_text(json.dumps(result))
            with self.assertRaisesRegex(ValueError, "source metadata"):
                receipt(path)

    def test_rejects_corruption_media_types_and_path_escape(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar"
            for option in ("bad_layer", "bad_type", "unsafe"):
                with self.subTest(option=option):
                    archive(path, **{option: True})
                    with self.assertRaises(ValueError):
                        inspect(path)

    def test_rejects_missing_blob(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar"
            archive(path)
            with tarfile.open(path) as original:
                items = [(m, original.extractfile(m).read()) for m in original.getmembers()]
            with tarfile.open(path, "w") as output:
                for member, data in items[1:]:
                    output.addfile(member, io.BytesIO(data))
            with self.assertRaises(KeyError):
                inspect(path)


class ResourceTest(unittest.TestCase):
    def test_containerd_inspects_the_exact_manifest_without_tag_aliases(self):
        with patch("runtime.tool", return_value="nerdctl"):
            runtime = Runtime("containerd")
        with patch.object(runtime, "inspect", return_value={"Os": "linux", "Architecture": "amd64"}) as inspect_image:
            runtime.image("registry/node@sha256:verified")
        inspect_image.assert_called_once_with("sha256:verified", image=True)

    def test_unlimited_resources_are_rejected(self):
        runtime = Runtime("docker")
        with patch.object(runtime, "invoke", return_value="max 100000"), self.assertRaisesRegex(RuntimeError, "CPU quota"):
            runtime.verify("node", 2, 1024, False)
        with patch.object(runtime, "invoke", side_effect=["200000 100000", "max"]), self.assertRaisesRegex(RuntimeError, "memory limit"):
            runtime.verify("node", 2, 1024, False)

    def test_nonhost_namespace_is_rejected(self):
        runtime = Runtime("docker")
        with patch.object(runtime, "invoke", side_effect=["200000 100000", "1073741824", "net:[wrong]"]), self.assertRaisesRegex(RuntimeError, "network namespace"):
            runtime.verify("node", 2, 1024, True)


if __name__ == "__main__":
    unittest.main()
