# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Public diagnostics retain measured failures while private evidence stays local."""

import json
from pathlib import Path
import tempfile
import unittest

from publish import publish


class PublishTest(unittest.TestCase):
    def test_export_preserves_originals_and_excludes_private_data_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / "original", root / "public"
            source.mkdir()
            (source / "nodes").mkdir()
            (source / "nodes/config.toml").write_text('writer_token = "private-value-123"\n')
            (source / "nodes/raw.log").write_text("private node log")
            (source / "runtime.log").write_text(
                "failure private-value-123\nAuthorization: Bearer other-secret-123\n"
                'CROWDB_S3_MASTER_KEY=1111111111111111\n'
                'Authorization: Basic encoded-user-password\n'
                'Credential=access-id-123/20261012/region/s3/aws4_request, Signature=signed-value-123\n')
            (source / "results.json").write_text(json.dumps({"status": "failed", "seconds": 12.5}))
            (source / "copied.log").symlink_to(source / "nodes/raw.log")
            (source / "service.toml").write_text('writer_token = "private-value-123"')
            self.assertEqual(publish(source, output), 2)
            self.assertEqual(json.loads((output / "results.json").read_text()),
                             {"status": "failed", "seconds": 12.5})
            text = (output / "runtime.log").read_text()
            for secret in ("private-value-123", "other-secret-123", "1111111111111111",
                           "encoded-user-password", "access-id-123", "signed-value-123"):
                self.assertNotIn(secret, text)
            self.assertIn("private-value-123", (source / "runtime.log").read_text())
            self.assertFalse((output / "nodes").exists())
            self.assertFalse((output / "copied.log").exists())
            self.assertFalse((output / "service.toml").exists())

    def test_export_cannot_rewrite_or_recurse_into_original_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for output in (root, root / "public", root.parent):
                with self.assertRaises(ValueError):
                    publish(root, output)

    def test_json_redaction_preserves_boolean_measurements_and_nested_structure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "original"
            source.mkdir()
            (source / "results.json").write_text(json.dumps({
                "token_registered": False, "config": {"writer_token": "secret-value"},
                "cases": [{"status": "failed", "seconds": 1.0}]}))
            publish(source, root / "public")
            result = json.loads((root / "public/results.json").read_text())
            self.assertFalse(result["token_registered"])
            self.assertEqual(result["config"]["writer_token"], "<redacted>")
            self.assertEqual(result["cases"], [{"status": "failed", "seconds": 1.0}])
