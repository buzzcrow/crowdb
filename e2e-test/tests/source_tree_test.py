# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Built working trees preserve provenance without committing user changes."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from image_identity import verify_server_sources

TOOL = Path(__file__).resolve().parents[2] / "container/oci-image/source_tree.py"


class SourceTreeTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.git("init", "-q")
        self.server = self.root / "app/server/src/main.rs"
        self.server.parent.mkdir(parents=True)
        self.server.write_text("original\n")
        self.git("add", ".")
        self.git("-c", "user.name=test", "-c", "user.email=test@example.invalid",
                 "commit", "-qm", "fixture")
        self.revision = self.git("rev-parse", "HEAD")

    def git(self, *arguments):
        return subprocess.check_output(["git", "-C", str(self.root), *arguments], text=True).strip()

    def snapshot(self):
        return subprocess.check_output([sys.executable, str(TOOL), str(self.root)], text=True).strip()

    def test_built_dirty_tree_preserves_head_and_existing_staging(self):
        self.server.write_text("staged\n")
        self.git("add", ".")
        self.server.write_text("built working content\n")
        before = self.git("diff", "--cached", "--binary")
        tree = self.snapshot()
        self.assertEqual(self.git("rev-parse", "HEAD"), self.revision)
        self.assertEqual(self.git("diff", "--cached", "--binary"), before)
        self.assertEqual(self.git("show", f"{tree}:app/server/src/main.rs"), "built working content")
        self.assertTrue(verify_server_sources(self.root, self.revision, tree)["server_sources_match"])
        self.server.write_text("edited after build\n")
        with self.assertRaisesRegex(ValueError, "main.rs"):
            verify_server_sources(self.root, self.revision, tree)

    def test_built_untracked_inputs_are_guarded_against_changes_and_deletion(self):
        extra = self.server.with_name("extra.rs")
        extra.write_text("built untracked input\n")
        tree = self.snapshot()
        self.assertTrue(verify_server_sources(self.root, self.revision, tree)["server_sources_match"])
        extra.write_text("changed input\n")
        with self.assertRaisesRegex(ValueError, "extra.rs"):
            verify_server_sources(self.root, self.revision, tree)
        extra.unlink()
        with self.assertRaisesRegex(ValueError, "extra.rs"):
            verify_server_sources(self.root, self.revision, tree)

    def test_client_changes_reuse_snapshot_but_new_production_manifest_does_not(self):
        tree = self.snapshot()
        tests = self.server.parent.parent / "tests"
        tests.mkdir()
        (tests / "new_test.rs").write_text("host test\n")
        self.assertTrue(verify_server_sources(self.root, self.revision, tree)["server_sources_match"])
        (self.server.parent.parent / "Cargo.toml").write_text('[package]\nname="new-server"\n')
        with self.assertRaisesRegex(ValueError, "Cargo.toml"):
            verify_server_sources(self.root, self.revision, tree)

    def test_source_label_must_be_an_available_tree(self):
        for tree in ("invalid", self.revision, "0" * 40):
            with self.subTest(tree=tree), self.assertRaises(ValueError):
                verify_server_sources(self.root, self.revision, tree)
