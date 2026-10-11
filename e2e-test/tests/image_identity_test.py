# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Verify image provenance across client commits and dirty server edits."""

import subprocess
import tempfile
import unittest
from pathlib import Path

from image_identity import verify_server_sources


class ImageIdentityTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git("init", "-q")
        for path in ("app/server/src/main.rs", "app/server/tests/api_test.rs", "e2e-test/client.py", "doc/test.md"):
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("original\n")
        self.manifest = self.root / "app/server/Cargo.toml"
        self.manifest.write_text('[package]\nname = "server"\nversion = "0.1.0"\n\n'
                                 '[[test]]\nname = "api_test"\npath = "tests/api_test.rs"\n')
        self.commit()
        self.revision = self.git("rev-parse", "HEAD")

    def git(self, *arguments):
        return subprocess.check_output(["git", "-C", str(self.root), *arguments], text=True).strip()

    def commit(self):
        self.git("add", ".")
        self.git("-c", "user.name=test", "-c", "user.email=test@example.invalid",
                 "commit", "-qm", "fixture")

    def test_client_and_component_test_changes_reuse_release_image(self):
        (self.root / "e2e-test/client.py").write_text("new client\n")
        (self.root / "app/server/tests/api_test.rs").unlink()
        self.manifest.write_text('[package]\nname = "server"\nversion = "0.1.0"\n')
        self.commit()
        (self.root / "doc/test.md").write_text("new measurements\n")
        identity = verify_server_sources(self.root, self.revision)
        self.assertTrue(identity["server_sources_match"])
        self.assertNotEqual(identity["client_revision"], identity["image_revision"])

    def test_dirty_server_edit_cannot_use_stale_image(self):
        (self.root / "app/server/src/main.rs").write_text("changed server\n")
        with self.assertRaisesRegex(ValueError, "server sources differ"):
            verify_server_sources(self.root, self.revision)

    def test_unpackaged_operator_documentation_reuses_image_but_entrypoint_changes_do_not(self):
        directory = self.root / "container/single-node-container"
        directory.mkdir(parents=True)
        (directory / "README.md").write_text("operator instructions\n")
        (directory / "entrypoint.sh").write_text("release runtime\n")
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        (directory / "README.md").write_text("new service-selection instructions\n")
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        (directory / "entrypoint.sh").write_text("changed runtime\n")
        with self.assertRaisesRegex(ValueError, "entrypoint.sh"):
            verify_server_sources(self.root, revision)

    def test_untracked_server_input_cannot_use_stale_image(self):
        (self.root / "app/server/src/extra.rs").write_text("new input\n")
        with self.assertRaisesRegex(ValueError, "extra.rs"):
            verify_server_sources(self.root, self.revision)

    def test_missing_source_label_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "full source revision"):
            verify_server_sources(self.root, None)

    def test_only_proven_dev_dependency_edges_can_change_in_existing_lock_packages(self):
        self.manifest.write_text(self.manifest.read_text() + '\n[dev-dependencies]\nhelper = "1"\n')
        lock = self.root / "Cargo.lock"
        lock.write_text('version = 4\n[[package]]\nname = "server"\nversion = "0.1.0"\n'
                        'dependencies = ["helper", "production"]\n[[package]]\nname = "helper"\nversion = "1"\n')
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        self.manifest.write_text(self.manifest.read_text().replace('helper = "1"', ''))
        lock.write_text(lock.read_text().replace('["helper", "production"]', '["production"]'))
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        lock.write_text(lock.read_text().replace('["production"]', '[]'))
        with self.assertRaisesRegex(ValueError, "Cargo.lock"):
            verify_server_sources(self.root, revision)

    def test_production_manifest_changes_require_rebuilding(self):
        self.manifest.write_text('[package]\nname = "server"\nversion = "0.2.0"\n')
        with self.assertRaisesRegex(ValueError, "Cargo.toml"):
            verify_server_sources(self.root, self.revision)

    def test_client_lock_entry_changes_reuse_image_but_server_dependency_changes_do_not(self):
        lock = self.root / "Cargo.lock"
        lock.write_text('version = 4\n[[package]]\nname = "e2e-test"\nversion = "1"\n'
                        'dependencies = []\n[[package]]\nname = "server"\nversion = "1"\n')
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        lock.write_text(lock.read_text().replace('dependencies = []', 'dependencies = ["server"]'))
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        lock.write_text(lock.read_text().replace('name = "server"\nversion = "1"',
                                                 'name = "server"\nversion = "2"'))
        with self.assertRaisesRegex(ValueError, "Cargo.lock"):
            verify_server_sources(self.root, revision)
