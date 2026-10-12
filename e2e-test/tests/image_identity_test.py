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

    def test_host_rpc_benchmark_does_not_exempt_server_rpc_sources(self):
        benchmark = self.root / "app/crowdb-cli/src/commands/bench/rpc.rs"
        benchmark.parent.mkdir(parents=True)
        benchmark.write_text("host benchmark\n")
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        benchmark.write_text("changed host benchmark\n")
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        (self.root / "app/server/src/main.rs").write_text("changed server RPC\n")
        with self.assertRaisesRegex(ValueError, "server sources differ"):
            verify_server_sources(self.root, revision)

    def test_host_container_cli_does_not_exempt_shared_deployment_or_runtime_inputs(self):
        source = self.root / "app/crowdb-cli/src/commands/container_cluster/runtime.rs"
        source.parent.mkdir(parents=True)
        source.write_text("host lifecycle\n")
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        source.write_text("changed host lifecycle\n")
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        shared = self.root / "lib/crowdb-console-shared/src/deployment.rs"
        shared.parent.mkdir(parents=True)
        shared.write_text("changed server dependency\n")
        with self.assertRaisesRegex(ValueError, "server sources differ"):
            verify_server_sources(self.root, revision)

    def test_component_task_edit_does_not_exempt_build_tasks_or_environment(self):
        manifest = self.root / "pixi.toml"
        original = '[dependencies]\npython = "3.14.*"\n[tasks]\ntest-storage = "old gate"\nbuild = "release build"\n'
        manifest.write_text(original)
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        manifest.write_text(original.replace('old gate', 'component gate'))
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        for changed in (original.replace('release build', 'changed build'),
                        original.replace('3.14.*', '3.13.*')):
            manifest.write_text(changed)
            with self.assertRaisesRegex(ValueError, "pixi.toml"):
                verify_server_sources(self.root, revision)

    def test_sdk_component_task_addition_preserves_environment_and_release_guards(self):
        manifest = self.root / "pixi.toml"
        original = ('[feature.iceberg-e2e.dependencies]\npython = "3.12.*"\n'
                    '[feature.iceberg-e2e.tasks]\ntest-iceberg-e2e = "existing container gate"\n')
        manifest.write_text(original)
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        added = original + 'test-iceberg-sdk-component = "host SDK proof"\n'
        manifest.write_text(added)
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        manifest.write_text(added.replace('3.12.*', '3.13.*'))
        with self.assertRaisesRegex(ValueError, "pixi.toml"):
            verify_server_sources(self.root, revision)

    def test_manual_engine_delegate_preserves_dependency_and_build_guards(self):
        manifest = self.root / "pixi.toml"
        original = ('[feature.iceberg-ecosystem.dependencies]\npython = "3.12.*"\n'
                    '[feature.iceberg-ecosystem.tasks]\ntest-iceberg-container-ecosystem = "old host client"\n'
                    '[tasks]\nbuild = "release build"\n')
        manifest.write_text(original)
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        manifest.write_text(original.replace("old host client", "root container delegate"))
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        for changed in (original.replace("3.12.*", "3.13.*"), original.replace("release build", "other build")):
            manifest.write_text(changed)
            with self.assertRaisesRegex(ValueError, "pixi.toml"):
                verify_server_sources(self.root, revision)

    def test_unbundled_browser_spec_can_move_without_exempting_ui_sources(self):
        directory=self.root / "app/crowdb-web/ui"
        spec=directory / "e2e/flows/12-cluster-discovery.spec.ts"
        shell=directory / "e2e/flows/00-shell-embedding.spec.ts"
        source=directory / "src/App.tsx"
        registry=directory / "e2e/flows/10-cluster-rack-node.spec.ts"
        warning=directory / "e2e/flows/13-todo-ui-behavior.spec.ts"
        native=directory / "e2e/flows/71-s3-native.spec.ts"
        selectors=[directory / "e2e" / name for name in
                   ("nativeS3.config.ts", "nativeDiagnostics.config.ts", "realBackend.config.ts")]
        for path in (spec,shell,registry,warning,source,native,*selectors):
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_text("original\n")
        self.commit()
        revision=self.git("rev-parse","HEAD")
        spec.write_text("migrated host browser cases\n")
        shell.unlink()
        registry.write_text("remaining native registry cases\n")
        warning.write_text("remaining native service deployment cases\n")
        native.unlink()
        for selector in selectors:
            selector.write_text("migrated host selection\n")
        self.assertTrue(verify_server_sources(self.root,revision)["server_sources_match"])
        source.write_text("changed deployed UI\n")
        with self.assertRaisesRegex(ValueError,"App.tsx"):
            verify_server_sources(self.root,revision)

    def test_dirty_server_edit_cannot_use_stale_image(self):
        (self.root / "app/server/src/main.rs").write_text("changed server\n")
        with self.assertRaisesRegex(ValueError, "server sources differ"):
            verify_server_sources(self.root, self.revision)

    def test_retired_host_harness_does_not_exempt_new_runtime_dependencies(self):
        harness = self.root / "lib/crowdb-test-harness/src/cluster.rs"
        harness.parent.mkdir(parents=True)
        harness.write_text("old native host fixture\n")
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        harness.unlink()
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        self.manifest.write_text(self.manifest.read_text() +
                                 '\n[dependencies]\ncrowdb-test-harness = { path = "../../lib/crowdb-test-harness" }\n')
        with self.assertRaisesRegex(ValueError, "Cargo.toml"):
            verify_server_sources(self.root, revision)

    def test_host_sdk_delegates_do_not_exempt_release_build_tooling(self):
        directory = self.root / "tools/pixi-tasks"
        directory.mkdir(parents=True)
        for name in ("test-core.sh", "test-storage.sh", "test-access.sh", "test-s3-e2e.sh", "test-s3-client.sh", "test-s3-sdk.sh", "test-iceberg-native.sh", "test-iceberg-e2e.sh", "test-iceberg-listing-native.sh", "test-pyiceberg-e2e.sh", "test-rust-iceberg-e2e.sh", "test-java-iceberg-fileio-e2e.sh", "build-cpp.sh"):
            (directory / name).write_text("original\n")
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        for name in ("test-core.sh", "test-storage.sh", "test-access.sh", "test-s3-e2e.sh", "test-s3-client.sh", "test-s3-sdk.sh", "test-iceberg-native.sh", "test-iceberg-e2e.sh", "test-iceberg-listing-native.sh", "test-pyiceberg-e2e.sh", "test-rust-iceberg-e2e.sh", "test-java-iceberg-fileio-e2e.sh"):
            (directory / name).write_text("changed host client delegate\n")
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        (directory / "build-cpp.sh").write_text("changed release build input\n")
        with self.assertRaisesRegex(ValueError, "build-cpp.sh"):
            verify_server_sources(self.root, revision)

    def test_unpackaged_operator_documentation_reuses_image_but_entrypoint_changes_do_not(self):
        directory = self.root / "container/single-node-container"
        directory.mkdir(parents=True)
        oci = self.root / "container/oci-image"
        oci.mkdir(parents=True)
        (oci / "README.md").write_text("operator instructions\n")
        (oci / "Dockerfile").write_text("release image recipe\n")
        (directory / "README.md").write_text("operator instructions\n")
        (directory / "entrypoint.sh").write_text("release runtime\n")
        self.commit()
        revision = self.git("rev-parse", "HEAD")
        (oci / "README.md").write_text("root containerd acceptance instructions\n")
        (directory / "README.md").write_text("new service-selection instructions\n")
        self.assertTrue(verify_server_sources(self.root, revision)["server_sources_match"])
        (oci / "Dockerfile").write_text("changed image recipe\n")
        with self.assertRaisesRegex(ValueError, "Dockerfile"):
            verify_server_sources(self.root, revision)
        (oci / "Dockerfile").write_text("release image recipe\n")
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
