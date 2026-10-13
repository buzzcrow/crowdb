# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""CI ownership must survive task consolidation without silently losing flows."""

import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/ci-checks"))
from e2e_coverage import check_coverage
from task_graph import TaskGraph

spec = importlib.util.spec_from_file_location("test_dispatch", ROOT / "tools/pixi-tasks/test.py")
dispatch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dispatch)


class TestDispatchTest(unittest.TestCase):
    def setUp(self):
        self.workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        self.graph = TaskGraph(ROOT)

    def test_regular_ci_has_one_owner_per_complete_domain(self):
        self.assertEqual(set(dispatch.DOMAINS), set(check_coverage(self.workflow, self.graph)))

    def test_diagnostic_exports_and_uploads_are_outside_compilation_cache(self):
        export = '${{ runner.temp }}/container-e2e-public-${{ github.run_id }}-${{ github.run_attempt }}'
        lines = self.workflow.splitlines()
        commands = [line for line in lines if 'python e2e-test/publish.py' in line]
        uploads = [line.strip().removeprefix('path: ') for line in lines
                   if 'path:' in line and 'container-e2e-public' in line]
        self.assertTrue(commands)
        self.assertEqual(len(commands), len(uploads))
        for command in commands:
            self.assertIn(f'--output "{export}"', command)
        self.assertEqual(set(uploads), {export + '/'})

    def test_compilation_cache_excludes_rootful_oci_state_and_runtime_artifacts(self):
        image = self.workflow.split('  ContainerImage:', 1)[1].split('  ContainerKV:', 1)[0]
        self.assertIn('cache-targets: false', image)
        self.assertIn('cache-directories: |\n          target/release\n          app/crowdb-diskio/build', image)
        self.assertIn("shared-key: container-release-v2-${{ hashFiles('pixi.lock', 'container/oci-image/stage.sh') }}", image)
        for job, following in (('IcebergE2E', 'ConsoleTests'), ('ContainerConsole', 'ContainerStorage')):
            block = self.workflow.split(f'  {job}:', 1)[1].split(f'  {following}:', 1)[0]
            self.assertIn('cache-targets: false', block)
            self.assertIn('cache-directories: target/debug', block)
            self.assertIn("shared-key: container-e2e-client-v2-${{ hashFiles('pixi.lock') }}", block)

    def test_missing_and_duplicate_domains_are_rejected(self):
        for workflow in (
                self.workflow.replace("pixi run test-e2e-s3", "echo removed"),
                self.workflow.replace("pixi run test-e2e-iceberg", "pixi run test-e2e-s3")):
            with self.assertRaisesRegex(ValueError, "container CI coverage"):
                check_coverage(workflow, self.graph)

    def test_incomplete_duplicate_or_undispatched_partition_is_rejected(self):
        for workflow in (
            self.workflow.replace("iceberg-core, iceberg-sdk, iceberg-grace", "iceberg-core, iceberg-sdk"),
            self.workflow.replace("iceberg-core, iceberg-sdk, iceberg-grace", "iceberg-core, iceberg-sdk, iceberg-sdk"),
            self.workflow.replace(' --partition "$E2E_PARTITION"', ''),
            self.workflow.replace(' --partition "$E2E_PARTITION"', ' --partition iceberg-core'),
        ):
            with self.assertRaisesRegex(ValueError, "partition"):
                check_coverage(workflow, self.graph)

    def test_narrowed_task_is_rejected(self):
        self.graph.tasks["default", "test-e2e-s3"] += " --scope s3.protocol"
        with self.assertRaisesRegex(ValueError, "without narrowing"):
            check_coverage(self.workflow, self.graph)

    def test_narrowed_workflow_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "narrows a required domain"):
            check_coverage(self.workflow.replace("pixi run test-e2e-s3",
                                                "pixi run test-e2e-s3 --scope s3.protocol"), self.graph)

    def test_hidden_aggregate_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate ownership"):
            check_coverage(self.workflow + "\n      run: pixi run test-suite\n", self.graph)

    def test_rust_assignments_match_the_executable_dispatch(self):
        spec = importlib.util.spec_from_file_location("check_tasks", ROOT / "tools/ci-checks/check-ci-test-tasks.py")
        checker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(checker)
        self.assertEqual(checker.COMPONENT_PACKAGES, {
            f"test-{group}": {f"crowdb-{package}" for package in packages}
            for group, packages in dispatch.RUST_GROUPS.items()})

    def test_kv_keeps_isolation_and_resources_can_be_overridden(self):
        with patch.dict(os.environ, {}, clear=True):
            command = dispatch.container_command("e2e", "kv", ["--memory-mib", "8192"])
        self.assertIn("--require-isolation", command)
        self.assertEqual(command[command.index("--node-cpus") + 1], "1")
        self.assertEqual(command[-2:], ["--memory-mib", "8192"])

    def test_ctest_keeps_parent_cases_and_excludes_nested_owners(self):
        directory = ROOT / "lib/crowdb-tree/build"
        listing = {"tests": [
            {"name": "Tree.ExactBytes", "command": [str(directory / "crowdb_tree_tests")]},
            {"name": "Rpc.Retry[1]", "command": ["cmake", "-D",
                f"TEST_EXECUTABLE={directory / 'crowdb-rpc-build/crowdb_rpc_tests'}"]}]}
        with patch.object(dispatch.subprocess, "check_output", return_value=json.dumps(listing)):
            command = dispatch.ctest_command("lib/crowdb-tree/build")
        import re
        self.assertIsNone(re.search(command[-1], "Tree.ExactBytes"))
        self.assertIsNotNone(re.search(command[-1], "Rpc.Retry[1]"))
        self.assertIsNone(re.search(command[-1], "Rpc.Retry1"))

    def test_ctest_refuses_ambiguous_domain_ownership(self):
        directory = ROOT / "lib/crowdb-tree/build"
        listing = {"tests": [
            {"name": "Shared.Name", "command": [str(path / "tests")]}
            for path in (directory, directory / "nested")]}
        with patch.object(dispatch.subprocess, "check_output", return_value=json.dumps(listing)):
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                dispatch.ctest_command("lib/crowdb-tree/build")
