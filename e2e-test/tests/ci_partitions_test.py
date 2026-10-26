# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""CI partitioning preserves every registered case exactly once."""

import unittest
import sys
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ci_partitions import PARTITIONS, NATIVE_DATA_SCOPES
from scenarios import select_cases


class CiPartitionsTest(unittest.TestCase):
    def test_complete_disjoint_domain_coverage(self):
        for domain, partitions in PARTITIONS.items():
            with self.subTest(domain=domain):
                expected = Counter(case.name for case in select_cases(layer=domain))
                actual = Counter()
                for partition in partitions:
                    cases = select_cases(layer=domain, partition=partition)
                    self.assertTrue(cases, partition)
                    actual.update(case.name for case in cases)
                self.assertEqual(actual, expected)
                self.assertTrue(all(count == 1 for count in actual.values()))

    def test_expensive_cases_have_independent_owners(self):
        for partition, domain, name in (
            ("console-transition", "console", "console::native_browser::production_split_and_transfer_journals"),
            ("console-inspection", "console", "console::native_browser::page_layout_and_iceberg_inspection"),
            ("iceberg-grace", "iceberg", "access_iceberg::rust_retired::official_rust_client_rejects_retired_native_catalog_after_full_grace"),
            ("iceberg-gc-convergence", "iceberg", "access_iceberg::gc::convergence::expired_inline_backlog_completes_with_foreground_progress"),
        ):
            self.assertEqual([case.name for case in select_cases(layer=domain, partition=partition)], [name])

    def test_iceberg_gc_runs_separately_from_ordinary_and_sdk_cases(self):
        expected = select_cases(layer="iceberg", scope="iceberg.gc")
        self.assertTrue(expected)
        actual = tuple(case for partition in ("iceberg-gc-convergence", "iceberg-gc-cleanup")
                       for case in select_cases(layer="iceberg", partition=partition))
        self.assertCountEqual(actual, expected)
        self.assertEqual(
            tuple(case for case in select_cases(layer="iceberg") if case.iceberg_gc),
            expected,
        )
        core = select_cases(layer="iceberg", partition="iceberg-core")
        self.assertTrue(core)
        self.assertTrue(all(not case.iceberg_gc for case in core))
        self.assertTrue(all(not case.scope.startswith("iceberg.sdk.") for case in core))

    def test_native_data_and_operations_preserve_stable_scope_ownership(self):
        data = select_cases(layer="console", partition="console-native-data")
        operations = select_cases(layer="console", partition="console-native-operations")
        self.assertEqual({case.scope for case in data}, NATIVE_DATA_SCOPES)
        self.assertTrue(all(case.scope.startswith("console.native.") for case in operations))
        self.assertTrue(all(case.scope not in NATIVE_DATA_SCOPES for case in operations))
        expected = tuple(case for case in select_cases(layer="console")
                         if case.scope.startswith("console.native.")
                         and case not in select_cases(layer="console", partition="console-transition")
                         and case not in select_cases(layer="console", partition="console-inspection"))
        self.assertCountEqual(data + operations, expected)

    def test_deployment_reference_flows_are_separate_from_service_recovery(self):
        references = select_cases(layer="deployment", partition="deployment-reference")
        services = select_cases(layer="deployment", partition="deployment-services")
        self.assertTrue(references)
        self.assertTrue(services)
        self.assertTrue(all(case.scope == "deployment.reference" for case in references))
        self.assertTrue(all(case.scope != "deployment.reference" for case in services))
        self.assertEqual({(case.oci_reference, case.oci_editable_ui) for case in references}, {
            ("docker-one", False), ("docker-three", False),
            ("compose-one", False), ("compose-three", False),
            ("docker-one", True), ("docker-three", True)})

    def test_iceberg_sdk_selection_preserves_the_full_grace_owner(self):
        grace = select_cases(layer="iceberg", partition="iceberg-grace")
        self.assertEqual(len(grace), 1)
        expected = tuple(
            case for case in select_cases(layer="iceberg")
            if case.scope.startswith("iceberg.sdk.") and case not in grace
        )
        self.assertEqual(select_cases(layer="iceberg", partition="iceberg-sdk"), expected)
