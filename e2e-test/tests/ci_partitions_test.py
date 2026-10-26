# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""CI partitioning preserves every registered case exactly once."""

import unittest
import sys
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ci_partitions import PARTITIONS
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
        ):
            self.assertEqual([case.name for case in select_cases(layer=domain, partition=partition)], [name])

    def test_iceberg_gc_runs_separately_from_ordinary_and_sdk_cases(self):
        expected = select_cases(layer="iceberg", scope="iceberg.gc")
        self.assertTrue(expected)
        self.assertEqual(select_cases(layer="iceberg", partition="iceberg-gc"), expected)
        self.assertEqual(
            tuple(case for case in select_cases(layer="iceberg") if case.iceberg_gc),
            expected,
        )
        core = select_cases(layer="iceberg", partition="iceberg-core")
        self.assertTrue(core)
        self.assertTrue(all(not case.iceberg_gc for case in core))
        self.assertTrue(all(not case.scope.startswith("iceberg.sdk.") for case in core))

    def test_iceberg_sdk_selection_preserves_the_full_grace_owner(self):
        grace = select_cases(layer="iceberg", partition="iceberg-grace")
        self.assertEqual(len(grace), 1)
        expected = tuple(
            case for case in select_cases(layer="iceberg")
            if case.scope.startswith("iceberg.sdk.") and case not in grace
        )
        self.assertEqual(select_cases(layer="iceberg", partition="iceberg-sdk"), expected)
