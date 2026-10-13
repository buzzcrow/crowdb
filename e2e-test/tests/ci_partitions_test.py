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
