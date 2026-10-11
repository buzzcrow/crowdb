# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import unittest
from scenarios import select_cases, case_nodes, case_resources


class OccupiedListenerBudgetTest(unittest.TestCase):
    def test_second_release_process_is_reserved_alongside_full_node_and_host_client(self):
        cases = select_cases(scope="deployment.listeners")
        self.assertEqual(len(cases), 2)
        for case in cases:
            self.assertEqual(case_nodes(case), 2)
            self.assertEqual(case_resources(case, 0.25), (3, 3072))
            self.assertEqual(case_resources(case, 3), (4, 3072))
