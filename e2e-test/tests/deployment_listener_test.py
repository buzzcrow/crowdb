# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import unittest
from scenarios import select_cases, case_nodes, case_resources


class FixedListenerRecoveryBudgetTest(unittest.TestCase):
    def test_release_node_and_host_client_budget_is_preserved(self):
        cases = select_cases(scope="deployment.listeners")
        self.assertEqual(len(cases), 2)
        for case in cases:
            self.assertEqual(case_nodes(case), 2)
            self.assertEqual(case_resources(case, 0.25), (1.5, 3072))
            self.assertEqual(case_resources(case, 3), (1.5, 3072))
