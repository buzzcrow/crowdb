# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import contextlib
import io
import json
import sys
import unittest
from unittest.mock import patch

import run


class SelectionTest(unittest.TestCase):
    def invoke(self, *arguments):
        output = io.StringIO()
        with patch.object(sys, "argv", ["run.py", *arguments]), \
                patch.object(run, "docker") as docker, contextlib.redirect_stdout(output), \
                contextlib.redirect_stderr(output):
            try:
                run.main()
            finally:
                docker.assert_not_called()
        return output.getvalue()

    def test_list_selects_exact_scope_and_case_without_runtime_prerequisites(self):
        cases = json.loads(self.invoke("--list", "--scope", "kv.group0"))
        self.assertEqual(len(cases), 8)
        self.assertTrue(all(case["scope"] == "kv.group0" for case in cases))
        selected = json.loads(self.invoke("--list", "--case", cases[0]["case"]))
        self.assertEqual(selected, [cases[0]])

    def test_missing_case_wrong_scope_and_single_case_isolation_fail_before_allocation(self):
        case = "kv::bootstrap::custom_replica"
        for arguments in (("--case", "missing"), ("--scope", "kv.routing", "--case", case),
                          ("--require-isolation", "--case", case),
                          ("--require-isolation", "--scope", "kv.client"),
                          ("--require-isolation", "--scope", "kv.metadata")):
            with self.assertRaises(SystemExit) as raised:
                self.invoke(*arguments)
            self.assertEqual(raised.exception.code, 2)

    def test_client_and_metadata_scopes_expose_their_named_flows(self):
        for scope, count in (("kv.client", 6), ("kv.metadata", 14)):
            cases = json.loads(self.invoke("--list", "--scope", scope))
            self.assertEqual(len(cases), count)
            self.assertTrue(all(case["scope"] == scope for case in cases))

    def test_layers_select_only_their_domains_and_count_dependencies(self):
        diskdb = json.loads(self.invoke("--list", "--layer", "diskdb"))
        self.assertEqual(len(diskdb), 3)
        self.assertTrue(all(case["scope"].startswith("diskdb.") and case["nodes"] == 2
                            for case in diskdb))
        kv = json.loads(self.invoke("--list", "--layer", "kv"))
        self.assertTrue(all(case["scope"].startswith("kv.") for case in kv))
        with self.assertRaises(SystemExit):
            self.invoke("--layer", "kv", "--case", diskdb[0]["case"])
        with self.assertRaises(SystemExit):
            self.invoke("--layer", "diskdb", "--require-isolation")
