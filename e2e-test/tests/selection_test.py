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

    def test_manual_tpc_is_explicit_and_missing_client_fails_before_servers(self):
        required = json.loads(self.invoke("--list", "--layer", "iceberg"))
        self.assertTrue(all(case["scope"] != "iceberg.manual.tpc" for case in required))
        manual = json.loads(self.invoke("--list", "--scope", "iceberg.manual.tpc"))
        self.assertEqual([case["case"] for case in manual], ["access_iceberg::tpc::parallel_loader_stress"])
        with patch("run.Path.is_file", return_value=False), self.assertRaises(SystemExit):
            self.invoke("--scope", "iceberg.manual.tpc")

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

    def test_console_operations_combines_host_client_scopes(self):
        cases = json.loads(self.invoke("--list", "--scope", "console.operations"))
        self.assertEqual(len(cases), 92)
        self.assertTrue(all(case["scope"] != "console.managed" for case in cases))
        exact = json.loads(self.invoke("--list", "--scope", "console.operations", "--case", cases[0]["case"]))
        self.assertEqual(exact, [cases[0]])

    def test_client_and_metadata_scopes_expose_their_named_flows(self):
        for scope, count in (("kv.client", 6), ("kv.metadata", 14)):
            cases = json.loads(self.invoke("--list", "--scope", scope))
            self.assertEqual(len(cases), count)
            self.assertTrue(all(case["scope"] == scope for case in cases))

    def test_layers_select_only_their_domains_and_count_dependencies(self):
        diskdb = json.loads(self.invoke("--list", "--layer", "diskdb"))
        self.assertEqual(len(diskdb), 5)
        self.assertTrue(all(case["scope"].startswith("diskdb.") and case["nodes"] == 2
                            for case in diskdb))
        dataset = json.loads(self.invoke("--list", "--layer", "dataset"))
        self.assertEqual(len(dataset), 2)
        self.assertTrue(all(case["scope"] == "dataset.http" and case["nodes"] == 13 for case in dataset))
        console = json.loads(self.invoke("--list", "--layer", "console"))
        self.assertEqual(len(console), 92)
        wal_nodes = {"restart_3node_1group": 4, "restart_5node_1group": 6,
                     "restart_5node_2group": 6, "restart_6node_2group_overlap": 7}
        for case in console:
            if case["scope"] == "console.web.wal":
                self.assertEqual(case["nodes"], wal_nodes[case["case"].rsplit("::", 1)[-1]])
            else:
                self.assertEqual(case["nodes"], 4 if case["scope"] in ("console.web.lifecycle", "console.web.recovery")
                                 else 1 if case["scope"] in ("console.web.disposable", "console.web.standalone", "console.web.registry")
                                 else 2 if case["scope"] == "console.web.bootstrap"
                                 else 13 if case["scope"] == "console.web.access"
                                 else 5 if case["scope"].startswith("console.web.")
                                 else 1 if case["scope"] in ("console.clients",) else 3)
        iceberg = json.loads(self.invoke("--list", "--layer", "iceberg"))
        self.assertEqual(len(iceberg), 32)
        self.assertTrue(all(case["scope"].startswith("iceberg.") and case["nodes"] == 14
                            for case in iceberg))
        kv = json.loads(self.invoke("--list", "--layer", "kv"))
        self.assertTrue(all(case["scope"].startswith("kv.") for case in kv))
        with self.assertRaises(SystemExit):
            self.invoke("--layer", "kv", "--case", diskdb[0]["case"])
        with self.assertRaises(SystemExit):
            self.invoke("--layer", "diskdb", "--require-isolation")
