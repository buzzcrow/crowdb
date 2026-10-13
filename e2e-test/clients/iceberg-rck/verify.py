# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Require fresh executed RCK results, including all six default selectors."""
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

source = Path(sys.argv[1])
results = source / "open-api/build/test-results/test"
suites = [ET.parse(path).getroot() for path in results.glob("TEST-*.xml")]
assert suites, "Gradle produced no RCK JUnit results"
tests = [test for suite in suites for test in suite.findall("testcase")]
assert tests, "Gradle executed no RCK tests"
assert all(not test.findall("failure") and not test.findall("error")
           and not test.findall("skipped") for test in tests), "RCK failures or skipped tests"
if not os.environ.get("CROWDB_ICEBERG_RCK_SELECTOR"):
    expected = {"testCreateNamespace()", "testBasicCreateTable()", "testRenameTable()",
                "testDropTable()", "testDropMissingTable()", "testListTables()"}
    assert len(tests) == 6 and {test.attrib["name"] for test in tests} == expected
print(f"Verified {len(tests)} executed Apache Iceberg RCK tests")
