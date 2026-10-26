# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Complete, disjoint CI partitions of the Console and Iceberg domains."""

PARTITIONS = {
    "console": ("console-api", "console-browser", "console-native",
                "console-transition", "console-inspection"),
    "iceberg": ("iceberg-core", "iceberg-gc", "iceberg-sdk", "iceberg-grace"),
}


def partition_for(case):
    if case.scope.startswith("console."):
        if case.name == "console::native_browser::production_split_and_transfer_journals":
            return "console-transition"
        if case.name == "console::native_browser::page_layout_and_iceberg_inspection":
            return "console-inspection"
        if case.scope.startswith("console.native."):
            return "console-native"
        if case.native_browser or case.shell_browser or case.console_browser:
            return "console-browser"
        return "console-api"
    if case.scope.startswith("iceberg."):
        if case.name == "access_iceberg::rust_retired::official_rust_client_rejects_retired_native_catalog_after_full_grace":
            return "iceberg-grace"
        if case.scope.startswith("iceberg.sdk."):
            return "iceberg-sdk"
        if case.scope == "iceberg.gc" or case.scope.startswith("iceberg.gc."):
            return "iceberg-gc"
        return "iceberg-core"
    return None
