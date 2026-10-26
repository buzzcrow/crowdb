# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Complete, disjoint CI partitions of the larger container domains."""

PARTITIONS = {
    "console": ("console-api", "console-browser", "console-native-data",
                "console-native-operations",
                "console-transition", "console-inspection"),
    "iceberg": ("iceberg-core", "iceberg-gc-convergence", "iceberg-gc-cleanup",
                "iceberg-sdk", "iceberg-grace"),
    "deployment": ("deployment-reference", "deployment-services"),
}

# Dataset and physical topology flows share one runner; service ownership,
# journals and recovery share the other. Keep membership semantic and stable.
NATIVE_DATA_SCOPES = frozenset({
    "console.native.browser.dataset",
    "console.native.browser.six-node",
    "console.native.browser.topology",
    "console.native.browser.retry",
    "console.native.geometry",
    "console.native.browser.hardware",
    "console.native.browser.maintenance",
    "console.native.browser.disk",
})


def partition_for(case):
    if case.scope.startswith("console."):
        if case.name == "console::native_browser::production_split_and_transfer_journals":
            return "console-transition"
        if case.name == "console::native_browser::page_layout_and_iceberg_inspection":
            return "console-inspection"
        if case.scope.startswith("console.native."):
            return ("console-native-data" if case.scope in NATIVE_DATA_SCOPES
                    else "console-native-operations")
        if case.native_browser or case.shell_browser or case.console_browser:
            return "console-browser"
        return "console-api"
    if case.scope.startswith("iceberg."):
        if case.name == "access_iceberg::rust_retired::official_rust_client_rejects_retired_native_catalog_after_full_grace":
            return "iceberg-grace"
        if case.scope.startswith("iceberg.sdk."):
            return "iceberg-sdk"
        if case.scope == "iceberg.gc" or case.scope.startswith("iceberg.gc."):
            if case.name == "access_iceberg::gc::convergence::expired_inline_backlog_completes_with_foreground_progress":
                return "iceberg-gc-convergence"
            return "iceberg-gc-cleanup"
        return "iceberg-core"
    if case.scope == "deployment" or case.scope.startswith("deployment."):
        return ("deployment-reference" if case.scope == "deployment.reference"
                else "deployment-services")
    return None
