# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Native write and read invariants behind the public SDK flow."""

from fixture import HTTP


def metrics(cluster):
    with HTTP.open(f"http://{cluster.addresses[12]}:7500/_crowdb/metrics", timeout=5) as response:
        exported = response.read().decode()
    values = {}
    for line in exported.splitlines():
        if line and not line.startswith("#"):
            name, value = line.rsplit(" ", 1)
            values[name] = value
    return values, exported


def verify_payload_copies(cluster, copying=False):
    values, exported = metrics(cluster)
    count = int(values["crowdb_s3_large_write_payload_copy_operations_total"])
    if (copying and count <= 0) or (not copying and count != 0):
        raise AssertionError("large payload copy count differs from the selected HTTP operation")
    (cluster.artifacts / "payload-copy-metrics.txt").write_text(exported)


def verify_native_metrics(cluster, copy_baseline=0):
    values, exported = metrics(cluster)
    def metric(name):
        key = "crowdb_s3_" + name
        if key not in values:
            raise AssertionError(f"missing native metric {key}")
        return int(values[key])
    if metric("native_direct_bytes_total") + metric("native_prefix_copy_bytes_total") <= 0:
        raise AssertionError("SDK did not exercise native body storage")
    for name in ("large_write_framed_owners_total", "large_write_framed_views_total",
                 "small_write_completed_total", "chunk_read_location_normalizations_total",
                 "chunk_read_range_locations_examined_total", "chunk_read_stream_windows_total",
                 "chunk_read_layout_queries_total", "chunk_read_layout_query_wait_ns_total",
                 "chunk_read_strip_read_wait_ns_total", "chunk_range_read_wait_ns_total",
                 "chunk_read_frame_decode_wait_ns_total"):
        if metric(name) <= 0:
            raise AssertionError(f"SDK did not exercise native {name}")
    if metric("large_write_payload_copy_operations_total") != copy_baseline:
        raise AssertionError("HTTP upload added a large writer payload copy")
    (cluster.artifacts / "native-metrics.txt").write_text(exported)
