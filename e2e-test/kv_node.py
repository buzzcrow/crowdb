# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Fixed-port standalone KV release-node launch configuration."""


def arguments(cluster, index, address):
    groups, stores = cluster.groups_per_node[index], cluster.stores_per_node[index]
    group_args = ("--groups", ",".join(map(str, groups))) if groups else ()
    store_args = ("--stores", ",".join(map(str, stores))) if stores else ()
    advertised = ("--management-addr", address, "--management-advertise-addr", address) if address else ()
    return ("server", "kv", "--root", "/opt/crowdb/data/kv", "--management-port", "7000",
            "--ports", "7001..7011", *store_args, *group_args, "--replica", str(index + 1),
            "--kv-backend", "block", "--wal-backend", "block-device", "--election-profile", "e2e",
            "--log-dir", "/opt/crowdb/data/log/kv", "--log", *advertised, *cluster.node_args[index])
