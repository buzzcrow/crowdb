# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Exact host-client witnesses survive another domain's crash and retirement."""

import concurrent.futures
import json
import threading

from console_standalone_fixture import StandaloneWebCluster
from fixture import Cluster

WEB_CASE = "console::web_isolation::survives_peer_fixture_retirement"


def run_mixed(image, artifacts, bundle, data_root=None, node_cpus=0.5):
    ready = threading.Barrier(2, timeout=180)
    retired, failed = threading.Event(), threading.Event()

    def scenario(web):
        try:
            cls = StandaloneWebCluster if web else Cluster
            cluster = cls(image, artifacts, data_root=data_root)
            cluster.node_cpus = node_cpus
            case = WEB_CASE if web else "e2e_three_node_cluster_kv_put_batch_delete"
            (cluster.artifacts / "case.json").write_text(json.dumps({
                "scope": "console.isolation" if web else "kv.isolation", "case": case}, indent=2))
            with cluster:
                cluster.client(bundle, "write", case)
                ready.wait()
                if web:
                    if not retired.wait(180) or failed.is_set():
                        raise AssertionError("KV peer failed before cross-domain survivor check")
                    cluster.ready()
                    cluster.client(bundle, "verify", case)
                else:
                    cluster.crash_restart()
                    cluster.client(bundle, "verify", case)
                    cluster.diagnostics()
                    cluster.close()
                    retired.set()
        except BaseException:
            failed.set()
            retired.set()
            ready.abort()
            raise

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(scenario, web) for web in (False, True)]
        for future in futures:
            future.result()
