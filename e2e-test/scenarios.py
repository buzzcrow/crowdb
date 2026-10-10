# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Named flow scopes, fresh server profiles and bounded host-client dispatch."""

import concurrent.futures
import json
from dataclasses import dataclass

from fixture import Cluster


@dataclass(frozen=True)
class Case:
    scope: str
    name: str
    stores: tuple = (0,)
    wired: bool = False
    stores_per_node: tuple | None = None
    groups: tuple = (1,)
    restart: bool = False
    groups_per_node: tuple | None = None


KV_CASES = (
    Case("kv.recovery", "kv::snapshot::snapshot_import_precedes_wiring_and_catches_up_tail",
         (0, 1, 2), groups_per_node=((1,), (1,), ())),
    Case("kv.membership", "kv::reconfiguration::quorum_expands_and_shrinks_without_losing_writes",
         (0, 1, 2, 3, 4)),
    Case("kv.routing", "kv::cluster::topology_and_follower_hint", (10, 20, 30), True),
    Case("kv.routing", "kv::cluster::dynamic_groups_isolate_data", (0, 1, 2), True),
    Case("kv.management", "kv::management::management_lifecycle"),
    Case("kv.membership", "kv::membership::replica_lifecycle"),
    Case("kv.membership", "kv::membership::epoch_replay_and_authority_bootstrap"),
    Case("kv.management", "kv::progressive::topology_wiring_and_progressive_setup", (0, 0)),
    Case("kv.management", "kv::maintenance::synchronous_step_down_fence"),
    Case("kv.management", "kv::maintenance::flush_and_wipe_preserve_group_and_resume_writes"),
    Case("kv.management", "kv::operations::async_and_sync_leader_lifecycle", (0, 1, 2), True),
    Case("kv.routing", "kv::routing::multi_store_isolation_and_recovery", wired=True,
         stores_per_node=((1, 2),) * 3, groups=(10,), restart=True),
)


def run_cases(image, artifacts, bundle, slots, data_root=None, scope="all"):
    selected = tuple(case for case in KV_CASES if scope == "all" or case.scope == scope)
    if not selected:
        raise ValueError(f"no migrated cases for scope {scope}")

    def run(case):
        count = len(case.stores_per_node or case.stores)
        cluster = Cluster(image, artifacts, data_root=data_root, node_count=count,
                          store_ids=case.stores, wire_replicas=case.wired,
                          stores_per_node=case.stores_per_node, groups=case.groups,
                          groups_per_node=case.groups_per_node)
        (cluster.artifacts / "case.json").write_text(json.dumps({
            "scope": case.scope, "case": case.name}, indent=2))
        with cluster:
            if case.restart:
                cluster.client(bundle, "write", case.name)
                cluster.crash_restart()
                cluster.client(bundle, "verify", case.name)
            else:
                cluster.client(bundle, case.name.rsplit("::", 1)[-1], case.name)

    with concurrent.futures.ThreadPoolExecutor(max_workers=slots) as pool:
        for _ in pool.map(run, selected):
            pass
