# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""KV store/group readiness and explicit peer wiring on fixed node listeners."""


def ready_nodes(origins, stores_per_node, groups_per_node, request, wait):
    for origin, stores, groups in zip(origins, stores_per_node, groups_per_node, strict=True):
        def matches(topology, stores=stores, groups=groups):
            found = {store["store_id"]: store for store in topology.get("stores", [])}
            return set(found) == set(stores) and all(
                {group["group_id"] for group in store.get("groups", [])} == set(groups)
                for store in found.values())

        wait(lambda origin=origin: request(origin, "/topology"), matches,
             f"host-accessible KV store/group readiness at {origin}")


def wire_nodes(origins, addresses, stores_per_node, groups, request):
    combined = []
    for origin, address in zip(origins, addresses, strict=True):
        topology = request(origin, "/topology")
        for store in topology["stores"]:
            store["listen_addr"] = address + ":" + store["listen_addr"].rsplit(":", 1)[1]
            combined.append(store)
    same_layout = all(stores == stores_per_node[0] for stores in stores_per_node)
    if not same_layout and any(len(stores) != 1 for stores in stores_per_node):
        raise ValueError("mixed multi-store layouts require explicit test-controlled wiring")
    for origin, stores in zip(origins, stores_per_node, strict=True):
        for store_id in stores:
            peers = [store for store in combined if not same_layout or store["store_id"] == store_id]
            for group in groups:
                request(origin, f"/stores/{store_id}/groups/{group}/remotes/batch", {"stores": peers})
