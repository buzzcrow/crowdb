# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Strict local-HCA Dataset publication, restart, owner and transport checks."""
import gc
import json
import os
from pathlib import Path
import crowdb_dataset_native as native
from real_consumer import publish_native, verify_native


def main():
    root = Path(os.environ["CROWDB_DATASET_CONSUMER_ROOT"])
    transport = dict(enabled=True, allow_tcp_fallback=False, listen_enabled=False,
                     device=os.environ["CROWDB_RDMA_DEVICE"], source_address=os.environ["CROWDB_RDMA_SOURCE_ADDR"],
                     cq_count=1, cq_depth=512, sq_depth=16, rq_depth=16, posted_receive_target=16,
                     registered_memory_budget_bytes=512 << 20, aggregation_enabled=False,
                     wire_layout="control_data", wire_slot_capacity=(1 << 20) + 1024,
                     send_arena_bytes=16 << 20, receive_arena_bytes=16 * ((1 << 20) + 1024))
    runtime = native.NativeRuntime(os.environ["CROWDB_MANAGEMENT_SEEDS"].split(","),
                                   options=json.dumps(dict(data_rdma=transport)))
    phase = os.environ["CROWDB_DATASET_PHASE"]
    try:
        if phase == "write":
            publish_native(runtime, root)
        else:
            verify_native(runtime, root)
        client = runtime.dataset("native-python-write")
        snapshot = bytes(json.loads(root.joinpath("python-snapshot.json").read_text()))
        batch = client.read_batch(snapshot, [list(b"one")], ["value"])
        held = batch.next()
        assert held.registration_id != 0
        first = held.to_bytes()
        while batch.next() is not None:
            pass
        topology = json.loads(runtime.topology())
        status = topology["data"]
        assert status["rdma"]["ready"] and not status["allow_tcp_fallback"]
        for key in ("fallback_count", "protocol_errors", "cq_errors", "send_errors", "receive_post_errors"):
            assert status["rdma"][key] == 0, (key, status)
        resources = json.loads(runtime.status())
        assert resources["held_bytes"] >= held.allocation_capacity
        runtime.close()
        assert held.to_bytes() == first
        del held
        gc.collect()
        assert json.loads(runtime.status())["held_bytes"] == 0
        root.joinpath(f"dataset-hca-{phase}.json").write_text(json.dumps(dict(
            phase=phase, topology=topology, resources=resources, errors=0,
            scope="same-host HCA functional acceptance"), indent=2))
        print(f"Dataset HCA {phase}: registered receive ownership, strict transport and shutdown passed")
    finally:
        runtime.close()


if __name__ == "__main__":
    main()
