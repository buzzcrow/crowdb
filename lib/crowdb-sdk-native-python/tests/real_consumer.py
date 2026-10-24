# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Installed native binding acceptance over the HTTP workload's exact snapshot."""
import gc
import hashlib
import json
import os
from pathlib import Path
import uuid
import time
from concurrent.futures import ThreadPoolExecutor

import crowdb_dataset_native as native

SIZES = (0, 128, 513, 4096, 65535, 262144, 1048576, 4194321)


def call(client, op, **kwargs):
    return json.loads(client.call(json.dumps(dict(op=op, **kwargs)), timeout=60))


def drain(batch):
    values = {}
    while (lease := batch.next()) is not None:
        values.setdefault(lease.field, bytearray()).extend(lease.to_bytes())
    return values


def identity():
    return list(uuid.uuid4().bytes)


def native_data():
    return (b"native-producer" * 300000)[:(4 << 20) + 17]


def bind(client, snapshot, worker=0, workers=1, ids=None, split=None):
    spec = {"snapshot": snapshot,
            "selection": {"Explicit": [list(item) for item in ids] if ids is not None else
                          [list(f"size-{i}".encode()) for i in range(8)]},
            "fields": ["value"], "batch_size": 2,
            "partition": {"worker": worker, "workers": workers, "rank": 0, "world_size": 1},
            "shuffle": None, "split": split}
    return call(client, "bind_read", spec=spec)


def continuation_parity(client, snapshot):
    selections = []
    for worker in range(2):
        cursor = bind(client, snapshot, worker, 2)["cursor"]
        batch = client.next_batch(cursor, timeout=60)
        header = json.loads(batch.header)
        first = drain(batch)
        replay = client.next_batch(cursor, timeout=60)
        assert json.loads(replay.header) == header
        assert drain(replay) == first
        selected = []
        while True:
            selected.extend(bytes(item) for item in header["sample_ids"])
            if header["end"]:
                break
            cursor = header["next"]
            batch = client.next_batch(cursor, timeout=60)
            header = json.loads(batch.header)
            drain(batch)
        selections.append(set(selected))
    assert not selections[0].intersection(selections[1])
    assert selections[0] | selections[1] == {f"size-{i}".encode() for i in range(8)}


def pressure_and_idle_replay(client, snapshot):
    cursor = bind(client, snapshot)["cursor"]
    batch = client.next_batch(cursor, timeout=120)
    print("Holding a native batch for 65 seconds; reads have no session TTL", flush=True)
    time.sleep(65)
    first = drain(batch)
    assert drain(client.next_batch(cursor, timeout=120)) == first
    runtime = native.NativeRuntime(os.environ["CROWDB_MANAGEMENT_SEEDS"].split(","),
                                   options=json.dumps(dict(held_bytes=128)))
    try:
        bounded = runtime.dataset(os.environ["CROWDB_DATASET_NAME"])
        first = bounded.read_batch(bytes.fromhex(snapshot), [list(b"size-1")], ["value"])
        held = first.next()
        pending = bounded.read_batch(bytes.fromhex(snapshot), [list(b"size-1")], ["value"])
        with ThreadPoolExecutor(max_workers=1) as worker:
            waiting = worker.submit(pending.next)
            time.sleep(0.1)
            assert not waiting.done(), "held capacity must stop production"
            pending.cancel()
            try:
                waiting.result(timeout=5)
            except native.CancelledError:
                pass
            else:
                raise AssertionError("cancel did not interrupt a capacity wait")
        assert json.loads(runtime.status())["held_bytes"] == 128
        runtime.close()
        assert held.to_bytes() == bytes([1]) * 128
    finally:
        runtime.close()


def publish_native(runtime, root):
    client = runtime.dataset("native-python-write")
    call(client, "create")
    upload, operation = identity(), identity()
    data = native_data()
    digest = list(hashlib.md5(data).digest())
    call(client, "create_upload", upload=upload, length=len(data), part_size=4 << 20,
                  md5=digest, expires_at=2**63)
    for index, offset in enumerate(range(0, len(data), 4 << 20)):
        client.upload_part(bytes(upload), index, data[offset:offset + (4 << 20)])
    call(client, "complete_upload", upload=upload)
    location = call(client, "upload_location", upload=upload)
    begin = {"parent": None, "source": "All", "schema": {"version": 1,
             "fields": [{"name": "value", "required": True}]},
             "inherit_parent_splits": False, "parent_split_revision": None,
             "splits": [{"name": "train", "definition": {"Selection": {"Explicit": [list(b"one")]}},
                         "origin": "Evaluated"}], "policy": None}
    call(client, "begin_composition", operation=operation, input=begin)
    edit = {"id": identity(), "expected_revision": 1, "schema": None, "splits": None, "policy": None,
            "samples": [{"Upsert": {"sample_id": list(b"one"), "fields": [{"name": "value",
                "value": {"Chunk": {"location": location, "length": len(data), "md5": digest}}}]}}]}
    call(client, "edit_composition", operation=operation, edit=edit)
    for page in range(32):
        if call(client, "evaluate_composition", operation=operation, revision=2, page=page)["complete"]:
            break
    else:
        raise AssertionError("composition evaluation did not complete")
    for _ in range(32):
        state = call(client, "prepare_composition", operation=operation, revision=2)
        if state["uploads"]["publication_ready"]:
            break
    else:
        raise AssertionError("composition ownership did not complete")
    snapshot = state["uploads"]["candidate"]
    call(client, "publish_composition", operation=operation, attempt={"version": 1, "id": identity(),
         "target": {"Snapshot": {"snapshot": snapshot}}, "expected_head": None})
    split_binding = bind(client, bytes(snapshot).hex(), ids=[b"one"], split={"revision": None, "name": "train"})
    split_batch = client.next_batch(split_binding["cursor"])
    assert json.loads(split_batch.header)["sample_ids"] == [list(b"one")]
    assert drain(split_batch)[0] == data
    for _ in range(32):
        if call(client, "cleanup_composition", operation=operation)["complete"]:
            break
    else:
        raise AssertionError("composition cleanup did not complete")
    try:
        call(client, "abort_upload", upload=upload)
    except native.ConflictError:
        pass
    else:
        raise AssertionError("published payload lost its durable owner")
    root.joinpath("python-snapshot.json").write_text(json.dumps(snapshot))
    verify_native(runtime, root)


def verify_native(runtime, root):
    client = runtime.dataset("native-python-write")
    snapshot = bytes(json.loads(root.joinpath("python-snapshot.json").read_text()))
    batch = client.read_batch(snapshot, [list(b"one")], ["value"])
    assert drain(batch)[0] == native_data()


def main():
    root = Path(os.environ["CROWDB_DATASET_CONSUMER_ROOT"])
    runtime = native.NativeRuntime(os.environ["CROWDB_MANAGEMENT_SEEDS"].split(","))
    try:
        if os.environ.get("CROWDB_DATASET_PHASE") == "verify":
            verify_native(runtime, root)
            print("native Python payload survives storage restart")
            return
        client = runtime.dataset(os.environ["CROWDB_DATASET_NAME"])
        snapshot = root.joinpath("snapshot").read_text()
        for i, size in enumerate(SIZES):
            batch = client.read_batch(bytes.fromhex(snapshot), [list(f"size-{i}".encode())], ["value"])
            assert drain(batch).get(0, b"") == bytes([i]) * size
        continuation_parity(client, snapshot)
        pressure_and_idle_replay(client, snapshot)
        publish_native(runtime, root)
        batch = client.read_batch(bytes.fromhex(snapshot), [list(b"size-1")], ["value"])
        held = batch.next()
        view = held.slice(3, 11)
        batch.cancel()
        try:
            batch.next()
        except native.CancelledError:
            pass
        else:
            raise AssertionError("cancelled producer continued")
        client.close()
        assert call(runtime.dataset(os.environ["CROWDB_DATASET_NAME"]), "open")
        runtime.close()
        assert held.to_bytes() == bytes([1]) * 128 and view.to_bytes() == bytes([1]) * 8
        assert json.loads(runtime.status())["held_bytes"] >= 128
        del held, view, batch
        gc.collect()
        assert json.loads(runtime.status())["held_bytes"] == 0
        print("native Python same-snapshot bytes, workers, continuation replay, publication, cancellation and lifetime passed")
    finally:
        runtime.close()


if __name__ == "__main__":
    main()
