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
    for command in [dict(op="begin_composition", operation=identity(), input={}),
                    dict(op="update_snapshot_head", snapshot=identity()),
                    dict(op="update_split_head", snapshot=identity(), revision=identity(), expected_head=None)]:
        try:
            client.call(json.dumps(command))
        except native.NativeError:
            pass
        else:
            raise AssertionError("removed workflow or missing explicit condition was accepted")
    upload, snapshot = identity(), identity()
    data = native_data()
    digest = list(hashlib.md5(data).digest())
    call(client, "create_upload", upload=upload, length=len(data), part_size=4 << 20,
                  md5=digest, expires_at=2**63)
    for index, offset in enumerate(range(0, len(data), 4 << 20)):
        client.upload_part(bytes(upload), index, data[offset:offset + (4 << 20)])
    call(client, "complete_upload", upload=upload)
    location = call(client, "upload_location", upload=upload)
    request = {"parent": None, "source": "All", "schema": {"version": 1,
               "fields": [{"name": "value", "required": True}]},
               "samples": [{"Upsert": {"sample_id": list(b"one"), "fields": [{"name": "value",
                   "value": {"Chunk": {"location": location, "length": len(data), "md5": digest}}}]}}],
               "splits": [{"name": "train", "definition": {"Selection": {"Explicit": [list(b"one")]}},
                           "origin": "Evaluated"}], "policy": {"overlap": "AllowOverlap", "coverage": "AllowUnassigned"}}
    prepared = call(client, "prepare_snapshot", snapshot=snapshot, input=request)
    assert prepared["snapshot"] == snapshot and prepared["sample_count"] == 1
    assert call(client, "prepare_snapshot", snapshot=snapshot, input=request) == prepared
    head = call(client, "update_snapshot_head", snapshot=snapshot, expected_head=None)
    assert head["latest"] == snapshot and head["stable"] is None
    check_head_conflicts(client, snapshot, request, head)
    split_binding = bind(client, bytes(snapshot).hex(), ids=[b"one"], split={"revision": None, "name": "train"})
    split_batch = client.next_batch(split_binding["cursor"])
    assert json.loads(split_batch.header)["sample_ids"] == [list(b"one")]
    assert drain(split_batch)[0] == data
    try:
        call(client, "abort_upload", upload=upload)
    except native.ConflictError:
        pass
    else:
        raise AssertionError("published payload lost its durable owner")
    root.joinpath("python-snapshot.json").write_text(json.dumps(snapshot))
    verify_native(runtime, root)


def check_head_conflicts(client, snapshot, request, head):
    # A losing complete result remains readable; only explicit new conditions retry publication.
    next_snapshot = identity()
    request = dict(request, parent=snapshot, source="All", schema=None, samples=[])
    call(client, "prepare_snapshot", snapshot=next_snapshot, input=request)
    assert call(client, "snapshot", snapshot=next_snapshot)
    assert drain(client.read_batch(bytes(next_snapshot), [list(b"one")], ["value"]))[0] == native_data()
    for _ in range(4):
        try:
            call(client, "update_snapshot_head", snapshot=next_snapshot, expected_head=None)
        except native.ConflictError as error:
            assert error.observed_head == head
            observed = error.observed_head
        else:
            raise AssertionError("stale explicit base unexpectedly published")
    head = call(client, "update_snapshot_head", snapshot=next_snapshot, expected_head=observed["generation"])
    assert head["latest"] == next_snapshot
    revision = identity()
    preparation = {"revision": revision, "policy": request["policy"], "splits": request["splits"]}
    prepared = call(client, "prepare_split_revision", snapshot=snapshot, input=preparation)
    assert call(client, "prepare_split_revision", snapshot=snapshot, input=preparation) == prepared
    try:
        call(client, "update_split_head", snapshot=snapshot, revision=revision,
             expected_revision=None, expected_head=None)
    except native.ConflictError as error:
        observed = error.observed_head
        assert observed["snapshot"] == snapshot and observed["revision"] is not None
    else:
        raise AssertionError("stale Split base unexpectedly published")
    updated = call(client, "update_split_head", snapshot=snapshot, revision=revision,
                   expected_revision=observed["revision"], expected_head=observed["generation"])
    assert updated["revision"] == revision


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
