# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Installed native binding acceptance over the HTTP workload's exact snapshot."""
import gc
import base64
import hashlib
import json
import os
from pathlib import Path
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
from urllib.request import Request, urlopen

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


def read_spec(snapshot, worker=0, workers=1, ids=None, split=None):
    return {"snapshot": snapshot,
            "selection": {"Explicit": [list(item) for item in ids] if ids is not None else
                          [list(f"size-{i}".encode()) for i in range(8)]},
            "fields": ["value"], "batch_size": 2,
            "partition": {"worker": worker, "workers": workers, "rank": 0, "world_size": 1},
            "shuffle": None, "split": split}


def bind(client, snapshot, worker=0, workers=1, ids=None, split=None):
    return call(client, "bind_read", spec=read_spec(snapshot, worker, workers, ids, split))


def http_read(name, suffix, body):
    endpoint = os.environ["CROWDB_DATASET_ENDPOINT"].rstrip("/")
    url = f"{endpoint}/api/dataset/v2/namespaces/dataset-ns/datasets/{quote(name, safe='')}/reads/{suffix}"
    headers = {"Content-Type": "application/json",
               "Authorization": "Bearer " + os.environ["CROWDB_DATASET_READ_TOKEN"]}
    with urlopen(Request(url, json.dumps(body).encode(), headers=headers), timeout=60) as response:
        data = response.read(16 * 1024 * 1024 + 1)
    assert len(data) <= 16 * 1024 * 1024, "semantic fixture response exceeded its bound"
    return data


def http_binding(name, spec):
    plan = dict(spec, selection={"kind": "explicit", "ids": [
        base64.b64encode(bytes(item)).decode() for item in spec["selection"]["Explicit"]]})
    if plan["split"] is not None:
        revision = plan["split"]["revision"]
        plan["split"] = dict(plan["split"], revision=bytes(revision).hex() if revision is not None else None)
    return json.loads(http_read(name, "bind", plan))


def http_batch(name, cursor):
    data = http_read(name, "next", {"cursor": cursor})
    assert data[:8] == b"CDBDS001"
    offset, header, fields, values, index = 8, None, [], {}, 0
    while offset < len(data):
        assert offset + 5 <= len(data), "truncated frame prefix"
        kind, length = data[offset], int.from_bytes(data[offset + 1:offset + 5], "big")
        offset += 5
        assert offset + length <= len(data), "truncated frame body"
        body = data[offset:offset + length]
        offset += length
        if kind == 1:
            assert header is None and 0 < length <= 8 * 1024 * 1024
            header = json.loads(body)
            fields = [i for i, field in enumerate(header["fields"]) if field["state"] == "value"]
        elif kind == 2:
            assert header is not None and index < len(fields) and 16 < length <= (4 << 20) + 16
            assert hashlib.md5(body[16:]).digest() == body[:16]
            values.setdefault(fields[index], bytearray()).extend(body[16:])
        elif kind == 3:
            assert length == 0 and header is not None and index < len(fields)
            field = header["fields"][fields[index]]
            value = values.get(fields[index], b"")
            assert len(value) == field["length"] and hashlib.md5(value).hexdigest() == field["md5"]
            index += 1
        elif kind == 0:
            assert length == 0 and header is not None and index == len(fields) and offset == len(data)
            return header, values
        else:
            raise AssertionError(f"unexpected Dataset frame {kind}: {body[:256]!r}")
    raise AssertionError("Dataset stream omitted verified EOF")


def http_header(header):
    return dict(header, snapshot=bytes(header["snapshot"]).hex(), sample_ids=[
        base64.b64encode(bytes(item)).decode() for item in header["sample_ids"]], fields=[dict(
            sample=base64.b64encode(bytes(field["sample_id"])).decode(), field=field["name"],
            state=field["state"], length=field["length"],
            md5=bytes(field["md5"]).hex() if field["md5"] is not None else None)
        for field in header["fields"]])


def continuation_parity(client, snapshot):
    selections = []
    other = native.NativeRuntime(os.environ["CROWDB_MANAGEMENT_SEEDS"].split(","))
    try:
        independent = other.dataset(os.environ["CROWDB_DATASET_NAME"])
        return compare_queries(client, independent, snapshot, selections)
    finally:
        other.close()


def compare_queries(client, independent, snapshot, selections):
    name = os.environ["CROWDB_DATASET_NAME"]
    for worker in range(2):
        cursor = bind(client, snapshot, worker, 2)["cursor"]
        assert http_binding(name, read_spec(snapshot, worker, 2))["cursor"] == cursor
        batch = client.next_batch(cursor, timeout=60)
        header = json.loads(batch.header)
        first = drain(batch)
        replay = client.next_batch(cursor, timeout=60)
        assert json.loads(replay.header) == header
        assert drain(replay) == first
        replay = independent.next_batch(cursor, timeout=60)
        assert json.loads(replay.header) == header and drain(replay) == first
        selected = []
        while True:
            assert http_batch(name, cursor) == (http_header(header), first)
            selected.extend(bytes(item) for item in header["sample_ids"])
            if header["end"]:
                break
            cursor = header["next"]
            batch = client.next_batch(cursor, timeout=60)
            header = json.loads(batch.header)
            first = drain(batch)
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
    deletion = check_head_conflicts(client, snapshot, request, head)
    root.joinpath("python-split-deletion.json").write_text(json.dumps(deletion))
    split_binding = bind(client, bytes(snapshot).hex(), ids=[b"one"], split={"revision": None, "name": "train"})
    split_batch = client.next_batch(split_binding["cursor"])
    header = json.loads(split_batch.header)
    values = drain(split_batch)
    assert header["sample_ids"] == [list(b"one")] and values[0] == data
    if "CROWDB_DATASET_ENDPOINT" in os.environ:
        spec = read_spec(bytes(snapshot).hex(), ids=[b"one"], split={"revision": None, "name": "train"})
        remote = http_binding("native-python-write", spec)
        assert remote["cursor"] == split_binding["cursor"]
        assert remote["split"]["revision"] == bytes(split_binding["split"]["revision"]).hex()
        assert http_batch("native-python-write", remote["cursor"]) == (http_header(header), values)
    root.joinpath("python-continuation.json").write_text(json.dumps(dict(
        cursor=split_binding["cursor"], header=header,
        fields={str(field): hashlib.sha256(value).hexdigest() for field, value in values.items()})))
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
    unused = identity()
    call(client, "prepare_split_revision", snapshot=snapshot,
         input=dict(preparation, revision=unused))
    deletion = call(client, "delete_split_revision", snapshot=snapshot, revision=unused)
    assert deletion["snapshot"] == bytes(snapshot).hex()
    assert deletion["revision"] == bytes(unused).hex()
    assert not deletion["complete"] and int(deletion["reclaim_after"]) > 0
    assert call(client, "delete_split_revision", snapshot=snapshot, revision=unused) == deletion
    return deletion


def verify_native(runtime, root):
    client = runtime.dataset("native-python-write")
    snapshot = bytes(json.loads(root.joinpath("python-snapshot.json").read_text()))
    batch = client.read_batch(snapshot, [list(b"one")], ["value"])
    assert drain(batch)[0] == native_data()
    saved = json.loads(root.joinpath("python-continuation.json").read_text())
    batch = client.next_batch(saved["cursor"])
    assert json.loads(batch.header) == saved["header"]
    values = drain(batch)
    assert {str(field): hashlib.sha256(value).hexdigest() for field, value in values.items()} == saved["fields"]
    deletion = json.loads(root.joinpath("python-split-deletion.json").read_text())
    observed = call(client, "delete_split_revision",
                    snapshot=list(bytes.fromhex(deletion["snapshot"])),
                    revision=list(bytes.fromhex(deletion["revision"])))
    assert observed == deletion or (observed["complete"] and observed["reclaim_after"] is None
                                    and observed["snapshot"] == deletion["snapshot"]
                                    and observed["revision"] == deletion["revision"])


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
