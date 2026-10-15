# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Installed-wheel v2 acceptance against a real Dataset HTTP listener."""
import asyncio
import hashlib
import io
import os
import tempfile
from pathlib import Path
from crowdb_dataset import DatasetClient, AsyncDatasetClient, SplitSource, new_id, sample_id


def prepare(client, operation, revision):
    page = "0"
    for _ in range(100):
        preview = client.evaluate_composition(operation, revision, page)
        if preview["complete"]: break
        page = preview["next_page"]
    else: raise AssertionError("composition evaluation budget")
    for _ in range(100):
        draft = client.prepare_composition(operation, revision)
        if draft["uploads"]["publication_ready"]: return draft["preparation"]["snapshot"]
    raise AssertionError("composition preparation budget")


def cleanup(client, operation):
    for _ in range(100):
        if client.cleanup_composition(operation)["complete"]: return
    raise AssertionError("cleanup budget")


def publish(client, operation, snapshot):
    request = {"attempt": new_id(), "snapshot": snapshot, "expected_head": client.head()["generation"]}
    receipt = client.publish_composition(operation, request)
    assert client.publish_composition(operation, request) == receipt
    assert client.commit_status(request["attempt"]) == receipt
    return receipt


def upload_source(client, source, upload_id, length, md5, part_size=1024*1024):
    """Caller-owned source adapter: use the same bytes/identity on retry.

    Rust and the server implement part idempotence and integrity. This adapter only
    pulls bounded bytes from an external reader; it does not modify that source.
    """
    client.create_upload({"id": upload_id, "length": str(length), "part_size": part_size, "md5": md5})
    remaining, index = length, 0
    while remaining:
        chunk = source.read(min(part_size, remaining))
        if not chunk: raise ValueError("truncated source")
        # Sources may return short reads. Accumulate at most one bounded part.
        while len(chunk) < min(part_size, remaining):
            more = source.read(min(part_size, remaining) - len(chunk))
            if not more: raise ValueError("truncated source")
            chunk += more
        client.upload_part(upload_id, index, chunk)
        remaining -= len(chunk)
        index += 1
    if source.read(1): raise ValueError("source exceeds declared length")
    return client.complete_upload(upload_id)


async def verify_async(endpoint, name, token, snapshot, revision):
    async with AsyncDatasetClient(endpoint, name, token) as owner:
        client = owner.control()
        plan = {"id": new_id(), "snapshot": snapshot, "selection": {"kind": "all"},
                "fields": ["value"], "batch_size": 1,
                "partition": {"worker": 0, "workers": 1, "rank": 0, "world_size": 1},
                "shuffle": None, "split": {"revision": revision, "name": "train/a b"}, "expected_page": "0"}
        for _ in range(100):
            result = await client.prepare_read(plan)
            if result["complete"]: break
            plan["expected_page"] = result["next_page"]
        else: raise AssertionError("read preparation budget")
        before = await client.progress(plan["id"])
        chunks, header = [], None
        async with client.next_batch(plan["id"]) as stream:
            async for kind, value in stream:
                if kind == "data": chunks.append(value)
                if kind == "header": header = value
        assert b"".join(chunks) == b"external source"
        assert await client.progress(plan["id"]) == before
        await client.acknowledge(plan["id"], before["cursor"], header["next"])
        assert (await client.close_read(plan["id"]))["closed"]
        pages = [page async for page in client.pages("samples", snapshot=snapshot, limit=1)]
        assert sum(len(page["entries"]) for page in pages) == 1


def main():
    endpoint, name = os.environ["CROWDB_DATASET_ENDPOINT"], os.environ["CROWDB_DATASET_NAME"]
    token = os.environ.get("CROWDB_DATASET_READ_TOKEN")
    with DatasetClient(endpoint, name, token) as owner:
        client = owner.control()
        client.create()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.bin"
            path.write_bytes(b"local file")
            local = client.upload_file(path, new_id())
            assert local["length"] == "10" and path.read_bytes() == b"local file"
            client.abort_upload(local["id"])
        upload, data = new_id(), b"external source"
        for _ in range(2):
            upload_source(client, io.BytesIO(data), upload, len(data), hashlib.md5(data).hexdigest(), 4)
        operation, sample = new_id(), sample_id(b"\x00\xff\x80")
        split = {"name": "train/a b", "definition": {"kind": "selection", "predicate": {"kind": "all"}}, "origin": {"kind": "evaluated"}}
        client.begin_composition({"operation": operation, "parent": None, "source": {"kind": "all"},
            "schema": {"version": 1, "fields": [{"name": "value", "required": True}]},
            "inherit_parent_splits": False, "parent_split_revision": None, "splits": [split], "policy": None})
        client.edit_composition(operation, {"edit": new_id(), "expected_revision": "1", "schema": None,
            "samples": [{"kind": "upsert", "id": sample, "fields": [{"name": "value", "value": {"kind": "upload", "id": upload}}]}], "splits": None, "policy": None})
        snapshot = prepare(client, operation, "2")
        publish(client, operation, snapshot)
        cleanup(client, operation)
        head = client.split_head(snapshot)
        revision = head["revision"]
        assert client.matrix(snapshot, [sample], ["value"])["entries"][0]["fields"][0]["length"] == str(len(data))
        assert client.preview(snapshot, sample, "value")["data"] == sample_id(data)
        split_op = new_id()
        client.begin_split(snapshot, {"operation": split_op, "expected_revision": revision,
            "policy": {"overlap": "disjoint", "coverage": "allow_unassigned"}, "splits": []})
        page = "0"
        for _ in range(100):
            preview = client.evaluate_split(split_op, "1", page)
            if preview["complete"]: break
            page = preview["next_page"]
        else: raise AssertionError("Split evaluation budget")
        candidate = client.prepare_split(split_op, "1")
        client.publish_split(split_op, {"attempt": new_id(), "revision": candidate["revision"], "expected_head": head["generation"]})
        assert client.head()["latest"] == snapshot
        source = SplitSource(snapshot, revision, "train/a b")
        assert next(client.pages("split_members", source=source))["entries"] == [sample]
        child = new_id()
        client.begin_composition({"operation": child, "parent": snapshot,
            "source": {"kind": "split", "source": source.record(), "selection": None}, "schema": None,
            "inherit_parent_splits": True, "parent_split_revision": revision, "splits": [], "policy": None})
        child_snapshot = prepare(client, child, "1")
        publish(client, child, child_snapshot)
        cleanup(client, child)
        assert client.summary(child_snapshot)["parent"] == snapshot
    asyncio.run(verify_async(endpoint, name, token, snapshot, revision))
    print("installed Python Split SDK round trip passed", flush=True)


if __name__ == "__main__": main()
