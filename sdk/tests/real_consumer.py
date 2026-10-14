# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Installed-wheel functional acceptance against an owned real Dataset listener."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
from crowdb_dataset import (AsyncDatasetClient, DatasetClient, DatasetError,
                            DatasetIterable, WorkerPartition, new_id, sample_id)

root = Path(os.environ["CROWDB_DATASET_CONSUMER_ROOT"])
root.mkdir(parents=True, exist_ok=True)
endpoint, name = os.environ["CROWDB_DATASET_ENDPOINT"], os.environ["CROWDB_DATASET_NAME"]
token = os.environ["CROWDB_DATASET_READ_TOKEN"]
state_file = root / "python-state.json"


def write():
    with DatasetClient(endpoint, name, token) as client:
        try: info = client.open()
        except DatasetError as error:
            if error.status != 404: raise
            info = client.create()
        path = root / "python-input.bin"
        block = b"python-payload!" * 65536
        with path.open("wb") as stream:
            for _ in range(10): stream.write(block)
        upload_id = new_id()
        upload = client.upload_file(path, upload_id)
        assert upload["state"] == "complete"
        publication = {"operation": new_id(), "parent": info["latest"], "manifest": {
            "schema": [{"name": "value", "required": True}, {"name": "label", "required": False}],
            "samples": [{"id": sample_id(f"python-{index}".encode()), "fields": {
                "value": {"kind": "payload", "upload": upload_id},
                "label": {"kind": "inline", "data": sample_id(bytes([index]))}}} for index in range(4)]}}
        prepared = client.prepare(publication)
        assert client.open()["latest"] == info["latest"]
        published = client.publish(publication)
        assert published["id"] == prepared["id"]
        state_file.write_text(json.dumps({"publication": publication, "snapshot": published["id"], "upload": upload_id}))
        print("installed Python SDK uploaded and published real Dataset bytes", flush=True)


async def verify_async(snapshot):
    async with AsyncDatasetClient(endpoint, name, token) as client:
        opened, record = await asyncio.gather(client.open(), client.snapshot(snapshot))
        assert opened["latest"] == record["id"] == snapshot
        count = 0
        async with client.stream_batch({"snapshot": snapshot, "sample_ids": [sample_id(b"python-0")], "fields": ["value"]}) as stream:
            async for kind, value in stream:
                if kind == "data": count += len(value)
        assert count == (root / "python-input.bin").stat().st_size


def verify():
    state = json.loads(state_file.read_text())
    snapshot = state["snapshot"]
    with DatasetClient(endpoint, name, token) as client:
        assert client.publish(state["publication"])["id"] == snapshot
        assert client.operation(state["publication"]["operation"])["id"] == snapshot
        assert client.upload_status(state["upload"])["state"] == "claimed"
        destination = root / "python-output.bin"
        client.download_to_path(snapshot, sample_id(b"python-0"), "value", destination)
        with destination.open("rb") as stream: actual = hashlib.file_digest(stream, "md5").digest()
        with (root / "python-input.bin").open("rb") as stream: expected = hashlib.file_digest(stream, "md5").digest()
        assert actual == expected
        owned = []
        for worker in range(2):
            plan = {"id": new_id(), "snapshot": "latest", "selection": {"kind": "prefix", "value": sample_id(b"python-")}, "fields": ["label"], "batch_size": 1, "shuffle": None}
            for batch in DatasetIterable(client, plan, partition=WorkerPartition(worker=worker, workers=2)):
                owned.extend(batch.header["sample_ids"])
                assert batch.fields[0] in (b"\0", b"\1", b"\2", b"\3")
                batch.acknowledge()
            client.close_read(plan["id"])
        assert len(owned) == len(set(owned)) == 4
        client.retain(snapshot)
        client.release(snapshot)
        client.set_stable(snapshot)
        assert client.list_snapshots(limit=1)["snapshots"][0]["id"] == snapshot
    asyncio.run(verify_async(snapshot))
    print("installed Python sync/async SDK verified real Dataset bytes after restart", flush=True)


if __name__ == "__main__":
    write() if os.environ["CROWDB_DATASET_PHASE"] == "write" else verify()
