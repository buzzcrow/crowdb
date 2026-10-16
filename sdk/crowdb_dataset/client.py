# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Python ergonomics over the Rust SDK. No Python HTTP or cursor implementation."""
import asyncio
import base64
import json
import os
import uuid
from dataclasses import dataclass


def new_id():
    return uuid.uuid4().hex


def sample_id(value):
    return base64.b64encode(value).decode("ascii")


class DatasetError(RuntimeError):
    def __init__(self, record, identity=None):
        super().__init__(record.get("message", "Dataset operation failed"))
        self.code = record.get("code", "invalid")
        self.status = record.get("status", 0)
        self.retryable = record.get("retryable", False)
        self.identity = record.get("identity", identity)


class ConflictError(DatasetError):
    pass


class IntegrityError(DatasetError):
    pass


class CancelledError(DatasetError):
    pass


def _error(record, identity=None):
    kind = {"conflict": ConflictError, "integrity": IntegrityError,
            "cancelled": CancelledError}.get(record.get("code"), DatasetError)
    return kind(record, identity)


def _encode(value):
    budget = [8 * 1024 * 1024]
    def check(item, depth=0):
        budget[0] -= 1 + (len(item) if isinstance(item, str) else 0)
        if budget[0] < 0:
            raise ValueError("Dataset metadata exceeds 8 MiB")
        if depth > 32:
            raise ValueError("metadata nesting limit")
        if isinstance(item, str) and len(item) > 128 * 1024:
            raise ValueError("metadata string limit; use file or buffer upload APIs")
        if isinstance(item, dict):
            for key, child in item.items():
                check(key, depth + 1)
                check(child, depth + 1)
        elif isinstance(item, (list, tuple)):
            for child in item: check(child, depth + 1)
    check(value)
    chunks, size = [], 0
    for fragment in json.JSONEncoder(separators=(",", ":")).iterencode(value):
        size += len(fragment)
        if size > 8 * 1024 * 1024:
            raise ValueError("Dataset metadata exceeds 8 MiB")
        chunks.append(fragment)
    return "".join(chunks)


def _result(operation, identity=None):
    success, value = operation.result()
    record = json.loads(value)
    if not success:
        raise _error(record, identity)
    return record


@dataclass
class Batch:
    header: dict
    fields: list


class BatchStream:
    """Verified frames; batch success requires the end event and HTTP EOF.

    Retained Python bytes are caller-owned. Close cancels only this stream.
    """
    def __init__(self, native):
        self._native = native
        self.header = None
        self._end = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._native is None:
            raise StopIteration
        event = self._native.next()
        if event is None:
            if not self._end:
                raise IntegrityError({"code": "integrity", "message": "stream ended before verified completion"})
            self.close()
            raise StopIteration
        kind, value = event
        if kind == "error":
            self.close()
            raise _error(json.loads(value))
        if kind == "header":
            value = self.header = json.loads(value)
        if kind == "end":
            self._end = True
        return kind, value

    def close(self):
        if self._native is not None:
            self._native.close()
            self._native = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def batch(self, max_bytes=8 * 1024 * 1024):
        fields, chunks, size = [], [], 0
        for kind, value in self:
            if kind == "data":
                size += len(value)
                if size > max_bytes:
                    self.close()
                    raise DatasetError({"code": "limit", "message": "use streaming or download_to_path for this batch"})
                chunks.append(value)
            elif kind == "field_end":
                fields.append(b"".join(chunks))
                chunks = []
        return Batch(self.header, fields)


class DatasetClient:
    """Create inside each worker process. Clones of HTTP state are managed in Rust."""
    def __init__(self, endpoint, name, bearer=None, **options):
        from ._native import NativeClient
        if isinstance(options.get("ca_pem"), bytes):
            options["ca_pem"] = options["ca_pem"].decode("ascii")
        self._native = NativeClient(endpoint, name, bearer, json.dumps(options))
        self._pid = os.getpid()

    def _check(self):
        if os.getpid() != self._pid:
            raise RuntimeError("create a Dataset client inside each worker process")
        if self._native is None:
            raise RuntimeError("Dataset client is closed")

    def _start(self, name, arguments):
        self._check()
        return self._native.start(name, _encode(arguments))

    def _call(self, name, arguments):
        return _result(self._start(name, arguments), arguments.get("operation", arguments.get("id")))

    def create(self): return self._call("create", {})
    def control(self):
        from .control import ControlClient
        return ControlClient(self)
    def open(self): return self._call("open", {})
    def prepare(self, publication): return self._call("prepare", publication)
    def publish(self, publication): return self._call("publish", publication)
    def operation(self, id): return self._call("operation", {"id": id})
    def snapshot(self, id): return self._call("snapshot", {"id": id})
    def manifest(self, id): return self._call("manifest", {"id": id})
    def retain(self, id): return self._call("retain", {"id": id})
    def release(self, id): return self._call("release", {"id": id})
    def set_stable(self, id): return self._call("set_stable", {"id": id})
    def list_snapshots(self, after=None, limit=100): return self._call("list_snapshots", {"after": after, "limit": limit})
    def create_upload(self, request): return self._call("create_upload", request)
    def upload_part(self, id, index, data):
        self._check()
        return _result(self._native.part(id, index, data), id)

    def upload_status(self, id): return self._call("upload_status", {"id": id})
    def complete_upload(self, id): return self._call("complete_upload", {"id": id})
    def abort_upload(self, id): return self._call("abort_upload", {"id": id})
    def upload_file(self, path, id, *, part_size=1024 * 1024, concurrency=4):
        return self._call("upload_file", {"path": os.fspath(path), "id": id, "part_size": part_size, "concurrency": concurrency})
    def download_to_path(self, snapshot, sample, field, path):
        return self._call("download_to_path", {"snapshot": snapshot, "sample": sample, "field": field, "path": os.fspath(path)})
    def start_read(self, plan): return self._call("start_read", plan)
    def resume_read(self, id): return self._call("resume_read", {"id": id})
    def close_read(self, id): return self._call("close_read", {"id": id})
    def acknowledge(self, id, expected, next): return self._call("acknowledge", {"id": id, "ack": {"expected": expected, "next": next}})
    def progress(self, id): return self._call("progress", {"id": id})
    def reclaim(self, id): return self._call("reclaim", {"id": id})
    def reclaim_status(self, id): return self._call("reclaim_status", {"id": id})

    def stream_batch(self, request):
        self._check()
        return BatchStream(self._native.stream("batch", _encode(request)))

    def next_batch(self, id):
        self._check()
        return BatchStream(self._native.stream("next", id))

    def get_batch(self, request, max_bytes=8 * 1024 * 1024):
        with self.stream_batch(request) as stream:
            return stream.batch(max_bytes)

    def refresh_bearer(self, bearer):
        self._check()
        self._native.refresh_bearer(bearer)

    def close(self):
        if self._native is not None:
            self._native.close()
            self._native = None

    def __enter__(self): return self
    def __exit__(self, *_): self.close()


class AsyncBatchStream:
    def __init__(self, stream): self._stream = stream
    def __aiter__(self): return self
    async def __anext__(self):
        # StopIteration cannot cross an asyncio Future boundary.
        def fetch():
            try: return True, next(self._stream)
            except StopIteration: return False, None
        try:
            found, event = await asyncio.to_thread(fetch)
        except asyncio.CancelledError:
            if self._stream._native is not None:
                self._stream._native.cancel()
            raise
        if not found: raise StopAsyncIteration
        return event
    async def close(self):
        if self._stream._native is not None:
            self._stream._native.cancel()
        await asyncio.to_thread(self._stream.close)
    async def __aenter__(self): return self
    async def __aexit__(self, *_): await self.close()


class AsyncDatasetClient:
    """Asynchronous calls share the same Rust client and cancel their Rust task."""
    def __init__(self, endpoint, name, bearer=None, **options):
        self._client = DatasetClient(endpoint, name, bearer, **options)

    def control(self):
        from .control import AsyncControlClient
        return AsyncControlClient(self._client)

    def __getattr__(self, name):
        method = getattr(DatasetClient, name, None)
        if method is None or name.startswith("_") or name in ("close", "stream_batch", "next_batch", "get_batch"):
            raise AttributeError(name)
        async def call(*args, **kwargs):
            # Capture the operation instead of running a second transport or state machine.
            owner = self._client
            class Capture:
                def _call(self, operation, arguments): return operation, arguments
            operation, arguments = method(Capture(), *args, **kwargs)
            pending = owner._start(operation, arguments)
            identity = arguments.get("operation", arguments.get("id"))
            try:
                return await asyncio.to_thread(_result, pending, identity)
            except asyncio.CancelledError:
                pending.cancel()
                await asyncio.to_thread(pending.result)
                raise
        return call

    async def refresh_bearer(self, bearer): self._client.refresh_bearer(bearer)

    async def upload_part(self, id, index, data):
        self._client._check()
        pending = self._client._native.part(id, index, data)
        try: return await asyncio.to_thread(_result, pending, id)
        except asyncio.CancelledError:
            pending.cancel()
            await asyncio.to_thread(pending.result)
            raise

    def stream_batch(self, request): return AsyncBatchStream(self._client.stream_batch(request))
    def next_batch(self, id): return AsyncBatchStream(self._client.next_batch(id))
    async def get_batch(self, request, max_bytes=8 * 1024 * 1024):
        fields, chunks, size, header = [], [], 0, None
        async with self.stream_batch(request) as stream:
            async for kind, value in stream:
                if kind == "header": header = value
                elif kind == "data":
                    size += len(value)
                    if size > max_bytes:
                        raise DatasetError({"code": "limit", "message": "use streaming or download_to_path for this batch"})
                    chunks.append(value)
                elif kind == "field_end":
                    fields.append(b"".join(chunks))
                    chunks = []
        return Batch(header, fields)

    async def close(self): self._client.close()
    async def __aenter__(self): return self
    async def __aexit__(self, *_): await self.close()
