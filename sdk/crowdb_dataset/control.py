# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Control v2 ergonomics; Rust owns validation, HTTP, retries and bounded work."""
import asyncio
import os
from dataclasses import dataclass
from .client import BatchStream, AsyncBatchStream, _result


def decimal(value):
    """Encode an exact u64; do not accept floats or booleans."""
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= (1 << 64) - 1:
        raise ValueError("expected an unsigned 64-bit integer")
    return str(value)


@dataclass(frozen=True)
class SplitSource:
    snapshot: str
    revision: str
    name: str

    def record(self):
        return {"snapshot": self.snapshot, "revision": self.revision, "name": self.name}


def _identity(arguments):
    request = arguments.get("request") or {}
    return request.get("attempt", request.get("edit", request.get("operation", request.get("id", arguments.get("id")))))


class ControlClient:
    """Shares its owner's runtime, authentication and budgets; closing the owner closes access.

    Request dictionaries follow the strict v2 schema. Integer counts/revisions/seeds
    use decimal strings; Sample IDs use canonical base64. Retain retry IDs yourself.
    """
    def __init__(self, owner): self._owner = owner

    def _call(self, name, arguments):
        return _result(self._owner._start("v2." + name, arguments), _identity(arguments))

    def capabilities(self): return self._call("capabilities", {})
    def create(self): return self._call("create", {})
    def open(self): return self._call("open", {})
    def head(self): return self._call("head", {})
    def commit_status(self, attempt): return self._call("commit_status", {"id": attempt})
    def summary(self, snapshot): return self._call("summary", {"id": snapshot})
    def retain(self, snapshot): return self._call("retain", {"id": snapshot})
    def release(self, snapshot): return self._call("release", {"id": snapshot})
    def set_stable(self, snapshot): return self._call("set_stable", {"id": snapshot})
    def lifecycle(self, snapshot): return self._call("lifecycle", {"id": snapshot})
    def reclaim(self, snapshot): return self._call("reclaim", {"id": snapshot})
    def prepare_inspection(self, snapshot): return self._call("prepare_inspection", {"id": snapshot})
    def inspection_status(self, snapshot): return self._call("inspection_status", {"id": snapshot})
    def contains(self, snapshot, sample): return self._call("contains", {"id": snapshot, "sample": sample})
    def matrix(self, snapshot, sample_ids, fields):
        return self._call("matrix", {"id": snapshot, "request": {"sample_ids": sample_ids, "fields": fields}})
    def preview(self, snapshot, sample, field, max_bytes=65536):
        return self._call("preview", {"id": snapshot, "sample": sample, "field": field, "max_bytes": max_bytes})

    def begin_composition(self, request): return self._call("begin_composition", {"request": request})
    def composition(self, operation): return self._call("composition", {"id": operation})
    def edit_composition(self, operation, request): return self._call("edit_composition", {"id": operation, "request": request})
    def evaluate_composition(self, operation, expected_revision, expected_page):
        return self._call("evaluate_composition", {"id": operation, "request": {"expected_revision": expected_revision, "expected_page": expected_page}})
    def prepare_composition(self, operation, expected_revision):
        return self._call("prepare_composition", {"id": operation, "request": {"expected_revision": expected_revision}})
    def publish_composition(self, operation, request): return self._call("publish_composition", {"id": operation, "request": request})
    def abandon_composition(self, operation, expected_revision):
        return self._call("abandon_composition", {"id": operation, "request": {"expected_revision": expected_revision}})
    def cleanup_composition(self, operation): return self._call("cleanup_composition", {"id": operation})

    def begin_split(self, snapshot, request): return self._call("begin_split", {"id": snapshot, "request": request})
    def split_draft(self, operation): return self._call("split_draft", {"id": operation})
    def edit_split(self, operation, request): return self._call("edit_split", {"id": operation, "request": request})
    def evaluate_split(self, operation, expected_revision, expected_page):
        return self._call("evaluate_split", {"id": operation, "request": {"expected_revision": expected_revision, "expected_page": expected_page}})
    def prepare_split(self, operation, expected_revision):
        return self._call("prepare_split", {"id": operation, "request": {"expected_revision": expected_revision}})
    def publish_split(self, operation, request): return self._call("publish_split", {"id": operation, "request": request})
    def abandon_split(self, operation, expected_revision):
        return self._call("abandon_split", {"id": operation, "request": {"expected_revision": expected_revision}})
    def cleanup_split(self, operation): return self._call("cleanup_split", {"id": operation})
    def split_head(self, snapshot): return self._call("split_head", {"id": snapshot})
    def split_revision(self, snapshot, revision): return self._call("split_revision", {"id": snapshot, "revision": revision})

    def create_upload(self, request): return self._call("create_upload", {"request": request})
    def upload_status(self, id): return self._call("upload_status", {"id": id})
    def complete_upload(self, id): return self._call("complete_upload", {"id": id})
    def abort_upload(self, id): return self._call("abort_upload", {"id": id})
    def upload_file(self, path, id): return self._call("upload_file", {"path": os.fspath(path), "id": id})
    def upload_part(self, id, index, data):
        self._owner._check()
        return _result(self._owner._native.part_v2(id, index, data), id)
    def bind_read(self, request): return self._call("bind_read", {"request": request})
    def next_batch(self, cursor):
        self._owner._check()
        return BatchStream(self._owner._native.stream("v2-next", cursor))

    def page(self, kind, *, snapshot=None, source=None, after=None, limit=20):
        """One bounded page; source is a SplitSource or its exact wire dictionary."""
        if kind not in ("datasets", "inventory", "roots", "children", "schema", "samples", "split_history", "split_members"):
            raise ValueError("unknown page kind")
        if isinstance(source, SplitSource): source = source.record()
        return self._call("page_" + kind, {"id": snapshot, "request": source, "after": after, "limit": limit})

    def pages(self, kind, **scope):
        """Lazy page iterator; Rust validates continuation and response bounds."""
        while True:
            page = self.page(kind, **scope)
            yield page
            if page["after"] is None: return
            scope["after"] = page["after"]


class AsyncControlClient:
    """Same Rust operations with cancellation propagated to their native tasks."""
    def __init__(self, owner): self._owner = owner

    def __getattr__(self, name):
        method = getattr(ControlClient, name, None)
        if method is None or name.startswith("_") or name in ("pages", "next_batch", "upload_part"):
            raise AttributeError(name)
        async def call(*args, **kwargs):
            class Capture:
                def _call(self, operation, arguments): return operation, arguments
            operation, arguments = method(Capture(), *args, **kwargs)
            pending = self._owner._start("v2." + operation, arguments)
            try: return await asyncio.to_thread(_result, pending, _identity(arguments))
            except asyncio.CancelledError:
                pending.cancel()
                await asyncio.to_thread(pending.result)
                raise
        return call

    async def upload_part(self, id, index, data):
        self._owner._check()
        pending = self._owner._native.part_v2(id, index, data)
        try: return await asyncio.to_thread(_result, pending, id)
        except asyncio.CancelledError:
            pending.cancel()
            await asyncio.to_thread(pending.result)
            raise

    def next_batch(self, cursor): return AsyncBatchStream(ControlClient(self._owner).next_batch(cursor))

    async def pages(self, kind, **scope):
        while True:
            page = await self.page(kind, **scope)
            yield page
            if page["after"] is None: return
            scope["after"] = page["after"]
