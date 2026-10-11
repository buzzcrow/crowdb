# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Fixture-local bounded lifecycle requests from host test clients."""

import json
import threading
import time


class Control:
    def __init__(self, cluster):
        self.cluster = cluster
        self.directory = cluster.artifacts / "control"
        self.directory.mkdir(exist_ok=True)
        self.done = threading.Event()
        self.worker = threading.Thread(target=self.serve, daemon=True)

    def serve(self):
        while not self.done.is_set():
            for path in sorted(self.directory.glob("*.request.json")):
                response = path.with_name(path.name.replace(".request.json", ".response.json"))
                if response.exists():
                    continue
                started = time.monotonic()
                try:
                    if path.stat().st_size > 4096:
                        raise ValueError("control request exceeds 4096 bytes")
                    body = json.loads(path.read_text())
                    result = {"ok": True, "result": self.cluster.control(body)}
                except Exception as error:
                    result = {"ok": False, "error": str(error)}
                result["seconds"] = time.monotonic() - started
                temporary = response.with_suffix(".tmp")
                temporary.write_text(json.dumps(result, indent=2))
                temporary.replace(response)
            self.done.wait(0.025)

    def __enter__(self):
        self.cluster.control_cancelled = self.done
        self.worker.start()
        return self

    def __exit__(self, _kind, _error, _traceback):
        self.done.set()
        self.worker.join(timeout=65)
        if self.worker.is_alive():
            raise RuntimeError("fixture control did not finish its bounded action")
