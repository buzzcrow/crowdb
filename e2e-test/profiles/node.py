# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Host deployment client requests retain the original admission bounds."""

import json
import os
import subprocess
import time
import urllib.error
import urllib.request

IMAGE = os.environ.get("CROWDB_CONTAINER_IMAGE", "crowdb-node:dev")
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def docker(*args):
    result = subprocess.run(["docker", *args], text=True, capture_output=True, timeout=60)
    if result.returncode:
        displayed = list(args)
        if "--json" in displayed:
            displayed[displayed.index("--json") + 1] = "<node-control request>"
        raise RuntimeError(f"docker {' '.join(displayed)}\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


def request(origin, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    try:
        reply = HTTP.open(urllib.request.Request(origin + path, data=data, headers={"Content-Type": "application/json"}), timeout=45)
    except urllib.error.HTTPError as error:
        error.add_note("Server response: " + error.read().decode())
        raise
    return json.load(reply)


def wait(action, predicate, description, budget=10):
    deadline = time.monotonic() + budget
    last = None
    while time.monotonic() < deadline:
        try:
            last = action()
            if predicate(last):
                return last
        except (OSError, urllib.error.HTTPError) as error:
            last = str(error)
        time.sleep(0.1)
    raise AssertionError(f"{description}: {last}")
