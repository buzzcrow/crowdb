# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Measure uncompressed image layers independently of daemon cache storage."""

import http.client
import json
import os
import socket
import subprocess
import sys
from urllib.parse import quote


class LocalDockerConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=5)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


endpoint = os.environ.get("DOCKER_HOST") or subprocess.check_output(
    ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"], text=True
).strip()
if not endpoint.startswith("unix://"):
    raise RuntimeError("Container acceptance requires a local Unix Docker endpoint")
connection = LocalDockerConnection(endpoint.removeprefix("unix://"))
try:
    connection.request("GET", f"/images/{quote(sys.argv[1], safe='')}/history")
    response = connection.getresponse()
    if response.status != 200:
        raise RuntimeError(f"Docker image history failed: {response.status} {response.read().decode()}")
    layers = json.load(response)
    print(sum(layer["Size"] for layer in layers))
finally:
    connection.close()
