# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
import json
import sys

container = json.load(sys.stdin)[0]
published = {port: bindings for port, bindings in container["NetworkSettings"]["Ports"].items() if bindings}
assert set(published) == {"9090/tcp", "9091/tcp", "9092/tcp"}, published
assert all(binding["HostIp"] == "127.0.0.1" for bindings in published.values() for binding in bindings), published
