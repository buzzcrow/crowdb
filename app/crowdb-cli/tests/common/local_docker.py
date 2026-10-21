#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Process-boundary Docker fixture; every mutation is persisted for assertions."""

import json
import os
from pathlib import Path
import sys

root = Path(os.environ["CROWDB_RUNTIME_ROOT"])
path = root / "docker.json"
state = json.loads(path.read_text())
args = sys.argv[1:]
state["calls"].append(args)
result = ""

def values(flag):
    return [args[index + 1] for index, arg in enumerate(args[:-1]) if arg == flag]

def one(flag):
    return values(flag)[0]

if args[:2] == ["context", "show"]:
    result = state["runtime"]["context"]
elif args[:2] == ["context", "inspect"]:
    result = [{"Endpoints": {"docker": {"Host": state["runtime"]["endpoint"]}}}]
elif args[0] == "info":
    result = state["runtime"]["daemon"]
elif args[:2] == ["image", "inspect"]:
    result = [{"Id": "sha256:" + "a" * 64, "Os": "linux", "Architecture": "amd64",
               "Config": {"Labels": {"org.opencontainers.image.source": "https://github.com/buzzcrow/crowdb"}}}]
elif args[:2] == ["ps", "-aq"]:
    result = "\n".join(state["container"])
elif len(args) > 1 and args[1] == "ls":
    result = "\n".join(state[args[0]])
elif len(args) > 1 and args[1] == "inspect":
    result = [state[args[0]][name] for name in args[2:]]
elif len(args) > 1 and args[1] == "create":
    name = args[-1]
    item = {"Id": name, "Name": name, "Labels": dict(value.split("=", 1) for value in values("--label"))}
    if args[0] == "network":
        item["Driver"] = "bridge"
    state[args[0]][name] = item
    result = name
elif args[0] == "run":
    name = one("--name")
    if state.get("fail_run_once") == name:
        del state["fail_run_once"]
        path.write_text(json.dumps(state))
        print("injected interrupted creation", file=sys.stderr)
        sys.exit(1)
    volume = dict(part.split("=", 1) for part in one("--mount").split(","))
    ports = {}
    for value in values("-p"):
        ip, host, container = value.split(":")
        ports[container + "/tcp"] = [{"HostIp": ip, "HostPort": host}]
    state["container"][name] = {
        "Id": name, "Name": "/" + name, "Image": args[-1], "State": {"Running": True},
        "Config": {"Labels": dict(value.split("=", 1) for value in values("--label")), "Env": values("-e")},
        "HostConfig": {"NanoCpus": round(float(one("--cpus")) * 1_000_000_000),
                       "Memory": int(one("--memory")[:-1]) * 1024 * 1024,
                       "NetworkMode": one("--network"), "PortBindings": ports},
        "Mounts": [{"Destination": volume["target"], "Type": "volume", "Name": volume["source"], "RW": True}]}
    result = name
elif args[0] in ("start", "stop"):
    state["container"][args[-1]]["State"]["Running"] = args[0] == "start"
    result = args[-1]
else:
    raise RuntimeError("unexpected Docker operation: " + repr(args))
path.write_text(json.dumps(state))
print(result if isinstance(result, str) else json.dumps(result))
