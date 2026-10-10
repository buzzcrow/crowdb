# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit lifecycle operations for launcher-owned nodes, preserving data."""

import argparse

from runtime import OWNER, Runtime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", choices=["docker", "containerd"], default="docker")
    parser.add_argument("--address", default="/run/containerd/containerd.sock")
    parser.add_argument("--namespace", default="crowdb")
    parser.add_argument("--snapshotter", choices=["overlayfs", "native"], default="overlayfs")
    parser.add_argument("operation", choices=["logs", "stop", "restart", "liveness", "readiness", "remove"])
    parser.add_argument("name")
    args = parser.parse_args()
    runtime = Runtime(args.runtime, args.address, args.namespace, args.snapshotter)
    if not runtime.inspect(args.name).get("Config", {}).get("Labels", {}).get(OWNER):
        parser.error("container is not owned by the CROWDB launcher")
    if args.operation in ("liveness", "readiness"):
        command = ["exec", args.name, "crowdb-monitor", args.operation]
    elif args.operation in ("stop", "restart"):
        command = [args.operation, "--time", "30", args.name]
    elif args.operation == "remove":
        command = ["rm", args.name]
    else:
        command = ["logs", args.name]
    print(runtime.invoke(*command), end="")


if __name__ == "__main__":
    main()
