#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Docker-only local cluster shortcuts, independent of crowdb-cli."""

import argparse
import sys

from docker_local_cleanup import selected, stop
from docker_local_launch import accepted, run
from docker_local_state import Docker, check_daemon, record_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("run", "stop"))
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--cluster")
    scope.add_argument("--all", action="store_true", help="explicitly select every recorded local cluster")
    parser.add_argument("--nodes", type=int, choices=(1, 3))
    parser.add_argument("--image")
    parser.add_argument("--port-base", type=int)
    parser.add_argument("--cpus", type=float)
    parser.add_argument("--memory-mib", type=int)
    args = parser.parse_args()
    if args.operation == "run" and args.all:
        parser.error("run selects one cluster and preserves its data")
    if args.operation != "run" and any(value is not None for value in
            (args.nodes, args.image, args.port_base, args.cpus, args.memory_mib)):
        parser.error("launch inputs apply only to run")
    args.cluster = args.cluster or "local"
    record_path(args.cluster)
    docker = Docker()
    if args.operation == "run":
        run(docker, accepted(docker, args))
        return
    records = selected(args.cluster, args.all)
    for record in records:
        check_daemon(docker, record)
    for record in records:
        stop(docker, record)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
