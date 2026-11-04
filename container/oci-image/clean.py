#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Clean stopped clusters and unused Docker resources; preserve running clusters."""

import argparse
from pathlib import Path
import shutil
import sys

from docker_local_cleanup import clean, image_garbage, selected
from docker_garbage import collect
from docker_local_state import Docker, RECORDS, check_daemon, load
from host_cleanup import check_data_users, clean as clean_host, selected_records
from host_data_cleanup import HostDataCleanup
from host_runtime import HostRuntime, require_owned


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument('--cluster', help='stop and clean only this recorded cluster')
    scope.add_argument('--all', action='store_true',
                       help='stop and clean every recorded cluster, then clean unused Docker resources')
    data = parser.add_mutually_exclusive_group()
    data.add_argument('--keep-data', action='store_true', help='preserve data and restart receipts')
    data.add_argument('--delete-data', action='store_true',
                      help='delete stopped-cluster data and unused Docker volumes (default)')
    parser.add_argument('--runtime-root', type=Path, default=RECORDS.parents[1])
    args = parser.parse_args(argv)
    stop_running = args.cluster is not None or args.all
    directory = args.runtime_root / 'persistent/oci-clusters'
    if args.cluster is None:
        records = selected(None, True, directory)
    else:
        record = load(args.cluster, directory)
        records = [record] if record else []
    hosts = selected_records(args.cluster, args.runtime_root)
    if args.cluster is not None and not records and not hosts:
        raise ValueError(f"cluster '{args.cluster}' has no ownership receipt; "
                         'use --all for recorded clusters or Docker/Compose tools for direct containers')
    use_docker = records or (not hosts and shutil.which('docker'))
    docker = Docker() if use_docker else None
    if docker is not None:
        for record in records:
            check_daemon(docker, record)
    runtime_identity = docker.identity() if docker is not None else None
    retained = []
    if not stop_running and docker is not None:
        stopped = []
        for record in records:
            if any(item['State']['Running'] for item in docker.owned(record, 'container')):
                print(f"Skipped running cluster {record['cluster']}")
                retained.append(record)
            else:
                stopped.append(record)
        records = stopped
    plans = []
    stopped_hosts = []
    retained_hosts = []
    for record in hosts:
        runtime = HostRuntime(record['options'], record['cluster'])
        runtime.check(record)
        current = runtime.named(record['name'])
        if current is not None:
            require_owned(current, record)
            if not stop_running and current['State']['Running']:
                print(f"Skipped running host cluster {record['cluster']}")
                retained_hosts.append(record)
                continue
        if not args.keep_data:
            check_data_users(runtime, record)
        plans.append(None if args.keep_data else HostDataCleanup(record, args.runtime_root))
        stopped_hosts.append(record)
    hosts = stopped_hosts
    for record in records:
        clean(docker, record, not args.keep_data, directory, stop_running=stop_running)
    for record, plan in zip(hosts, plans):
        clean_host(record, stop_running=stop_running)
        if plan is not None:
            runtime = HostRuntime(record['options'], record['cluster'])
            runtime.check(record)
            check_data_users(runtime, record)
            plan.remove()
        print(f"Cleaned host container for {record['cluster']}; "
              + ('preserved data and restart receipts' if args.keep_data
                 else 'deleted node data and receipts; preserved supplied disk files and devices'))
    if not records and not hosts and not retained and not retained_hosts:
        print('No recorded CROWDB clusters to clean')
    if docker is not None:
        if args.cluster is None:
            collect(docker, retained + (records if args.keep_data else []),
                    retained_hosts + (hosts if args.keep_data else []),
                    delete_data=not args.keep_data, runtime=runtime_identity)
        image_garbage(docker, directory, runtime=runtime_identity)
    else:
        print('No local Docker runtime selected; skipped dangling Docker image cleanup')


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
