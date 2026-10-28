#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Clean recorded Docker and containerd clusters and their data by default."""

import argparse
from pathlib import Path
import shutil
import sys

from docker_local_cleanup import clean, image_garbage, selected
from docker_local_state import Docker, RECORDS, check_daemon, load
from host_cleanup import check_data_users, clean as clean_host, selected_records
from host_data_cleanup import HostDataCleanup
from host_runtime import HostRuntime, require_owned


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument('--cluster', help='clean only this recorded cluster')
    scope.add_argument('--all', action='store_true', help='clean every recorded cluster (default)')
    data = parser.add_mutually_exclusive_group()
    data.add_argument('--keep-data', action='store_true', help='preserve data and restart receipts')
    data.add_argument('--delete-data', action='store_true', help='delete owned data (default)')
    parser.add_argument('--runtime-root', type=Path, default=RECORDS.parents[1])
    args = parser.parse_args(argv)
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
    plans = []
    for record in hosts:
        runtime = HostRuntime(record['options'], record['cluster'])
        runtime.check(record)
        current = runtime.named(record['name'])
        if current is not None:
            require_owned(current, record)
        if not args.keep_data:
            check_data_users(runtime, record)
        plans.append(None if args.keep_data else HostDataCleanup(record, args.runtime_root))
    for record in records:
        clean(docker, record, not args.keep_data, directory)
    for record, plan in zip(hosts, plans):
        clean_host(record)
        if plan is not None:
            runtime = HostRuntime(record['options'], record['cluster'])
            runtime.check(record)
            check_data_users(runtime, record)
            plan.remove()
        print(f"Cleaned host container for {record['cluster']}; "
              + ('preserved data and restart receipts' if args.keep_data
                 else 'deleted node data and receipts; preserved supplied disk files and devices'))
    if not records and not hosts:
        print('No recorded CROWDB clusters to clean')
    if docker is not None:
        image_garbage(docker, directory, runtime=records[0]['runtime'] if records else docker.identity())
    else:
        print('No local Docker runtime selected; skipped dangling Docker image cleanup')


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
