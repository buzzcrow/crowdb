#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Resolve host PIDs for Paxos-KV servers in an external Docker deployment."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess


def server_pid(process_table):
    matches = []
    for row in process_table.splitlines()[1:]:
        fields = row.split(None, 1)
        if len(fields) != 2:
            continue
        args = shlex.split(fields[1])
        if args and Path(args[0]).name == 'crowdb-paxos-kv-server':
            matches.append(int(fields[0]))
    if len(matches) != 1 or matches[0] <= 0:
        raise ValueError(f'expected exactly one live Paxos-KV server; found {matches}')
    return matches[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    state = json.loads(args.state.read_text())
    names = state['containers']
    if len(names) != 3 or len(set(names)) != 3:
        raise ValueError('expected three distinct external containers')
    records = []
    mapped_files = {}
    for name in names:
        found = json.loads(subprocess.check_output(['docker', 'inspect', name], text=True, timeout=60))[0]
        labels = found['Config'].get('Labels') or {}
        if not found['State']['Running'] or labels.get('crowdb.paxos-kv.deployment') != state['id']:
            raise ValueError(f'container not running or deployment identity differs: {name}')
        table = subprocess.check_output(['docker', 'top', name, '-eo', 'pid,args'], text=True, timeout=60)
        pid = server_pid(table)
        # Container executable paths do not exist in the host mount namespace.
        # Keep the matching binary and mapped libraries for stack symbolization.
        executable = next(shlex.split(row.split(None, 1)[1])[0]
                          for row in table.splitlines()[1:]
                          if row.split(None, 1)[0] == str(pid))
        symbols = args.output.parent / 'symbols'
        symbols.mkdir(exist_ok=True)
        binary = symbols / f'{name}-crowdb-paxos-kv-server'
        subprocess.run(['docker', 'cp', f'{name}:{executable}', str(binary)], check=True, timeout=60)
        for row in Path(f'/proc/{pid}/maps').read_text().splitlines():
            fields = row.split(None, 5)
            if len(fields) == 6 and 'x' in fields[1] and fields[5].startswith('/'):
                mapped_files.setdefault(fields[5], name)
        records.append({'name': name, 'pid': pid, 'host_uid': Path(f'/proc/{pid}').stat().st_uid,
                        'executable': executable, 'symbol_binary': str(binary),
                        'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                        'inspect': found, 'processes': table})
    if len({record['inspect']['Image'] for record in records}) != 1 or len({record['binary_sha256'] for record in records}) != 1:
        raise ValueError('combined server profile requires matching server images and binaries')
    # Some release binaries have symbols but no GNU build-id. A private symfs
    # also resolves those binaries and the exact container DSOs, without using
    # possibly different host libraries or modifying the user's perf cache.
    rootfs = args.output.parent / 'symbols' / 'rootfs'
    for path, name in mapped_files.items():
        if path.endswith(' (deleted)'):
            raise ValueError(f'cannot archive a replaced executable mapping: {path}')
        target = rootfs / path.lstrip('/')
        target.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(['docker', 'cp', '-L', f'{name}:{path}', str(target)], check=True, timeout=60)
    args.output.write_text(json.dumps(records, indent=2) + '\n')
    print(','.join(str(record['pid']) for record in records))


if __name__ == '__main__':
    main()
