#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Package the actual instrumented server and libraries into a diagnostic image."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid


def run(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT).strip()


def stage(binary, destination):
    symbols = run('readelf', '--dyn-syms', '--wide', str(binary))
    if '__asan_init' not in symbols:
        raise ValueError('server does not link ASan; build with CROWDB_ASAN=1')
    (destination / 'lib').mkdir(parents=True)
    shutil.copy2(binary, destination / 'server')
    dependencies = run('ldd', str(binary))
    (destination / 'dependencies.txt').write_text(dependencies + '\n')
    if 'not found' in dependencies:
        raise ValueError(dependencies)
    # Package non-system runtime dependencies, including the exact ASan runtime.
    roots = [Path(os.environ['CONDA_PREFIX']).resolve(), binary.parent.resolve()]
    for line in dependencies.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[1] == '=>' and parts[2].startswith('/'):
            path = Path(parts[2]).resolve()
            if any(path.is_relative_to(root) for root in roots):
                shutil.copy2(path, destination / 'lib' / parts[0])
    asan = [p for p in (destination / 'lib').iterdir() if p.name.startswith('libasan.so')]
    if len(asan) != 1:
        raise ValueError('expected one linked ASan library')
    return asan[0].name


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--base-image', required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    asan = stage(args.binary.resolve(), args.output)
    base_metadata = run('docker', 'image', 'inspect', args.base_image)
    (args.output / 'base-image.json').write_text(base_metadata + '\n')
    base = json.loads(base_metadata)[0]['Id']
    # BuildKit interprets a bare sha256 ID as a registry repository. A unique
    # local tag pins this build's base without racing a user-managed tag.
    local_base = 'crowdb-sanitizer-base:' + uuid.uuid4().hex
    (args.output / 'Dockerfile').write_text(f'''FROM {local_base}
COPY server /opt/crowdb/asan/bin/crowdb-paxos-kv-server
COPY lib /opt/crowdb/asan/lib
RUN mkdir -p /opt/crowdb/data/asan
LABEL crowdb.sanitizer="address"
ENV LD_LIBRARY_PATH="/opt/crowdb/asan/lib:/opt/crowdb/lib" \\
    LD_PRELOAD="/opt/crowdb/asan/lib/{asan}" \\
    ASAN_OPTIONS="detect_leaks=1:abort_on_error=0:log_path=/opt/crowdb/data/asan/report"
ENTRYPOINT ["/opt/crowdb/asan/bin/crowdb-paxos-kv-server"]
''')
    run('docker', 'tag', base, local_base)
    try:
        subprocess.run(['docker', 'build', '--iidfile', str(args.output / 'image-id'), str(args.output)], check=True)
    finally:
        run('docker', 'image', 'rm', local_base)


if __name__ == '__main__':
    main()
