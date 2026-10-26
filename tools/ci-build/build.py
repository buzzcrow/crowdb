# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Export production staging or compile shared release container-test clients."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

from compiled_artifacts import digest, identity, source_tree

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'target/ci-build'


def run(command, **kwargs):
    print('+ ' + ' '.join(map(str, command)), flush=True)
    subprocess.run(command, cwd=kwargs.pop('cwd', ROOT), check=True, **kwargs)


def relative_path(path):
    path = Path(path)
    if path.is_relative_to(ROOT):
        return str(path.relative_to(ROOT))
    for relative in ('sdk/target', 'target'):
        base = (ROOT / relative).resolve()
        if path.is_relative_to(base):
            return str(Path(relative) / path.relative_to(base))
    raise ValueError(f'build output is outside the checkout target trees: {path}')


def cargo(command, variant, manifest=None, target=None):
    metadata_command = ['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1']
    if manifest:
        metadata_command += ['--manifest-path', str(manifest)]
        command = [*command, '--manifest-path', str(manifest)]
    if target:
        command = [*command, '--target-dir', str(target)]
    packages = {entry['id']: entry for entry in json.loads(
        subprocess.check_output(metadata_command, cwd=ROOT, text=True))['packages']}
    descriptor, log = tempfile.mkstemp(prefix=f'{variant}-', suffix='.jsonl', dir=OUTPUT)
    print('+ ' + ' '.join(map(str, command)) + f' (metadata log: {log})', flush=True)
    entries = []
    with os.fdopen(descriptor, 'w') as stream, subprocess.Popen(
            [*command, '--message-format=json'], cwd=ROOT, stdout=subprocess.PIPE,
            text=True) as process:
        try:
            for line in process.stdout:
                stream.write(line)
                message = json.loads(line)
                if message.get('reason') == 'compiler-message':
                    print(message['message'].get('rendered', ''), end='', flush=True)
                if message.get('reason') != 'compiler-artifact' or not message.get('executable'):
                    continue
                package = packages.get(message['package_id'])
                if package is not None:
                    entries.append(dict(package=package['name'], name=message['target']['name'],
                        test=message['profile']['test'], variant=variant,
                        path=relative_path(message['executable'])))
        except BaseException:
            process.terminate()
            raise
        if process.wait():
            raise subprocess.CalledProcessError(process.returncode, command)
    return entries


def package(kind, targets, files, build_identity, **attributes):
    files = sorted(set(files) | {entry['path'] for entry in targets})
    manifest = dict(build_identity, targets=targets,
                    files={path: digest(ROOT / path) for path in files}, **attributes)
    descriptor = OUTPUT / kind / 'manifest.json'
    descriptor.parent.mkdir(exist_ok=True)
    descriptor.write_text(json.dumps(manifest, indent=2) + '\n')
    manifest_path = ('target/compiled-tests/manifest.json' if kind == 'clients'
                     else 'target/ci-build/runtime/manifest.json')
    temporary = OUTPUT / f'{kind}.tar.gz.partial'
    with tarfile.open(temporary, 'w:gz', compresslevel=1, dereference=True) as archive:
        archive.add(descriptor, arcname=manifest_path)
        for path in files:
            archive.add(ROOT / path, arcname=path, recursive=False)
    if source_tree(ROOT) != build_identity['source_tree']:
        raise RuntimeError('source tree changed during the build; rebuild the compiled artifact')
    temporary.replace(OUTPUT / f'{kind}.tar.gz')
    print(f'Compiled artifact {kind}: {len(targets)} targets, {len(files)} files', flush=True)


def runtime():
    build_identity = identity(ROOT, 'runtime')
    staging = ROOT / 'target/container-runtime'
    for name, expected in [('SOURCE_REVISION', build_identity['revision']),
                           ('SOURCE_TREE', build_identity['source_tree'])]:
        if (staging / name).read_text().strip() != expected:
            raise ValueError(f'staged runtime {name} differs from the checkout; rerun stage.sh')
    files = [str(path.relative_to(ROOT)) for path in staging.rglob('*') if path.is_file()]
    if not files:
        raise ValueError('staged runtime is empty')
    package('runtime', [], files, build_identity)


def sdk_distribution():
    destination = OUTPUT / 'sdk-distribution'
    destination.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='sdk-source-', dir=OUTPUT) as directory:
        source = Path(directory)
        run(['maturin', 'sdist', '--manifest-path',
             str(ROOT / 'sdk/crates/crowdb-sdk-python/Cargo.toml'), '--out', str(source)])
        archive, = source.glob('*.tar.gz')
        with tarfile.open(archive) as stream:
            stream.extractall(source / 'unpacked', filter='data')
        project, = (source / 'unpacked').iterdir()
        # Build the shipped source archive, preserving the independent SDK workspace.
        run(['maturin', 'build', '--release', '--locked', '--interpreter', sys.executable,
             '--out', str(destination)], cwd=project,
            env=dict(os.environ, CARGO_TARGET_DIR=str(ROOT / 'sdk/target/sdist')))
        shipped = destination / archive.name
        shipped.write_bytes(archive.read_bytes())
    wheel, = destination.glob('*.whl')
    return dict(wheel=str(wheel.relative_to(ROOT)), sdist=str(shipped.relative_to(ROOT)))


def clients():
    build_identity = identity(ROOT, 'clients')
    # Integration targets also build the normal CLI binary used by CARGO_BIN_EXE.
    # Select both packages together so their shared feature graph compiles once.
    targets = cargo(['cargo', 'test', '--release', '--locked', '-p', 'e2e-test',
                     '-p', 'crowdb-cli', '--test', 'domain_test', '--test',
                     'cluster_workloads_test', '--no-run'], 'root')
    targets += cargo(['cargo', 'build', '--release', '--locked', '-p', 'crowdb-sdk-dataset',
                      '--example', 'payload', '--example', 'split_control'],
                     'sdk', ROOT / 'sdk/Cargo.toml')
    targets += cargo(['cargo', 'build', '--release', '--locked'], 'apache',
                     ROOT / 'e2e-test/clients/iceberg-rust/Cargo.toml', ROOT / 'target/iceberg-sdk')
    targets = list({(entry['package'], entry['variant'], entry['name'], entry['test'], entry['path']): entry
                    for entry in targets}.values())
    required = [('e2e-test', 'domain_test', True, 'root'),
                ('crowdb-cli', 'cluster_workloads_test', True, 'root'),
                ('crowdb-cli', 'crowdb-cli', False, 'root'),
                ('crowdb-sdk-dataset', 'payload', False, 'sdk'),
                ('crowdb-sdk-dataset', 'split_control', False, 'sdk'),
                ('crowdb-iceberg-rust-client-fixture', 'crowdb-iceberg-rust-client-fixture',
                 False, 'apache')]
    selected = []
    for package_name, name, test, variant in required:
        matches = [entry for entry in targets if entry['package'] == package_name
                   and entry['name'] == name and entry['test'] == test and entry['variant'] == variant]
        if len(matches) != 1:
            raise ValueError(f'compiled client missing or ambiguous: {package_name}::{name}')
        selected.extend(matches)
    distribution = sdk_distribution()
    files = list(distribution.values())
    package('clients', selected, files, build_identity, sdk_distribution=distribution)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=('runtime', 'clients'))
    arguments = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    {'runtime': runtime, 'clients': clients}[arguments.kind]()


if __name__ == '__main__':
    main()
