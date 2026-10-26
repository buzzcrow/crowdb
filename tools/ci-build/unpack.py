# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Restore a same-source compiled artifact after validating every member."""

import argparse
import json
import os
from pathlib import Path
import shutil
import tarfile
import tempfile

from compiled_artifacts import artifact_path, digest, validate_identity

ROOT = Path(__file__).resolve().parents[2]
MANIFESTS = {'clients': 'target/compiled-tests/manifest.json',
             'runtime': 'target/ci-build/runtime/manifest.json'}


def descriptor(archive):
    members = archive.getmembers()
    names = [member.name for member in members]
    if len(names) != len(set(names)):
        raise ValueError('compiled archive has duplicate members')
    manifest_names = set(MANIFESTS.values()) & set(names)
    if len(manifest_names) != 1:
        raise ValueError('compiled archive needs exactly one artifact manifest')
    name, = manifest_names
    member = archive.getmember(name)
    if not member.isfile() or member.size > 1024 * 1024:
        raise ValueError('compiled artifact manifest is not a bounded regular file')
    manifest = json.load(archive.extractfile(member))
    if MANIFESTS.get(manifest.get('kind')) != name:
        raise ValueError('compiled artifact kind does not match its manifest path')
    if set(names) != set(manifest['files']) | {name}:
        raise ValueError('compiled archive file membership differs from its manifest')
    if manifest['kind'] == 'runtime' and any(
            not filename.startswith('target/container-runtime/') for filename in manifest['files']):
        raise ValueError('runtime archive contains a file outside its staged runtime')
    return name, manifest, members


def install_file(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor_fd, temporary = tempfile.mkstemp(prefix='.compiled-', dir=destination.parent)
    os.close(descriptor_fd)
    try:
        shutil.copy2(source, temporary)
        os.replace(temporary, destination)
    finally:
        Path(temporary).unlink(missing_ok=True)


def install_runtime(staging, root):
    destination = artifact_path(root, 'target/container-runtime')
    previous = staging / 'previous-runtime'
    if destination.exists() or destination.is_symlink():
        destination.rename(previous)
    try:
        (staging / 'target/container-runtime').rename(destination)
    except BaseException:
        if previous.exists() or previous.is_symlink():
            previous.rename(destination)
        raise


def unpack(path, root=ROOT):
    root = Path(root).resolve()
    with tarfile.open(path) as archive:
        name, manifest, members = descriptor(archive)
        validate_identity(root, manifest)
        destinations = {member.name: artifact_path(root, member.name) for member in members}
        output = root / 'target/ci-build'
        output.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='unpack-', dir=output) as directory:
            staging = Path(directory)
            for member in members:
                if not member.isfile():
                    raise ValueError(f'compiled archive has a non-regular member: {member.name}')
                staged = staging / member.name
                staged.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, staged.open('wb') as destination:
                    shutil.copyfileobj(source, destination)
                if member.name != name and digest(staged) != manifest['files'][member.name]:
                    raise ValueError(f'compiled artifact digest mismatch: {member.name}')
                staged.chmod(member.mode & 0o777)
            if manifest['kind'] == 'runtime':
                for filename, expected in [('SOURCE_REVISION', manifest['revision']),
                                           ('SOURCE_TREE', manifest['source_tree'])]:
                    if (staging / 'target/container-runtime' / filename).read_text().strip() != expected:
                        raise ValueError(f'staged runtime {filename} differs from its manifest')
                install_runtime(staging, root)
            # No consumer file is changed until all archive hashes have passed.
            for relative, destination in destinations.items():
                if manifest['kind'] != 'runtime' or relative == name:
                    install_file(staging / relative, destination)
    print(f'Restored {manifest["kind"]}: {len(manifest["files"])} verified files', flush=True)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    unpack(parser.parse_args().archive)


if __name__ == '__main__':
    main()
