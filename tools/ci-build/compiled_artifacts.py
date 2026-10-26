# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Validate the release host clients exported by the client build job."""

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def source_tree(root):
    return subprocess.check_output(
        [sys.executable, str(Path(root) / 'container/oci-image/source_tree.py'), str(root)],
        cwd=root, text=True).strip()


def identity(root, kind):
    root = Path(root).resolve()
    return dict(schema_version=1, kind=kind, profile='release', build_root=str(root),
                revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root,
                                                 text=True).strip(),
                source_tree=source_tree(root), lock_digest=digest(root / 'pixi.lock'))


def validate_identity(root, manifest):
    actual = identity(root, manifest['kind'])
    for field in ('schema_version', 'kind', 'profile', 'revision', 'source_tree', 'lock_digest'):
        if manifest.get(field) != actual[field]:
            raise ValueError(f'compiled artifact {field} differs from the checkout')
    if manifest['kind'] not in {'runtime', 'clients'}:
        raise ValueError('unrecognized compiled artifact kind')
    if manifest['kind'] == 'clients' and manifest.get('build_root') != actual['build_root']:
        raise ValueError('compiled clients require the same checkout path for embedded fixtures')


def artifact_path(root, relative):
    """Allow configured target symlinks while confining files to their target tree."""
    original = str(relative)
    relative = PurePosixPath(relative)
    if (relative.is_absolute() or '..' in relative.parts or str(relative) in {'.', ''}
            or str(relative) != original):
        raise ValueError(f'unsafe compiled artifact path: {relative}')
    if relative.parts[0] == 'target':
        base = Path(root) / 'target'
    elif relative.parts[:2] == ('sdk', 'target'):
        base = Path(root) / 'sdk/target'
    else:
        raise ValueError(f'compiled artifact is outside an owned target tree: {relative}')
    destination = Path(root) / relative
    if not destination.resolve().is_relative_to(base.resolve()):
        raise ValueError(f'compiled artifact escapes its target tree: {relative}')
    return destination


class CompiledArtifacts:
    def __init__(self, root, directory):
        self.root = Path(root).resolve()
        self.directory = Path(directory).resolve()
        self.manifest = json.loads((self.directory / 'manifest.json').read_text())
        validate_identity(self.root, self.manifest)
        if self.manifest['kind'] != 'clients':
            raise ValueError('host clients require a clients artifact')
        self.verified = set()

    @classmethod
    def from_environment(cls, root):
        directory = os.environ.get('CROWDB_COMPILED_TESTS')
        return cls(root, directory) if directory else None

    def file(self, relative):
        path = artifact_path(self.root, relative)
        if relative not in self.manifest['files']:
            raise ValueError(f'file is outside the compiled artifact: {relative}')
        if relative not in self.verified:
            if digest(path) != self.manifest['files'][relative]:
                raise ValueError(f'compiled artifact digest mismatch: {relative}')
            self.verified.add(relative)
        return path

    def target(self, package, name, test=False, variant='root'):
        targets = [entry for entry in self.manifest['targets']
                   if entry['package'] == package and entry['name'] == name and entry['test'] == test
                   and entry['variant'] == variant]
        if len(targets) != 1:
            raise ValueError(f'compiled target missing or ambiguous: {package}::{name}, test={test}')
        return self.file(targets[0]['path'])

    def install_sdk(self, destination):
        """Install the verified wheel into an isolated consumer without a compiler."""
        destination = Path(destination).resolve()
        destination.mkdir(parents=True, exist_ok=True)
        distribution = self.manifest['sdk_distribution']
        for relative in distribution.values():
            source = self.file(relative)
            shutil.copy2(source, destination / source.name)
        python = destination / 'consumer/bin/python'
        subprocess.run([sys.executable, '-m', 'venv', '--without-pip', str(python.parent.parent)],
                       check=True, timeout=600)
        environment = dict(os.environ, PATH=str(python.parent))
        wheel = destination / Path(distribution['wheel']).name
        subprocess.run([sys.executable, '-m', 'pip', '--python', str(python), 'install',
                        '--no-index', '--no-deps', '--force-reinstall', str(wheel)],
                       cwd=destination, env=environment, check=True, timeout=600)
        subprocess.run([str(python), '-c', 'from crowdb_dataset import DatasetClient; '
                        'from crowdb_dataset._native import NativeClient; '
                        'print("installed native Dataset SDK")'],
                       cwd=destination, env=environment, check=True, timeout=600)
        return python
