# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Compiled consumers reject stale, partial and unowned build outputs."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci-build'))
import compiled_artifacts

spec = importlib.util.spec_from_file_location('compiled_unpack', ROOT / 'tools/ci-build/unpack.py')
unpack = importlib.util.module_from_spec(spec)
spec.loader.exec_module(unpack)


class CompiledArtifactsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.expected = dict(schema_version=1, kind='clients', profile='release',
                             build_root=str(self.root), revision='a' * 40,
                             source_tree='b' * 40, lock_digest='c' * 64)
        self.path = 'target/release/domain-test'
        self.body = b'compiled executable\n'
        self.manifest = dict(self.expected, files={self.path: hashlib.sha256(self.body).hexdigest()},
                             targets=[dict(package='e2e-test', name='domain_test', test=True,
                                           variant='root', path=self.path)])
        identity = patch.object(compiled_artifacts, 'identity',
                                side_effect=lambda root, kind: dict(self.expected, kind=kind))
        identity.start()
        self.addCleanup(identity.stop)

    def archive(self, manifest=None, members=None):
        manifest = self.manifest if manifest is None else manifest
        descriptor = unpack.MANIFESTS[manifest['kind']]
        archive = self.root / 'clients.tar.gz'
        members = [(self.path, self.body, tarfile.REGTYPE)] if members is None else members
        with tarfile.open(archive, 'w:gz') as stream:
            for name, body, kind in [(descriptor, json.dumps(manifest).encode(), tarfile.REGTYPE), *members]:
                member = tarfile.TarInfo(name)
                member.type = kind
                member.size = len(body) if kind == tarfile.REGTYPE else 0
                member.mode = 0o755
                stream.addfile(member, io.BytesIO(body) if member.isfile() else None)
        return archive

    def test_restore_and_select_exact_release_executable_preserves_mode(self):
        unpack.unpack(self.archive(), self.root)
        artifact = compiled_artifacts.CompiledArtifacts(self.root, self.root / 'target/compiled-tests')
        executable = artifact.target('e2e-test', 'domain_test', test=True)
        self.assertEqual(executable.read_bytes(), self.body)
        self.assertEqual(executable.stat().st_mode & 0o777, 0o755)
        with self.assertRaisesRegex(ValueError, 'missing or ambiguous'):
            artifact.target('e2e-test', 'domain_test', test=False)

    def test_revision_source_lock_profile_and_build_path_must_match(self):
        for field in ('schema_version', 'revision', 'source_tree', 'lock_digest', 'profile', 'build_root'):
            with self.subTest(field=field):
                manifest = dict(self.manifest, **{field: 'stale'})
                with self.assertRaises(ValueError):
                    unpack.unpack(self.archive(manifest), self.root)
                self.assertFalse((self.root / self.path).exists())

    def test_corrupt_payload_cannot_replace_an_existing_executable(self):
        existing = self.root / self.path
        existing.parent.mkdir(parents=True)
        existing.write_bytes(b'previous executable')
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            unpack.unpack(self.archive(members=[(self.path, b'corrupt', tarfile.REGTYPE)]), self.root)
        self.assertEqual(existing.read_bytes(), b'previous executable')

    def test_missing_extra_duplicate_and_symlink_members_are_rejected(self):
        for members in ([], [(self.path, self.body, tarfile.REGTYPE), ('target/extra', b'x', tarfile.REGTYPE)],
                        [(self.path, self.body, tarfile.REGTYPE)] * 2,
                        [(self.path, b'', tarfile.SYMTYPE)]):
            with self.subTest(members=members), self.assertRaises(ValueError):
                unpack.unpack(self.archive(members=members), self.root)
            self.assertFalse((self.root / self.path).exists())

    def test_consumption_rechecks_payload_after_restore(self):
        unpack.unpack(self.archive(), self.root)
        (self.root / self.path).write_bytes(b'changed after download')
        artifact = compiled_artifacts.CompiledArtifacts(self.root, self.root / 'target/compiled-tests')
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            artifact.target('e2e-test', 'domain_test', test=True)

    def test_owned_target_symlink_is_supported_but_nested_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as output:
            (self.root / 'target').symlink_to(output, target_is_directory=True)
            unpack.unpack(self.archive(), self.root)
            self.assertEqual((Path(output) / 'release/domain-test').read_bytes(), self.body)
            (Path(output) / 'escape').symlink_to(self.root, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, 'escapes'):
                compiled_artifacts.artifact_path(self.root, 'target/escape/unowned')
        for path in ('../outside', '/tmp/outside', 'sdk/src/lib.rs', 'target/../outside',
                     'target/./file', 'target//file'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                compiled_artifacts.artifact_path(self.root, path)

    def test_duplicate_target_selection_cannot_choose_arbitrarily(self):
        self.manifest['targets'] *= 2
        unpack.unpack(self.archive(), self.root)
        artifact = compiled_artifacts.CompiledArtifacts(self.root, self.root / 'target/compiled-tests')
        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            artifact.target('e2e-test', 'domain_test', test=True)

    def test_runtime_restore_replaces_stale_files_and_checks_source_receipts(self):
        files = {'target/container-runtime/SOURCE_REVISION': self.expected['revision'].encode(),
                 'target/container-runtime/SOURCE_TREE': self.expected['source_tree'].encode(),
                 'target/container-runtime/bin/server': self.body}
        manifest = dict(self.manifest, kind='runtime', targets=[],
                        files={name: hashlib.sha256(body).hexdigest() for name, body in files.items()})
        staging = self.root / 'target/container-runtime'
        staging.mkdir(parents=True)
        (staging / 'obsolete-server').write_bytes(b'stale')
        members = [(name, body, tarfile.REGTYPE) for name, body in files.items()]
        unpack.unpack(self.archive(manifest, members), self.root)
        self.assertFalse((staging / 'obsolete-server').exists())
        self.assertEqual((staging / 'bin/server').read_bytes(), self.body)
        wrong_receipt = 'target/container-runtime/SOURCE_TREE'
        files[wrong_receipt] = b'wrong source tree'
        manifest['files'][wrong_receipt] = hashlib.sha256(files[wrong_receipt]).hexdigest()
        with self.assertRaisesRegex(ValueError, 'SOURCE_TREE'):
            unpack.unpack(self.archive(manifest, [(name, body, tarfile.REGTYPE)
                                                for name, body in files.items()]), self.root)
        self.assertEqual((staging / 'SOURCE_TREE').read_text(), self.expected['source_tree'])
