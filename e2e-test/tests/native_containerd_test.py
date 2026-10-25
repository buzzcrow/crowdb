# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Native host provenance, early validation and owned-daemon teardown boundaries."""

import contextlib
import io
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import MagicMock, patch

import run
from oci_native_runtime import NativeRuntime, docker_free
from oci_host_node import HostNode


class NativeContainerdTest(unittest.TestCase):
    def test_host_cleanup_only_skips_absent_receipts_and_rejects_invalid_ownership(self):
        node = HostNode.__new__(HostNode)
        node.cluster = 'test'
        node.env = {'CROWDB_RUNTIME_ROOT':'/unused'}
        with patch('oci_host_node.read_private', return_value=None), patch.object(node, 'record') as record:
            node.cleanup()
            record.assert_not_called()
        for error in ('corrupt receipt', 'foreign owner'):
            with patch('oci_host_node.read_private', return_value={}), \
                    patch.object(node, 'record', side_effect=ValueError(error)), \
                    patch('oci_host_node.clean') as clean:
                with self.assertRaisesRegex(ValueError, error): node.cleanup()
                clean.assert_not_called()

    def test_native_profile_listing_and_invalid_budgets_have_no_runtime_effects(self):
        for options in [('--concurrency','2'), ('--concurrency','1','--memory-mib','2048'),
                        ('--concurrency','1','--layer','kv')]:
            with patch.object(sys,'argv',['run.py','--profile','containerd-host',*options]), \
                    patch('native_containerd_profile.docker_free') as preflight, \
                    contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit): run.main()
                preflight.assert_not_called()
        output=io.StringIO()
        with patch.object(sys,'argv',['run.py','--profile','containerd-host','--concurrency','1','--list']), \
                patch('native_containerd_profile.docker_free') as preflight, contextlib.redirect_stdout(output):
            run.main()
            preflight.assert_not_called()
        self.assertIn('local_protocols_and_production_metadata_recreation',output.getvalue())

    def test_docker_client_socket_daemon_and_container_host_cannot_claim_native_execution(self):
        for condition in ('client','socket','daemon','container'):
            with self.subTest(condition=condition), \
                    patch('oci_native_runtime.shutil.which',return_value='/docker' if condition=='client' else None), \
                    patch('oci_native_runtime.Path.exists',autospec=True,
                          side_effect=lambda path: str(path)==('/var/run/docker.sock' if condition=='socket' else '/.dockerenv')
                          if condition in ('socket','container') else False), \
                    patch('oci_native_runtime.Path.glob',return_value=[MagicMock(read_text=lambda:'dockerd')] if condition=='daemon' else []):
                with self.assertRaises(ValueError): docker_free()
        with patch('oci_native_runtime.shutil.which',return_value=None), \
                patch('oci_native_runtime.Path.exists',autospec=True,side_effect=lambda path:str(path)=='/.dockerenv'), \
                patch('oci_native_runtime.Path.glob',return_value=[]):
            docker_free(allow_container_host=True)
            with self.assertRaisesRegex(ValueError,'substitute'): docker_free()

    def test_owned_daemon_is_closed_after_body_failure_without_touching_other_processes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            prefix=root/'tools'
            (prefix/'bin').mkdir(parents=True)
            for name in ('containerd','containerd-shim-runc-v2','nerdctl','runc'):
                (prefix/'bin'/name).touch()
            process=MagicMock(pid=4242)
            process.poll.return_value=None
            runtime=MagicMock()
            runtime.invoke.return_value='{"ID":"owned","Driver":"native"}'
            with patch.dict(os.environ,{'CROWDB_OCI_RUNTIME_PREFIX':str(prefix)}), \
                    patch('oci_native_runtime.docker_free'), patch('oci_native_runtime.os.geteuid',return_value=0), \
                    patch('oci_native_runtime.Runtime',return_value=runtime), \
                    patch('oci_native_runtime.subprocess.run',return_value=MagicMock(stdout='version=4\nimports=["/etc/containerd/conf.d/*.toml"]\ndisabled_plugins=[]\n[plugins."io.containerd.server.v1.grpc"]\naddress="/run/containerd/containerd.sock"\n[plugins."io.containerd.server.v1.ttrpc"]\naddress="/run/containerd/containerd.sock.ttrpc"\n')), \
                    patch('oci_native_runtime.subprocess.Popen',return_value=process), \
                    patch('oci_native_runtime.os.killpg') as kill:
                with self.assertRaisesRegex(RuntimeError,'test body'), NativeRuntime(root/'owned','test'):
                    raise RuntimeError('test body')
                config = tomllib.loads((root/'owned/config.toml').read_text())
                self.assertEqual(config['imports'], [])
                self.assertTrue(config['plugins']['io.containerd.server.v1.grpc']['address'].startswith('/tmp/crowdb-ctd-'))
                self.assertEqual(config['plugins']['io.containerd.server.v1.ttrpc']['address'], config['plugins']['io.containerd.server.v1.grpc']['address'] + '.ttrpc')
                self.assertIn('io.containerd.cri.v1.runtime',config['disabled_plugins'])
                kill.assert_called_once_with(4242,signal.SIGTERM)
                process.wait.assert_called_once_with(timeout=10)

    def test_host_fixture_imports_cannot_shadow_the_e2e_artifact_publisher(self):
        result=subprocess.run([sys.executable,'-c',
            'import native_containerd_profile; import publish; print(publish.__file__)'],
            env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[1])),
            capture_output=True,text=True,check=True,timeout=10)
        self.assertEqual(Path(result.stdout.strip()).resolve(),Path(__file__).resolve().parents[1]/'publish.py')
