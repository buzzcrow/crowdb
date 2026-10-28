# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Container diagnostic ownership and failure propagation contracts."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'container/oci-image'))
SPEC = importlib.util.spec_from_file_location('paxos_cluster_diagnostics', ROOT / 'container/oci-image/paxos-kv-cluster.py')
cluster = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cluster)
PID_SPEC = importlib.util.spec_from_file_location('paxos_profile_pids', ROOT / 'tools/profiling/container-server-pids.py')
profile_pids = importlib.util.module_from_spec(PID_SPEC)
PID_SPEC.loader.exec_module(profile_pids)


class PaxosKvDiagnosticsTest(unittest.TestCase):
    def test_profile_selects_server_instead_of_container_init(self):
        table = ('PID COMMAND\n101 /usr/bin/tini -- /entrypoint server paxos-kv\n'
                 '102 /opt/crowdb/bin/crowdb-paxos-kv-server --root /data\n'
                 '103 logger crowdb-paxos-kv-server\n')
        self.assertEqual(profile_pids.server_pid(table), 102)

    def test_profile_rejects_absent_or_ambiguous_server(self):
        for table in ['PID COMMAND\n101 /usr/bin/tini\n',
                      'PID COMMAND\n101 crowdb-paxos-kv-server\n102 crowdb-paxos-kv-server\n']:
            with self.assertRaisesRegex(ValueError, 'exactly one live'):
                profile_pids.server_pid(table)

    def test_foreign_container_is_never_stopped(self):
        found = {'Config': {'Labels': {cluster.LABEL: 'foreign'}}}
        with patch.object(cluster, 'docker', return_value=json.dumps([found])) as docker:
            with self.assertRaisesRegex(RuntimeError, 'ownership mismatch'):
                cluster.stop({'id': 'owned', 'containers': ['node']}, graceful=True)
            self.assertEqual(docker.call_args_list[0].args, ('inspect', 'node'))
            self.assertEqual(docker.call_count, 1)

    def check_shutdown(self, code=0, oom=False, report=False):
        state = {'id': 'owned', 'containers': ['node'], 'sanitizer': True}
        found = {'Config': {'Labels': {cluster.LABEL: 'owned'}},
                 'State': {'ExitCode': code, 'OOMKilled': oom}}
        calls = []
        def docker(*args):
            calls.append(args)
            if args[0] == 'inspect':
                return json.dumps([found])
            if args[0] == 'cp':
                output = Path(args[-1])
                output.mkdir()
                if report:
                    (output / 'report.1').write_text('ERROR: LeakSanitizer: detected memory leaks')
            return 'service lifecycle' if args[0] == 'logs' else ''
        with tempfile.TemporaryDirectory() as temporary, patch.object(cluster, 'docker', side_effect=docker):
            artifacts = Path(temporary)
            failures = cluster.stop(state, artifacts, graceful=True)
            self.assertTrue((artifacts / 'node/inspect.json').is_file())
            self.assertTrue((artifacts / 'node/stdout.log').is_file())
            self.assertTrue((artifacts / 'node/log').is_dir())
        self.assertLess(calls.index(('stop', '--time', '30', 'node')), calls.index(('rm', '-f', '-v', 'node')))
        return failures

    def test_clean_shutdown_passes(self):
        self.assertEqual(self.check_shutdown(), [])

    def test_collection_failure_preserves_node_and_stops_other_nodes(self):
        state = {'id': 'owned', 'containers': ['first', 'second'], 'sanitizer': True}
        found = {'Config': {'Labels': {cluster.LABEL: 'owned'}},
                 'State': {'ExitCode': 0, 'OOMKilled': False}}
        calls = []
        def docker(*args):
            calls.append(args)
            if args[0] == 'inspect':
                return json.dumps([found])
            if args[0] == 'cp':
                if args[1].startswith('first:'):
                    raise RuntimeError('cannot read diagnostic directory')
                Path(args[-1]).mkdir(exist_ok=True)
            return ''
        with tempfile.TemporaryDirectory() as temporary, patch.object(cluster, 'docker', side_effect=docker):
            failures = cluster.stop(state, Path(temporary), graceful=True)
        self.assertIn('cannot read diagnostic directory', '\n'.join(failures))
        self.assertEqual(state['containers'], ['first'])
        self.assertIn(('stop', '--time', '30', 'second'), calls)
        self.assertIn(('rm', '-f', '-v', 'second'), calls)
        self.assertNotIn(('rm', '-f', '-v', 'first'), calls)

    def test_all_ownership_is_checked_before_any_stop(self):
        def docker(*args):
            owner = 'owned' if args[1] == 'first' else 'foreign'
            return json.dumps([{'Config': {'Labels': {cluster.LABEL: owner}}}])
        with patch.object(cluster, 'docker', side_effect=docker) as mocked:
            with self.assertRaisesRegex(RuntimeError, 'ownership mismatch: second'):
                cluster.stop({'id': 'owned', 'containers': ['first', 'second']}, graceful=True)
            self.assertTrue(all(call.args[0] == 'inspect' for call in mocked.call_args_list))

    def test_killed_oom_and_sanitizer_report_each_fail(self):
        self.assertTrue(self.check_shutdown(code=137))
        self.assertTrue(self.check_shutdown(oom=True))
        self.assertIn('LeakSanitizer', '\n'.join(self.check_shutdown(report=True)))

    def test_sampler_success_cannot_hide_failed_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = root / 'deployment.json'
            state.write_text(json.dumps({'containers': ['a', 'b', 'c'], 'endpoints': ['http://localhost:1'] * 3}))
            cli = root / 'fake-cli'
            cli.write_text('#!/usr/bin/env python3\nimport sys\nfrom pathlib import Path\n'
                           'Path(sys.argv[sys.argv.index("--output")+1]).write_text(\'{"passed":false}\')\n')
            cli.chmod(0o700)
            env = dict(os.environ, KV_BENCH_DEPLOYMENT=str(state), KV_BENCH_CLI=str(cli),
                       KV_BENCH_LOG_ROOT=str(root / 'logs'))
            run = subprocess.run(['bash', '-c', 'source tools/benchmark/bench-kv-common.sh; invoke test write'],
                                 cwd=ROOT, env=env, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
            self.assertIn('failed benchmark report', run.stderr)


if __name__ == '__main__':
    unittest.main()
