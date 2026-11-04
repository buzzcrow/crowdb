# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Batch setup failure does not impersonate a dispatched browser failure."""

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from browser_batch import run_browser_batches
from fixture import Cluster
from report import write_report


class TestCluster(Cluster):
    def __init__(self, directory, case):
        super().__init__('image', directory)
        self.case_name = case
        self.start_error = None
        self.collection_error = None
        self.cleanup_error = None
        self.collections = self.cleanups = 0
        (self.artifacts / 'case.json').write_text(json.dumps({
            'case': case, 'scope': 'console.native.browser.lifecycle'}))

    def start(self):
        if self.start_error:
            raise self.start_error
        return self

    def diagnostics(self):
        self.collections += 1
        if self.collection_error:
            raise self.collection_error

    def close(self):
        self.cleanups += 1
        if self.cleanup_error:
            raise self.cleanup_error
        self.closed = True
        self.write_timings()


class BrowserBatchProvenanceTest(unittest.TestCase):
    def run_failed_startup(self, directory, *, cleanup_error=None, collection_error=None):
        peer = TestCluster(directory, 'prepared-peer')
        failed = TestCluster(directory, 'failed-startup')
        failed.start_error = RuntimeError('original startup rejected')
        peer.cleanup_error, peer.collection_error = cleanup_error, collection_error
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            errors = run_browser_batches([peer, failed], 2, lambda case: case,
                                         lambda *_: self.fail('client was dispatched'))
        self.assertIs(errors[0], failed.start_error)
        self.assertEqual((peer.collections, peer.cleanups), (1, 1))
        self.assertEqual((failed.collections, failed.cleanups), (1, 1))
        self.assertTrue(failed.closed)
        self.assertEqual(failed.timings['status'], 'failed')
        self.assertEqual(output.getvalue().count('[FAILED CASE] failed-startup:'), 1)
        self.assertIn('original startup rejected', output.getvalue())
        self.assertIn('[NOT EXECUTED CASE] prepared-peer:', output.getvalue())
        return peer, errors, output.getvalue()

    def test_prepared_peer_is_not_executed_and_cannot_be_reported_as_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            peer, errors, output = self.run_failed_startup(directory)
            self.assertEqual(len(errors), 1)
            self.assertTrue(peer.closed)
            self.assertNotIn('[FAILED CASE] prepared-peer:', output)
            self.assertEqual(peer.timings['status'], 'not_executed')
            self.assertIn('failed-startup', peer.timings['not_executed_reason'])
            timing = json.loads((peer.artifacts / 'timing.json').read_text())
            self.assertEqual(timing['status'], 'not_executed')
            write_report(Path(directory), {'status': 'failed'})
            report = json.loads((Path(directory) / 'results.json').read_text())
            self.assertTrue(all(case['status'] != 'pass' for case in report['cases']))
            self.assertEqual({case['case']: case['status'] for case in report['cases']}, {
                'prepared-peer': 'not_executed', 'failed-startup': 'failed'})
            self.assertEqual(report['timings']['status'], 'failed')

    def test_peer_cleanup_failure_retains_original_startup_and_its_own_error(self):
        with tempfile.TemporaryDirectory() as directory:
            cleanup = RuntimeError('peer cleanup rejected')
            peer, errors, output = self.run_failed_startup(directory, cleanup_error=cleanup)
            self.assertEqual(errors[1:], [cleanup])
            self.assertEqual(peer.timings['status'], 'failed')
            self.assertIn('[FAILED CASE] prepared-peer: peer cleanup rejected', output)
            self.assertNotIn('[FAILED CASE] prepared-peer: original startup rejected', output)

    def test_peer_collection_failure_still_cleans_and_retains_its_own_error(self):
        with tempfile.TemporaryDirectory() as directory:
            collection = OSError('peer artifact disk full')
            peer, errors, output = self.run_failed_startup(directory, collection_error=collection)
            self.assertEqual(errors[1:], [collection])
            self.assertTrue(peer.closed)
            self.assertEqual(peer.timings['status'], 'failed')
            self.assertIn('[FAILED CASE] prepared-peer: peer artifact disk full', output)
            self.assertNotIn('[FAILED CASE] prepared-peer: original startup rejected', output)

    def test_peer_collection_and_cleanup_failures_are_both_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            collection = OSError('peer artifact disk full')
            cleanup = RuntimeError('peer cleanup rejected')
            peer, errors, output = self.run_failed_startup(
                directory, collection_error=collection, cleanup_error=cleanup)
            self.assertEqual(errors[1:], [collection])
            self.assertIn(str(cleanup), collection.__notes__)
            self.assertIs(collection.__cause__, cleanup)
            self.assertEqual(peer.timings['status'], 'failed')
            self.assertIn('[FAILED CASE] prepared-peer: peer artifact disk full', output)
            self.assertIn('[FAILED CASE] prepared-peer: peer cleanup rejected', output)

    def test_real_client_and_cleanup_failures_remain_independent_of_successful_peer(self):
        with tempfile.TemporaryDirectory() as directory:
            failed, peer = TestCluster(directory, 'failed-client'), TestCluster(directory, 'passed-client')
            failure = RuntimeError('browser assertion failed')
            failed.cleanup_error = RuntimeError('owned browser cleanup failed')
            executed = []

            def execute(case, cluster):
                executed.append(case)
                if cluster is failed:
                    raise failure

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                errors = run_browser_batches([failed, peer], 2, lambda case: case, execute)
            self.assertCountEqual(executed, [failed, peer])
            self.assertEqual(errors, [failure])
            self.assertEqual(failed.timings['status'], 'failed')
            self.assertEqual(peer.timings['status'], 'pass')
            self.assertIn('[FAILED CASE] failed-client: browser assertion failed', output.getvalue())
            self.assertEqual(output.getvalue().count('[FAILED CASE] failed-client:'), 1)
            self.assertIn('[CLEANUP ERROR] failed-client: owned browser cleanup failed', output.getvalue())
            self.assertIn('owned browser cleanup failed', failure.__notes__)
            self.assertNotIn('[FAILED CASE] passed-client:', output.getvalue())
            self.assertNotIn('[CLEANUP ERROR] passed-client:', output.getvalue())
            self.assertNotIn('[NOT EXECUTED CASE]', output.getvalue())
