# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import unittest
from browser_batch import run_browser_batches


class BrowserBatchTest(unittest.TestCase):
    def fixture(self, case):
        owner = self

        class Fixture:
            def __enter__(self):
                owner.events.append(('start', case))
                if case == owner.start_failure:
                    raise ValueError('startup rejected')
                owner.active.add(case)
                return self

            def __exit__(self, kind, error, traceback):
                owner.events.append(('close', case))
                owner.failures[case] = error
                owner.active.remove(case)

        return Fixture()

    def setUp(self):
        self.events = []
        self.active = set()
        self.start_failure = None
        self.failures = {}

    def test_clients_share_stable_bounded_batch_and_teardown_precedes_next_batch(self):
        def execute(case, fixture):
            self.assertEqual(self.active, {0, 1} if case < 2 else {2})
            self.events.append(('client', case))

        self.assertEqual(run_browser_batches([0, 1, 2], 2, self.fixture, execute), [])
        self.assertEqual(self.events[:2], [('start', 0), ('start', 1)])
        self.assertLess(self.events.index(('client', 0)), self.events.index(('close', 1)))
        self.assertLess(self.events.index(('client', 1)), self.events.index(('close', 1)))
        self.assertLess(self.events.index(('close', 0)), self.events.index(('start', 2)))
        self.assertEqual(self.active, set())

    def test_client_failure_collects_other_result_and_closes_both_fixtures(self):
        def execute(case, fixture):
            self.events.append(('client', case))
            if case == 0:
                raise ValueError('browser failed')

        errors = run_browser_batches([0, 1], 2, self.fixture, execute)
        self.assertEqual([str(error) for error in errors], ['browser failed'])
        self.assertIn(('client', 1), self.events)
        self.assertEqual(self.active, set())
        self.assertEqual(str(self.failures[0]), 'browser failed')
        self.assertIsNone(self.failures[1])

    def test_partial_startup_closes_started_peer_without_dispatching_clients(self):
        self.start_failure = 1
        errors = run_browser_batches([0, 1], 2, self.fixture,
                                     lambda *_: self.fail('partial batch dispatched'))
        self.assertEqual([str(error) for error in errors], ['startup rejected'])
        self.assertEqual(self.events, [('start', 0), ('start', 1), ('close', 0)])
        self.assertEqual(self.active, set())

    def test_fail_fast_closes_current_batch_before_skipping_later_fixtures(self):
        def execute(case, fixture):
            if case == 0:
                raise ValueError('browser failed')

        errors = run_browser_batches([0, 1], 1, self.fixture, execute, fail_fast=True)
        self.assertEqual([str(error) for error in errors], ['browser failed'])
        self.assertEqual(self.events, [('start', 0), ('close', 0)])
        self.assertEqual(self.active, set())

    def test_large_fixture_does_not_serialize_later_small_browser_batches(self):
        def execute(case, fixture):
            self.assertEqual(self.active, {0} if case == 0 else {1, 2})
            self.events.append(('client', case))

        errors = run_browser_batches([0, 1, 2], 2, self.fixture, execute,
                                     resources=lambda case: (1.5, 12800 if case == 0 else 6656),
                                     budget=(4, 15989))
        self.assertEqual(errors, [])
        self.assertLess(self.events.index(('close', 0)), self.events.index(('start', 1)))
        self.assertLess(self.events.index(('start', 2)), self.events.index(('client', 1)))
        self.assertLess(self.events.index(('client', 2)), self.events.index(('close', 1)))
        self.assertEqual(self.active, set())
