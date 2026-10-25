# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from scenarios import Case, run_cases


class RunnerProgressionTest(unittest.TestCase):
    def run_selection(self, fail_fast, stable=False, slots=1):
        started = []
        with tempfile.TemporaryDirectory() as temporary:
            class Fixture:
                def __init__(self, case):
                    self.case = case
                    self.artifacts = Path(temporary) / case.name
                    self.artifacts.mkdir()

                def __enter__(self):
                    return self

                def __exit__(self, *_):
                    pass

                def client(self, *_):
                    started.append(self.case.name)
                    if self.case.name == 'first':
                        raise ValueError('original failure')

            cases = [Case('test', name, native_client=stable) for name in ('first', 'next', 'last')]
            with patch('scenarios.select_cases', return_value=cases), \
                 patch('scenarios.allocate_cluster', side_effect=lambda case, *_: Fixture(case)):
                with self.assertRaisesRegex(ValueError, 'original failure'):
                    run_cases('image', temporary, 'bundle', slots, fail_fast=fail_fast)
        return started

    def test_first_failure_stops_queued_cases_without_interrupting_executor(self):
        self.assertEqual(self.run_selection(True), ['first'])

    def test_default_collects_all_independent_cases(self):
        self.assertEqual(self.run_selection(False), ['first', 'next', 'last'])

    def test_fail_fast_finishes_already_owned_browser_peers(self):
        self.assertCountEqual(self.run_selection(True, stable=True, slots=2), ['first', 'next'])

    def test_runner_uses_each_case_cost_and_keeps_browser_networks_separate(self):
        peers = threading.Barrier(2, timeout=5)
        finished = set()
        with tempfile.TemporaryDirectory() as temporary:
            cases = [Case('test', name) for name in ('left', 'right')]
            cases.append(Case('deployment.browser', 'browser'))

            class Fixture:
                def __init__(self, case):
                    self.name = case.name
                    self.artifacts = Path(temporary) / self.name
                    self.artifacts.mkdir()

                def __enter__(self):
                    return self

                def __exit__(self, *_):
                    finished.add(self.name)

                def client(self, *_):
                    peers.wait()

                def browser(self):
                    self_owner.assertEqual(finished, {'left', 'right'})

            self_owner = self
            with patch('scenarios.select_cases', return_value=cases), \
                 patch('scenarios.allocate_cluster', side_effect=lambda case, *_: Fixture(case)):
                run_cases('image', temporary, 'bundle', 2, resource_budget=(4, 4096))
        self.assertEqual(finished, {'left', 'right', 'browser'})
