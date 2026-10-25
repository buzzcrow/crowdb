# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import concurrent.futures
import threading
import unittest

from resource_dispatch import batch_slots, run_with_resources


class ResourceDispatchTest(unittest.TestCase):
    def test_large_case_is_exclusive_but_small_cases_share_the_budget(self):
        cases = ("large", "left", "right", "last")
        costs = {case: (3.75, 5888) if case in ("large", "last") else (1.5, 2560)
                 for case in cases}
        started = {case: threading.Event() for case in cases}
        finished = {case: threading.Event() for case in cases}
        release = threading.Event()

        def execute(case):
            started[case].set()
            if case == "large":
                self.assertTrue(release.wait(5))
            elif case in ("left", "right"):
                self.assertTrue(finished["large"].is_set())
                self.assertTrue(started["right" if case == "left" else "left"].wait(5))
            else:
                self.assertTrue(all(finished[peer].is_set() for peer in cases[:-1]))
            finished[case].set()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as owner:
            result = owner.submit(run_with_resources, cases, 2, execute, costs.__getitem__,
                                  (4, 8192), threading.Event())
            try:
                self.assertTrue(started["large"].wait(5))
                self.assertFalse(started["left"].is_set())
                self.assertFalse(started["right"].is_set())
            finally:
                release.set()
            self.assertEqual(result.result(timeout=10), [])
        self.assertTrue(all(event.is_set() for event in finished.values()))

    def test_memory_budget_prevents_parallel_dispatch_even_with_idle_cpu(self):
        completed = []

        def execute(case):
            self.assertEqual(completed, list(range(case)))
            completed.append(case)

        self.assertEqual(run_with_resources(range(3), 3, execute, lambda _: (1, 4096),
                                             (4, 6144), threading.Event()), [])
        self.assertEqual(completed, [0, 1, 2])

    def test_completion_refills_a_slot_without_waiting_for_an_unrelated_peer(self):
        first = threading.Event()
        next_case = threading.Event()

        def execute(case):
            if case == 0:
                first.set()
                self.assertTrue(next_case.wait(5))
            elif case == 1:
                self.assertTrue(first.wait(5))
            else:
                next_case.set()

        self.assertEqual(run_with_resources(range(3), 2, execute, lambda _: (1.5, 2560),
                                             (4, 8192), threading.Event()), [])

    def test_failure_drains_owned_peer_and_does_not_dispatch_later_cases(self):
        stopped = threading.Event()
        ready = threading.Barrier(2, timeout=5)
        finished = threading.Event()

        def execute(case):
            self.assertLess(case, 2)
            ready.wait()
            if case == 0:
                stopped.set()
                raise ValueError("original failure")
            self.assertTrue(stopped.wait(5))
            finished.set()

        errors = run_with_resources(range(3), 2, execute, lambda _: (1, 1024), (4, 8192), stopped)
        self.assertEqual([str(error) for error in errors], ["original failure"])
        self.assertTrue(finished.is_set())

    def test_oversized_case_is_rejected_before_any_fixture_starts(self):
        with self.assertRaisesRegex(ValueError, "budget"):
            run_with_resources([0], 2, lambda _: self.fail("allocated oversized fixture"),
                               lambda _: (1.5, 7168), (4, 6144), threading.Event())

    def test_browser_batch_stays_within_both_budgets(self):
        costs = {"small": (1.5, 2560), "large": (3.5, 5120)}
        self.assertEqual(batch_slots(["small", "large"], 2, costs.__getitem__, (4, 8192)), 1)
        self.assertEqual(batch_slots(["small"] * 2, 2, costs.__getitem__, (4, 8192)), 2)
