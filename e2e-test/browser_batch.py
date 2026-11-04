# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Bounded host browser batches with stable owned container networks."""

import concurrent.futures
from contextlib import ExitStack
from fixture_result import CaseNotExecuted
from resource_dispatch import resource_batches


class BatchFixture:
    """Retain each client's failure until its own deferred lifecycle teardown."""
    def __init__(self, cluster, case_name, errors):
        self.cluster = cluster
        self.case_name = getattr(cluster, 'case_name', case_name)
        self.errors = errors
        self.failure = None

    def __enter__(self):
        return self.cluster.__enter__()

    def __exit__(self, kind, error, traceback):
        if self.failure is not None:
            error = self.failure
            kind, traceback = type(error), error.__traceback__
        try:
            return self.cluster.__exit__(kind, error, traceback)
        except Exception as cleanup_error:
            # An unwind error belongs to this fixture, not the next peer.
            self.errors.append(cleanup_error)
            return False


def _enter_fixtures(stack, owners, errors):
    clusters = []
    for owner in owners:
        try:
            clusters.append(stack.enter_context(owner))
        except BaseException as error:
            # Keep startup first even when peer cleanup also fails.
            if isinstance(error, Exception):
                errors.append(error)
            for entered in owners[:len(clusters)]:
                entered.failure = CaseNotExecuted(owner.case_name)
            raise
    return clusters


def run_browser_batches(cases, slots, prepare, execute, fail_fast=False,
                        resources=lambda _: (0, 0), budget=(float("inf"), float("inf"))):
    errors = []
    for batch in resource_batches(cases, slots, resources, budget):
        # Docker bridge creation/removal must finish before browser requests.
        # Keep all fixtures in this bounded batch until every client has ended.
        try:
            with ExitStack() as stack:
                owners = [BatchFixture(prepare(case), getattr(case, 'name', str(case)), errors)
                          for case in batch]
                clusters = _enter_fixtures(stack, owners, errors)
                with concurrent.futures.ThreadPoolExecutor(max_workers=slots) as pool:
                    futures = {pool.submit(execute, case, cluster): owner
                               for case, cluster, owner in zip(batch, clusters, owners)}
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            future.result()
                        except Exception as error:
                            futures[future].failure = error
                            errors.append(error)
        except Exception as error:
            if not any(error is previous for previous in errors):
                errors.append(error)
        if fail_fast and errors:
            break
    return errors
