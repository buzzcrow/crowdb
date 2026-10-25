# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Bounded host browser batches with stable owned container networks."""

import concurrent.futures
from contextlib import ExitStack


class BatchFixture:
    """Retain each client's failure until its own deferred lifecycle teardown."""
    def __init__(self, cluster):
        self.cluster = cluster
        self.failure = None

    def __enter__(self):
        return self.cluster.__enter__()

    def __exit__(self, kind, error, traceback):
        if error is None and self.failure is not None:
            error = self.failure
            kind, traceback = type(error), error.__traceback__
        return self.cluster.__exit__(kind, error, traceback)


def run_browser_batches(cases, slots, prepare, execute, fail_fast=False):
    errors = []
    for offset in range(0, len(cases), slots):
        batch = cases[offset:offset + slots]
        # Docker bridge creation/removal must finish before browser requests.
        # Keep all fixtures in this bounded batch until every client has ended.
        try:
            with ExitStack() as stack:
                owners = [BatchFixture(prepare(case)) for case in batch]
                clusters = [stack.enter_context(owner) for owner in owners]
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
            errors.append(error)
        if fail_fast and errors:
            break
    return errors
