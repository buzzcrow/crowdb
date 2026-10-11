# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Bounded host browser batches with stable owned container networks."""

import concurrent.futures
from contextlib import ExitStack


def run_browser_batches(cases, slots, prepare, execute):
    errors = []
    for offset in range(0, len(cases), slots):
        batch = cases[offset:offset + slots]
        # Docker bridge creation/removal must finish before browser requests.
        # Keep all fixtures in this bounded batch until every client has ended.
        try:
            with ExitStack() as stack:
                clusters = [stack.enter_context(prepare(case)) for case in batch]
                with concurrent.futures.ThreadPoolExecutor(max_workers=slots) as pool:
                    futures = [pool.submit(execute, case, cluster)
                               for case, cluster in zip(batch, clusters)]
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            future.result()
                        except Exception as error:
                            errors.append(error)
        except Exception as error:
            errors.append(error)
    return errors
