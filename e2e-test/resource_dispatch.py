# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Admit fixtures in selection order against their individual resource costs."""

import concurrent.futures
from collections import deque


def run_with_resources(cases, slots, execute, resources, budget, stopped):
    pending = deque((case, resources(case)) for case in cases)
    if slots < 1 or any(any(cost > limit for cost, limit in zip(costs, budget))
                        for _, costs in pending):
        raise ValueError("fixture exceeds the dispatch resource budget")
    errors, active = [], {}
    used = [0, 0]
    with concurrent.futures.ThreadPoolExecutor(max_workers=slots) as pool:
        try:
            while active or (pending and not stopped.is_set()):
                while pending and len(active) < slots and not stopped.is_set():
                    case, costs = pending[0]
                    if any(value + cost > limit for value, cost, limit in zip(used, costs, budget)):
                        break
                    pending.popleft()
                    used = [value + cost for value, cost in zip(used, costs)]
                    active[pool.submit(execute, case)] = costs
                if not active:
                    break
                completed, _ = concurrent.futures.wait(active, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in completed:
                    costs = active.pop(future)
                    used = [value - cost for value, cost in zip(used, costs)]
                    try:
                        future.result()
                    except Exception as error:
                        errors.append(error)
        except BaseException:
            stopped.set()
            for future in active:
                future.cancel()
            raise
    return errors


def resource_batches(cases, slots, resources, budget):
    """Pack ordered, stable batches against their actual combined costs."""
    costs = [(case, resources(case)) for case in cases]
    if slots < 1 or any(any(cost > limit for cost, limit in zip(values, budget))
                        for _, values in costs):
        raise ValueError("fixture exceeds the dispatch resource budget")
    batch, used = [], [0, 0]
    for case, values in costs:
        if batch and (len(batch) == slots or any(
                value + cost > limit for value, cost, limit in zip(used, values, budget))):
            yield batch
            batch, used = [], [0, 0]
        batch.append(case)
        used = [value + cost for value, cost in zip(used, values)]
    if batch:
        yield batch
