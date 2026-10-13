# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Validate one regular CI owner for each complete container domain."""

import re
import shlex
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "e2e-test"))
from ci_partitions import PARTITIONS

DOMAINS = {"kv", "diskdb", "diskio", "chunkdb", "chunk-stream", "chunk-client",
           "chunk-kv", "s3", "iceberg", "dataset", "console", "deployment"}


def check_partitions(block, domain):
    if "--partition" not in block:
        if re.search(r"^\s+partition:", block, re.M):
            raise ValueError("partition matrix must dispatch its selected partition")
        return
    expected = PARTITIONS.get(domain)
    matrix = re.search(r"^\s+partition: \[([^\]]+)\]$", block, re.M)
    actual = tuple(item.strip() for item in matrix[1].split(",")) if matrix else ()
    if not expected or len(actual) != len(expected) or set(actual) != set(expected):
        raise ValueError(f"container CI coverage: {domain} partition matrix must cover each partition exactly once")
    if "E2E_PARTITION: ${{ matrix.partition }}" not in block:
        raise ValueError("partition matrix must dispatch through E2E_PARTITION")
    invocation = f'pixi run test-e2e-{domain} --partition "$E2E_PARTITION"'
    lines = [line.strip().removeprefix("run: ") for line in block.splitlines()
             if f"pixi run test-e2e-{domain}" in line]
    if lines != [invocation]:
        raise ValueError("partition invocation must use the complete matrix selection")


def expand_matrix(workflow):
    if 'pixi run "test-e2e-$domain"' not in workflow:
        return workflow
    if "E2E_DOMAINS: ${{ matrix.domains }}" not in workflow or "for domain in $E2E_DOMAINS; do" not in workflow:
        raise ValueError("domain matrix must be dispatched through E2E_DOMAINS")
    for line in workflow.splitlines():
        if 'pixi run "test-e2e-$domain"' in line and line.strip() != 'pixi run "test-e2e-$domain"':
            raise ValueError("domain matrix must run complete domains without additional selectors")
    domains = re.findall(r"^\s+domains: ([\w -]+)$", workflow, re.M)
    if not domains:
        raise ValueError("domain matrix has no domains")
    return workflow + "\n" + "\n".join(
        f"pixi run test-e2e-{domain}" for row in domains for domain in row.split())


def check_coverage(workflow, graph):
    if re.search(r"pixi run (?:test-container-e2e|test-suite)(?:\s|$)", workflow):
        raise ValueError("regular CI must use domain tasks so duplicate ownership is visible")
    owners = {}
    blocks = re.split(r"^  ([A-Za-z][\w-]*):\s*$", workflow.split("jobs:", 1)[1], flags=re.M)
    for name, block in zip(blocks[1::2], blocks[2::2]):
        block = re.sub(r"\\\n\s*", " ", block)
        expanded = expand_matrix(block)
        for line in block.splitlines():
            if re.search(r"pixi run test-e2e-[\w-]+", line) and any(
                    flag in line for flag in ("--scope", "--case", "--layer", "--profile", "--list")):
                raise ValueError(f"{name} narrows a required domain in its CI invocation")
        for key in graph.calls(expanded):
            if not key[1].startswith("test-e2e-"):
                continue
            domain = key[1].removeprefix("test-e2e-")
            if domain not in DOMAINS:
                raise ValueError(f"unknown CI domain {domain}")
            check_partitions(block, domain)
            command = shlex.split(graph.command(key).splitlines()[0])
            if command != ["python", "tools/pixi-tasks/test.py", "e2e", domain]:
                raise ValueError(f"{key[1]} must run its complete domain without narrowing selectors")
            owners.setdefault(domain, []).append(name)
    missing = sorted(DOMAINS - owners.keys())
    duplicated = {domain: jobs for domain, jobs in owners.items() if len(jobs) != 1}
    if missing or duplicated:
        raise ValueError(f"container CI coverage: missing={missing}, duplicate owners={duplicated}")
    return owners
