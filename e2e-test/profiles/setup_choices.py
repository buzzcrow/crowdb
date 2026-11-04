# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit choices for fixtures that test bootstrap rather than default setup."""


def diskless_setup():
    services = [
        {"kind": "paxos-kv", "enabled": True,
         "listeners": {"http_port": 10000, "rpc_port": 10100}}]
    services += [
        {"kind": kind, "enabled": False, "listeners": {}}
        for kind in ("diskdb", "diskio", "chunkdb", "chunk-kv", "access-server")]
    return {"storage": {"mode": "diskless"}, "services": services}


def diskless_policy():
    lines = ['[storage]', 'mode = "diskless"']
    for service in diskless_setup()["services"]:
        lines += ['[[services]]', f'kind = "{service["kind"]}"',
                  f'enabled = {str(service["enabled"]).lower()}']
        if service["listeners"]:
            lines += ['[services.listeners]',
                      *(f'{key} = {value}' for key, value in service["listeners"].items())]
    return '\n'.join(lines) + '\n'
