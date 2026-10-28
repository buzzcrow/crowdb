#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit standalone Paxos-KV container deployment; never called by bench."""
from ports import PAXOS_KV_SERVER_MGMT_BASE, PAXOS_KV_SERVER_RPC_BASE

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.request import build_opener, ProxyHandler, Request

LABEL = "crowdb.paxos-kv.deployment"
HTTP = build_opener(ProxyHandler({}))


def docker(*args):
    p = subprocess.run(["docker", *args], text=True, capture_output=True, timeout=60)
    if p.returncode:
        raise RuntimeError(p.stdout + p.stderr)
    return (p.stdout + p.stderr if args[0] == "logs" else p.stdout).strip()


def request(origin, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    with HTTP.open(Request(origin + path, data=data, headers={"Content-Type": "application/json"}), timeout=5) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def wait_ready(origins):
    deadline = time.monotonic() + 30
    last = None
    while time.monotonic() < deadline:
        try:
            result = [request(origin, "/topology") for origin in origins]
            if all(t.get("stores") and t["stores"][0].get("groups") for t in result):
                return result
        except (OSError, ValueError) as error:
            last = str(error)
        time.sleep(.1)
    raise RuntimeError(f"node readiness timeout: {last}")


def stop(state, artifacts=None, graceful=False):
    # Check every identity before mutating any deployment member.
    inspected = {}
    for name in state["containers"]:
        found = json.loads(docker("inspect", name))[0]
        if (found["Config"].get("Labels") or {}).get(LABEL) != state["id"]:
            raise RuntimeError(f"ownership mismatch: {name}")
        inspected[name] = found
    failures = []
    remaining = []
    for name in state["containers"]:
        try:
            failures.extend(stop_node(name, inspected[name], state, artifacts, graceful))
        except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired) as error:
            # Retain this node for another collection attempt, but still stop
            # and collect the other owned nodes. Never report missing evidence
            # as a successful sanitizer run.
            failures.append(f"{name}: cleanup incomplete: {error}")
            remaining.append(name)
    state["containers"] = remaining
    return failures


def stop_node(name, found, state, artifacts, graceful):
    failures = []
    if graceful:
        docker("stop", "--time", "30", name)
        found = json.loads(docker("inspect", name))[0]
        if found["State"]["ExitCode"] != 0 or found["State"]["OOMKilled"]:
            failures.append(f"{name}: {found['State']}")
    if artifacts is not None:
        target = artifacts / name
        target.mkdir(parents=True, exist_ok=True)
        (target / "inspect.json").write_text(json.dumps(found, indent=2))
        (target / "stdout.log").write_text(docker("logs", name))
        docker("cp", f"{name}:/opt/crowdb/data/kv/log/.", str(target / "log"))
        if state.get("sanitizer"):
            docker("cp", f"{name}:/opt/crowdb/data/asan/.", str(target / "asan"))
            for report in (target / "asan").glob("report*"):
                failures.append(f"{name}: sanitizer report {report}\n{report.read_text()}")
    docker("rm", "-f", "-v", name)
    return failures


def start(args):
    if not args.image:
        raise ValueError("up requires --image")
    if args.sanitizer:
        image = json.loads(docker("image", "inspect", args.image))[0]
        if image["Config"].get("Labels", {}).get("crowdb.sanitizer") != "address":
            raise ValueError("--sanitizer requires a staged ASan image")
    if args.state.exists():
        raise ValueError("state file exists; stop its deployment first")
    if len(args.offsets) != 3 or len(set(args.offsets)) != 3 or any(o < 0 or o > 999 for o in args.offsets):
        raise ValueError("three distinct offsets in 0..999 required")
    if args.cpuset and len(args.cpuset) != 3:
        raise ValueError("supply one cpuset per node")
    ports = [port + offset for offset in args.offsets for port in [PAXOS_KV_SERVER_MGMT_BASE, PAXOS_KV_SERVER_RPC_BASE]]
    if len(set(ports)) != len(ports):
        raise ValueError("offset port pools overlap")
    for port in ports:
        with socket.socket() as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", port))
    state = {"id":uuid.uuid4().hex,"containers":[],"endpoints":[],"network":"host","offsets":args.offsets,
             "sanitizer":args.sanitizer}
    args.state.parent.mkdir(parents=True, exist_ok=True)
    try:
        for i, offset in enumerate(args.offsets):
            name = f"paxos-kv-{state['id'][:12]}-{i+1}"
            command = ["run", "-d", "--name", name, "--network", "host", "--label", f"{LABEL}={state['id']}",
                       "--pids-limit", "256", "-e", f"TOKIO_WORKER_THREADS={len(os.sched_getaffinity(0))}"]
            if args.memory:
                command += ["--memory", args.memory, "--memory-swap", args.memory]
            if args.cpus is not None:
                command += ["--cpus", str(args.cpus)]
            if args.cpuset:
                command += ["--cpuset-cpus", args.cpuset[i]]
            command += [args.image]
            if not args.sanitizer:
                command += ["server", "paxos-kv"]
            command += ["--root","/opt/crowdb/data/kv",
                        "--management-port",str(PAXOS_KV_SERVER_MGMT_BASE+offset),"--management-addr","0.0.0.0",
                        "--management-advertise-addr","127.0.0.1","--ports",str(PAXOS_KV_SERVER_RPC_BASE+offset),
                        "--stores","0","--groups","1","--replica",str(i+1),
                        "--kv-backend","mem-block","--wal-backend","mem-block","--event-write",
                        "--election-profile", args.election_profile,
                        "--max-inflight",str(args.window),"--coalesce-max-keys",str(args.coalesce),
                        "--peer-pool-size","4","--rpc-workers",str(args.workers),"--log"]
            docker(*command)
            state["containers"].append(name)
            state["endpoints"].append(f"http://127.0.0.1:{PAXOS_KV_SERVER_MGMT_BASE+offset}")
            args.state.write_text(json.dumps(state, indent=2))
        topologies = wait_ready(state["endpoints"])
        stores = []
        for topology, offset in zip(topologies, args.offsets, strict=True):
            store = topology["stores"][0]
            store["listen_addr"] = f"127.0.0.1:{PAXOS_KV_SERVER_RPC_BASE+offset}"
            stores.append(store)
        for origin in state["endpoints"]:
            request(origin,"/stores/0/groups/1/remotes/batch",{"stores":stores})
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            groups = [request(o,"/topology")["stores"][0]["groups"][0] for o in state["endpoints"]]
            leaders = {g["leader_id"] for g in groups}
            if len(leaders) == 1 and next(iter(leaders)) in (1,2,3):
                print(json.dumps(state, indent=2))
                return
            time.sleep(.1)
        raise RuntimeError("leader convergence timeout")
    except BaseException:
        for name in state["containers"]:
            try:
                print(docker("logs", name), file=sys.stderr)
            except RuntimeError as error:
                print(f"Cannot collect logs for {name}: {error}", file=sys.stderr)
        failures = stop(state, args.state.parent / "startup-failure" if args.sanitizer else None, args.sanitizer)
        for failure in failures:
            print(failure, file=sys.stderr)
        if state["containers"]:
            args.state.write_text(json.dumps(state, indent=2))
        else:
            args.state.unlink(missing_ok=True)
        raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=("up","down"))
    p.add_argument("--state",type=Path,required=True)
    p.add_argument("--image")
    p.add_argument("--sanitizer", action="store_true", help="direct-server ASan image, staged by sanitizer tooling")
    p.add_argument("--graceful", action="store_true", help="SIGTERM shutdown; fail on nonzero exit or OOM")
    p.add_argument("--artifacts", type=Path, help="retain shutdown inspect, logs and sanitizer reports")
    p.add_argument("--offsets",type=int,nargs=3,default=[0,1,2])
    p.add_argument("--memory", help="optional per-node memory limit; omitted for unrestricted performance baseline")
    p.add_argument("--cpus",type=float,help="optional per-node CPU quota; omitted for unrestricted performance baseline")
    p.add_argument("--cpuset",action="append",default=[])
    p.add_argument("--workers",type=int,default=2)
    p.add_argument("--window",type=int,default=32)
    p.add_argument("--coalesce",type=int,default=16)
    p.add_argument("--election-profile", choices=("default", "e2e"), default="default")
    args = p.parse_args()
    if args.action == "up":
        start(args)
    else:
        state = json.loads(args.state.read_text())
        if state.get("sanitizer") and (not args.graceful or args.artifacts is None):
            raise ValueError("sanitizer shutdown requires --graceful --artifacts")
        original = dict(state)
        failures = stop(state, args.artifacts, args.graceful)
        args.state.with_suffix(".stopped.json").write_text(json.dumps(
            {"state":original, "failures":failures, "remaining_containers":state["containers"]}, indent=2))
        if state["containers"]:
            args.state.write_text(json.dumps(state, indent=2))
        else:
            args.state.unlink()
        if failures:
            raise RuntimeError("\n".join(failures))


if __name__ == "__main__":
    main()
