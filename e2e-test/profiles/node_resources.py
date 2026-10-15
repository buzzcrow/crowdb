# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Read only the owned container's cgroup and process resource counters."""

import json
from pathlib import Path


def capture(docker, names, destination):
    result = {}
    for name in names:
        try:
            state = json.loads(docker("inspect", "--format", "{{json .State}}", name))
            pid = int(state["Pid"])
            node = {key: state.get(key) for key in ("Pid", "OOMKilled", "ExitCode", "Error")}
            result[name] = node
            if not pid:
                continue
            cgroups = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
            relative = next(line.removeprefix("0::") for line in cgroups if line.startswith("0::"))
            if not relative.startswith("/") or ".." in Path(relative).parts:
                raise ValueError("invalid owned process cgroup")
            root = Path("/sys/fs/cgroup") / relative.lstrip("/")
            node["cgroup"] = relative
            for field in ("pids.current", "pids.max", "pids.peak", "pids.events", "memory.current", "memory.max", "memory.events", "cpu.stat"):
                try:
                    node[field] = (root / field).read_text().strip()
                except OSError as error:
                    node[field] = {"read_error": str(error)}
            processes = []
            for process in (root / "cgroup.procs").read_text().splitlines():
                if not process.isdecimal():
                    raise ValueError("invalid owned cgroup process")
                try:
                    fields = dict(line.split(":", 1) for line in Path(f"/proc/{process}/status").read_text().splitlines() if ":" in line)
                    processes.append({key: fields[key].strip() for key in ("Name", "Pid", "Threads")})
                except OSError as error:
                    processes.append({"Pid": process, "read_error": str(error)})
            node["processes"] = sorted(processes, key=lambda row: int(row.get("Threads", 0)), reverse=True)[:32]
        except (OSError, ValueError, RuntimeError, KeyError, StopIteration) as error:
            result.setdefault(name, {})["capture_error"] = str(error)
    destination.write_text(json.dumps(result, indent=2))
    print("Owned node resource diagnostics: " + json.dumps(result), flush=True)
    return result


def verify_reaping(invoke, name, wait):
    """Create actual orphan children and require the image init to collect them."""
    leader = invoke("exec", name, "cat", "/proc/1/comm").strip()
    assert leader == "tini", f"OCI PID 1 must reap orphaned processes, got {leader}"
    pid = int(json.loads(invoke("inspect", name))[0]["State"]["Pid"])
    invoke("exec", name, "sh", "-c",
           'i=0; while [ "$i" -lt 64 ]; do sh -c "sleep 0.05 &"; i=$((i+1)); done')

    def children():
        pending = []
        for child in Path(f"/proc/{pid}/task/{pid}/children").read_text().split():
            try:
                fields = dict(line.split(":", 1) for line in Path(f"/proc/{child}/status").read_text().splitlines() if ":" in line)
            except FileNotFoundError:
                continue
            if fields["Name"].strip() == "sleep" or fields["State"].strip().startswith("Z"):
                pending.append({key: fields[key].strip() for key in ("Name", "Pid", "State")})
        return pending

    wait(children, lambda pending: not pending, "OCI init collects exited orphan children", seconds=3)
    print("OCI init reaped 64 orphan children", flush=True)


def verify_stopped(invoke, name):
    state = json.loads(invoke("inspect", name))[0]["State"]
    assert state["ExitCode"] == 0 and not state.get("OOMKilled", False), state
