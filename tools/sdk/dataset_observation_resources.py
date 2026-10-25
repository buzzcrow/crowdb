# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Read-only owned cgroup/proc snapshots and explicitly scoped counter deltas."""

import json
from pathlib import Path
import time


def _integers(text):
    result = {}
    for line in text.splitlines():
        name, value = line.split()
        number = int(value)
        if number < 0 or name in result:
            raise ValueError("negative or duplicate counter")
        result[name] = number
    return result


def _process(proc, pid, clock):
    result = {"pid": int(pid), "started_monotonic_ns": clock()}
    try:
        stat = (proc / str(pid) / "stat").read_text()
        result["raw_stat"] = stat
        fields = stat.rsplit(") ", 1)[1].split()
        result.update(starttime=int(fields[19]), cpu_ticks=int(fields[11]) + int(fields[12]))
        status = (proc / str(pid) / "status").read_text()
        result["raw_status"] = status
        for name, key in (("VmRSS", "rss_kib"), ("VmHWM", "lifetime_peak_rss_kib")):
            values = [line.split()[1:] for line in status.splitlines() if line.startswith(name + ":")]
            if len(values) != 1 or len(values[0]) != 2 or values[0][1] != "kB":
                result.setdefault("unavailable", {})[key] = "missing_or_invalid_proc_field"
            else:
                result[key] = int(values[0][0])
    except (OSError, ValueError, IndexError) as error:
        result["unavailable"] = {"process": f"{type(error).__name__}: {error}"}
    result["finished_monotonic_ns"] = clock()
    return result


class OwnedSources:
    """Resolve owned cgroups once; read only local files at trial boundaries."""

    def __init__(self, nodes, proc=Path("/proc"), groups=Path("/sys/fs/cgroup"),
                 clock=time.monotonic_ns):
        self.nodes, self.proc, self.groups, self.clock = nodes, proc, groups, clock

    @classmethod
    def discover(cls, docker, names, run_id, image_id, data_paths, **options):
        if len(names) != len(data_paths):
            raise ValueError("owned container and data-mount counts differ")
        source = cls([], **options)
        for index, name in enumerate(names):
            record = json.loads(docker("inspect", name))[0]
            mounted = any(mount.get("Source") == str(data_paths[index])
                          and mount.get("Destination") == "/opt/crowdb/data"
                          for mount in record.get("Mounts", []))
            if (record["Config"].get("Labels", {}).get("org.crowdb.e2e.run") != run_id
                    or record["Image"] != image_id or record["Name"] != "/" + name
                    or not record["Id"] or not record["State"]["Running"] or not mounted):
                raise ValueError("container ownership, image, mount or running state changed: " + name)
            pid = int(record["State"]["Pid"])
            if pid <= 0:
                raise ValueError("invalid owned init PID")
            lines = (source.proc / str(pid) / "cgroup").read_text().splitlines()
            relative = next((line.removeprefix("0::") for line in lines if line.startswith("0::")), None)
            if relative is None or not relative.startswith("/") or relative == "/" or ".." in Path(relative).parts:
                raise ValueError("invalid owned cgroup")
            process = _process(source.proc, pid, source.clock)
            if "starttime" not in process:
                raise ValueError("cannot establish owned init process identity: " + name)
            source.nodes.append({"name": name, "node": index, "container_id": record["Id"],
                                 "image_id": image_id, "init_pid": pid,
                                 "init_starttime": process["starttime"], "cgroup": relative,
                                 "data_mount": str(data_paths[index])})
        return source

    def capture(self, client_pid):
        result = {"started_monotonic_ns": self.clock(), "started_unix_ns": str(time.time_ns()), "nodes": {}}
        for identity in self.nodes:
            node = {"identity": identity, "started_monotonic_ns": self.clock()}
            root = self.groups / identity["cgroup"].lstrip("/")
            for name in ("cpu.stat", "memory.current", "memory.peak", "memory.stat", "memory.events"):
                try:
                    raw = (root / name).read_text()
                    node[name] = {"raw": raw}
                    if name in ("cpu.stat", "memory.stat", "memory.events"):
                        node[name]["values"] = _integers(raw)
                    else:
                        node[name]["bytes"] = int(raw)
                except (OSError, ValueError) as error:
                    node.setdefault("unavailable", {})[name] = f"{type(error).__name__}: {error}"
            try:
                processes = (root / "cgroup.procs").read_text().split()
                if len(processes) > 256 or any(not pid.isdecimal() for pid in processes):
                    raise ValueError("owned cgroup exceeds bounded process observation")
                node["processes"] = [_process(self.proc, pid, self.clock) for pid in processes]
                node["init"] = _process(self.proc, identity["init_pid"], self.clock)
                membership = (self.proc / str(identity["init_pid"]) / "cgroup").read_text()
                node["raw_init_cgroup"] = membership
                if ("0::" + identity["cgroup"]) not in membership.splitlines() or node["init"].get(
                        "starttime") != identity["init_starttime"]:
                    node["identity_error"] = "owned_init_identity_or_cgroup_changed"
            except (OSError, ValueError) as error:
                node.setdefault("unavailable", {})["processes"] = f"{type(error).__name__}: {error}"
                node["identity_error"] = "cannot_verify_owned_membership"
            node["finished_monotonic_ns"] = self.clock()
            result["nodes"][identity["name"]] = node
        result["client"] = _process(self.proc, client_pid, self.clock)
        result["finished_monotonic_ns"] = self.clock()
        result["finished_unix_ns"] = str(time.time_ns())
        return result


def _delta(before, after, names):
    result = {"values": {}, "unavailable": {}}
    for name in names:
        first, last = before.get(name), after.get(name)
        if type(first) is not int or type(last) is not int or first < 0 or last < first:
            result["unavailable"][name] = "missing_invalid_or_reset"
        else:
            result["values"][name] = last - first
    return result


def resource_delta(before, after):
    result = {"scope": "enclosing_foreground_and_background_not_atomic", "nodes": {}}
    for name in before["nodes"].keys() | after["nodes"].keys():
        first, last = before["nodes"].get(name, {}), after["nodes"].get(name, {})
        identity = (first.get("identity"), first.get("init", {}).get("starttime"))
        current = (last.get("identity"), last.get("init", {}).get("starttime"))
        if identity != current or identity[1] is None or first.get("identity_error") or last.get("identity_error"):
            result["nodes"][name] = {"unavailable": "identity_missing_or_changed"}
        else:
            result["nodes"][name] = _delta(first.get("cpu.stat", {}).get("values", {}),
                                          last.get("cpu.stat", {}).get("values", {}),
                                          ("usage_usec", "user_usec", "system_usec"))
    first, last = before["client"], after["client"]
    if first.get("starttime") is None or (first.get("pid"), first.get("starttime")) != (
            last.get("pid"), last.get("starttime")):
        result["client"] = {"unavailable": "identity_missing_or_changed"}
    else:
        result["client"] = _delta(first, last, ("cpu_ticks",))
    return result


def local_delta(before, after):
    """Selected transport and shared-reader counters, never process RUSAGE sums."""
    result = {"scope": "selected_local_transport_and_shared_reader_cumulative", "groups": {}}
    for group in ("metadata", "aggregate", "data"):
        def selected(value):
            topology = value.get("topology") or {}
            metadata = topology.get("metadata") or {}
            selected_group = metadata.get(group) if isinstance(metadata, dict) else None
            return topology.get("data") if group == "data" else (
                selected_group.get("transport") if isinstance(selected_group, dict) else None)
        first, last = selected(before), selected(after)
        if not isinstance(first, dict) or not isinstance(last, dict):
            result["groups"][group] = {"unavailable": "not_exposed_or_capture_failed"}
            continue
        keys = ("schema_version", "process_id", "io_engines", "io_workers", "send_queue_capacity",
                "tcp_nodelay", "tcp_quickack", "event_write", "allow_tcp_fallback", "rdma_compiled")
        if (type(first.get("schema_version")) is not int or first.get("schema_version") != 1
                or type(last.get("schema_version")) is not int or type(first.get("process_id")) is not int
                or type(last.get("process_id")) is not int or first["process_id"] <= 0) or any(
                key not in first or key not in last for key in keys) or any(
                first.get(key) != last.get(key) for key in keys):
            result["groups"][group] = {"unavailable": "schema_process_or_configuration_changed"}
            continue
        counters = _delta(first, last, ("rpc_frames_sent", "wire_bytes_sent", "writev_calls",
                                       "writev_progress_calls", "send_queue_rejects"))
        counters["not_measured"] = ["receive_frames", "receive_bytes", "all_native_rpc"]
        counters["rdma"] = _rdma_delta(first.get("rdma"), last.get("rdma"))
        result["groups"][group] = counters
    result["resources"] = _delta(before.get("resources") or {}, after.get("resources") or {},
                                  ("compatibility_copy_bytes",))
    resources_before, resources_after = before.get("resources") or {}, after.get("resources") or {}
    gauges = ("requests", "producer_bytes", "producer_bytes_high_water", "held_bytes", "held_bytes_high_water")
    result["resource_observations"] = {
        "scope": "current_gauges_and_lifetime_high_water_not_trial_peak",
        "before": {key: _gauge(resources_before, key) for key in gauges},
        "after": {key: _gauge(resources_after, key) for key in gauges},
    }
    first, last = resources_before.get("read_flow"), resources_after.get("read_flow")
    result["read_flow"] = ({"unavailable": "not_exposed_or_capture_failed"}
                           if not isinstance(first, dict) or not isinstance(last, dict) else
                           {"scope": "shared_reader_cumulative_not_per_request",
                            **_delta(first, last, READ_FLOW_COUNTERS)})
    result["not_measured"] = ["all_copy_bytes", "per_trial_held_peak", "held_wait_ns",
                              "tcp_receive_frames_bytes", "other_control_transports"]
    return result


READ_FLOW_COUNTERS = (
    "location_normalizations", "locations_examined", "range_locations_examined", "stream_windows",
    "stream_units_completed", "stream_out_of_order", "stream_credit_stalls", "stream_credit_wait_ns",
    "stream_bytes_reserved", "stream_bytes_released", "layout_queries", "layout_query_wait_ns",
    "strip_read_wait_ns", "chunk_read_wait_ns", "frame_decode_wait_ns", "frame_parse_wait_ns",
)
RDMA_COUNTERS = (
    "rpc_frames_sent", "rpc_frames_received", "wire_bytes_sent", "wire_bytes_received", "direct_sends",
    "direct_payload_bytes", "copied_payload_bytes", "copied_aggregate_payload_bytes",
    "copied_staging_payload_bytes", "copied_framing_control_bytes", "control_fragments_sent",
    "control_fragments_received", "copied_control_send_bytes", "copied_control_receive_bytes",
    "control_send_stalls", "control_receive_stalls",
)


def _gauge(source, key):
    value = source.get(key)
    return ({"value": value} if type(value) is int and value >= 0 else
            {"unavailable": "missing_or_invalid_gauge"})


def _rdma_delta(first, last):
    if not isinstance(first, dict) or not isinstance(last, dict):
        return {"unavailable": "rdma_not_active_or_not_exposed"}
    identity = ("device", "gid", "generation", "protocol_version", "wire_layout")
    if (first.get("ready") is not True or last.get("ready") is not True
            or any(type(record.get(key)) is not str or not record[key]
                   for record in (first, last) for key in ("device", "gid", "wire_layout"))
            or any(type(record.get(key)) is not int or record[key] < 0
                   for record in (first, last) for key in ("generation", "protocol_version"))
            or any(key not in first or key not in last or first[key] != last[key] for key in identity)):
        return {"unavailable": "rdma_not_ready_or_identity_changed"}
    return {"scope": "selected_rdma_transport_only_not_all_runtime_traffic",
            **_delta(first, last, RDMA_COUNTERS)}
