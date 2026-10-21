# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Scoped Docker stop and cleanup; retained volumes and receipts are the default."""

import json

from docker_local_state import RECORDS, check_daemon, load, record_path, require_owned


def selected(cluster, all_clusters, directory=RECORDS):
    if all_clusters:
        if directory.is_symlink():
            raise ValueError("cluster receipt directory must not be a symlink")
        return [load(path.stem, directory) for path in sorted(directory.glob("*.json"))]
    record = load(cluster, directory)
    if record is None:
        raise ValueError("cluster has no local Docker receipt; use Docker tools for direct containers")
    return [record]


def current(docker, record, kind, identity):
    item = json.loads(docker.call(kind, "inspect", identity))[0]
    require_owned(item, record, kind)
    check_daemon(docker, record)
    return item


def stop(docker, record):
    check_daemon(docker, record)
    for item in docker.owned(record, "container"):
        item = current(docker, record, "container", item["Id"])
        if item["State"]["Running"]:
            docker.call("stop", "--time", "20", item["Id"])


def clean(docker, record, delete_data=False, directory=RECORDS):
    stop(docker, record)
    for item in docker.owned(record, "container"):
        item = current(docker, record, "container", item["Id"])
        if item["State"]["Running"]:
            raise ValueError("container restarted during clean; retry after stopping its owner")
        docker.call("rm", item["Id"])
    for item in docker.owned(record, "network"):
        item = current(docker, record, "network", item["Id"])
        if item.get("Containers"):
            raise ValueError("owned bridge still has attached containers; refusing network deletion")
        docker.call("network", "rm", item["Id"])
    if delete_data:
        for item in docker.owned(record, "volume"):
            item = current(docker, record, "volume", item["Name"])
            if docker.call("ps", "-aq", "--filter", "volume=" + item["Name"]):
                raise ValueError("owned volume is referenced by another container")
            docker.call("volume", "rm", item["Name"])
        record_path(record["cluster"], directory).unlink()
    print(f"Cleaned containers and bridges for {record['cluster']}; "
          + ("deleted owned data volumes" if delete_data else "preserved data volumes and restart inputs"))


def image_garbage(docker, directory=RECORDS, runtime=None):
    # Only dangling CROWDB images are garbage. Receipts retain image identity
    # even after their last container is cleaned; a stopped foreign user also
    # protects an image. Docker's non-force removal handles reference races.
    runtime = docker.identity() if runtime is None else runtime
    if docker.identity() != runtime:
        raise ValueError("Docker endpoint/context changed before image cleanup")
    retained = {record["image_id"] for record in selected(None, True, directory)}
    candidates = docker.call("image", "ls", "-q", "--filter", "dangling=true", "--filter",
                             "label=org.opencontainers.image.source=https://github.com/buzzcrow/crowdb").splitlines()
    for identity in candidates:
        item = json.loads(docker.call("image", "inspect", identity))[0]
        if item.get("Config", {}).get("Labels", {}).get("org.opencontainers.image.source") != "https://github.com/buzzcrow/crowdb":
            continue
        if item["Id"] in retained or docker.call("ps", "-aq", "--filter", "ancestor=" + item["Id"]):
            continue
        if item.get("RepoTags") or item.get("RepoDigests"):
            continue
        if docker.identity() != runtime:
            raise ValueError("Docker endpoint/context changed during image cleanup")
        docker.call("image", "rm", item["Id"])
