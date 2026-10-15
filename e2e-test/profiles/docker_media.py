# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Own one loop mapping in an isolated helper without granting the node privileges."""

import json
import os
from pathlib import Path
import shutil

from node import docker, wait
from supplied_media import Media

ROOT = Path(__file__).resolve().parents[2]


class DockerMedia(Media):
    def __init__(self, root, name, image):
        self.root = root / "supplied-media"
        self.backing = self.root / "loop-backing.img"
        self.file = self.root / "supplied-file.img"
        self.bad = self.root / "supplied-nonblank.img"
        self.raw = self.root / "supplied-loop"
        self.name = name + "-media"
        self.image = image
        self.owner = name
        self.host_root = root
        self.descriptor = None
        pixi = Path(shutil.which("pixi")).resolve()
        try:
            docker("run", "-d", "--init", "--name", self.name, "--label", f"org.crowdb.e2e.run={name}",
                   "--network", "none", "--privileged", "--entrypoint", "/bin/bash",
                   "--mount", f"type=bind,source={root},target=/owned",
                   "--mount", f"type=bind,source={ROOT},target={ROOT},readonly",
                   "--mount", f"type=bind,source={pixi},target=/usr/local/bin/pixi,readonly",
                   "--env", "CROWDB_OWNED_MEDIA_ROOT=/owned", "--env", f"CROWDB_OWNED_MEDIA_GID={os.getgid()}",
                   "--workdir", str(ROOT), image, "-euc",
                   'exec pixi run --as-is --manifest-path container/oci-image/pixi.toml -e runtime '
                   'python e2e-test/profiles/supplied_media.py')
            ready = root / "media-ready.json"
            wait(ready.exists, bool, "owned Docker loop helper readiness")
            observed = json.loads(ready.read_text())
            self.identity = tuple(observed["identity"])
            self.permissions = observed["permissions"]
            self.descriptor = os.open(self.raw, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            self.status()
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.descriptor is not None:
            os.close(self.descriptor)
            self.descriptor = None
        cleanup_owned_helper(self.name, self.host_root, self.image, self.owner)

    def binding(self, data):
        assert data == self.host_root / "node"
        return json.loads(docker("exec", "--user", "10001:10001", self.owner, "cat", "/opt/crowdb/data/node-binding.json"))


def cleanup_owned_helper(name, root, image, owner):
    names = docker("container", "ls", "-a", "--filter", f"name={name}", "--format", "{{.Names}}")
    if name not in names.splitlines():
        return
    current = json.loads(docker("inspect", name))[0]
    if (current["Image"] != image or current["Config"].get("Labels", {}).get("org.crowdb.e2e.run") != owner
            or not any(mount.get("Source") == str(root) and mount.get("Destination") == "/owned"
                       for mount in current.get("Mounts", []))):
        raise RuntimeError("media cleanup refuses changed image, owner or data mount")
    try:
        docker("stop", "--time", "10", name)
        stopped = json.loads(docker("inspect", name))[0]
        logs = docker("logs", name)
        if logs:
            print(logs, flush=True)
        assert stopped["State"]["ExitCode"] == 0, stopped["State"]
        ready = root / "media-ready.json"
        if ready.exists():
            assert json.loads((root / "media-closed.json").read_text()) == json.loads(ready.read_text())
    finally:
        docker("rm", "-f", name)


class NodeRuntime:
    def __init__(self, name):
        self.name = name

    def invoke(self, *args):
        return docker(*(self.name if argument == "crowdb-node" else argument for argument in args))
