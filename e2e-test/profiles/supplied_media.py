# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Private disposable loop/files inside the owned privileged Linux host fixture."""

import errno
import fcntl
import json
import os
from pathlib import Path
import stat
import struct
import subprocess
import uuid
import signal

CAPACITY = 65 * 1024 ** 2 + 512
TAIL_OFFSET = 64 * 1024 ** 2
TAIL = b"owned-unused-physical-tail"
LOOP_SET_FD = 0x4C00
LOOP_CLR_FD = 0x4C01
LOOP_SET_STATUS64 = 0x4C04
LOOP_GET_STATUS64 = 0x4C05
LOOP_CTL_GET_FREE = 0x4C82


class Media:
    def __init__(self, root):
        if os.geteuid() != 0 or not ("CROWDB_CONTAINERD_DATA" in os.environ or "CROWDB_OWNED_MEDIA_ROOT" in os.environ):
            raise RuntimeError("supplied media requires the explicit owned privileged host profile")
        self.root = Path(root)
        self.root.mkdir(mode=0o770)
        group = int(os.environ.get("CROWDB_OWNED_MEDIA_GID", "10001"))
        os.chown(self.root, 10001, group)
        self.root.chmod(0o770)
        self.backing = self.root / "loop-backing.img"
        self.file = self.root / "supplied-file.img"
        self.bad = self.root / "supplied-nonblank.img"
        self.raw = self.root / "supplied-loop"
        self.descriptor = None
        self.bound = False
        for path in (self.backing, self.file, self.bad):
            with path.open("xb") as file:
                file.truncate(CAPACITY)
                file.seek(TAIL_OFFSET)
                file.write(TAIL)
                if path == self.bad:
                    file.seek(TAIL_OFFSET - 1)
                    file.write(b"X")
                file.flush()
                os.fsync(file.fileno())
            os.chown(path, 10001, group)
            path.chmod(0o660)
        observed = self.backing.stat()
        self.identity = (observed.st_dev, observed.st_ino)
        try:
            control = os.open("/dev/loop-control", os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                number = fcntl.ioctl(control, LOOP_CTL_GET_FREE)
            finally:
                os.close(control)
            major, minor = map(int, Path(f"/sys/block/loop{number}/dev").read_text().strip().split(":"))
            os.mknod(self.raw, stat.S_IFBLK | 0o600, os.makedev(major, minor))
            self.descriptor = os.open(self.raw, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
            backing = os.open(self.backing, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                fcntl.ioctl(self.descriptor, LOOP_SET_FD, backing)
                self.bound = True
            finally:
                os.close(backing)
            status = self.status()
            # Pin our exact mapping while making process interruption autoclear it.
            struct.pack_into("I", status, 52, struct.unpack_from("I", status, 52)[0] | 4)
            fcntl.ioctl(self.descriptor, LOOP_SET_STATUS64, status)
            self.status()
            os.chown(self.raw, 10001, group)
            self.raw.chmod(0o660)
            assert self.exclusive_available(), "fresh owned loop is already claimed"
            self.permissions = {path.name: ownership(path) for path in (self.backing, self.file, self.bad, self.raw)}
        except BaseException:
            self.close()
            raise

    def status(self):
        status = bytearray(232)
        fcntl.ioctl(self.descriptor, LOOP_GET_STATUS64, status, True)
        device, inode, raw, offset, limit = struct.unpack_from("QQQQQ", status)
        if (device, inode) != self.identity or raw != 0 or offset != 0 or limit != 0:
            raise RuntimeError("owned loop backing identity changed; refusing mutation/cleanup")
        return status

    def arguments(self):
        return ["--disk-file", str(self.file), "--disk-file", str(self.bad), "--device", str(self.raw)]

    def binding(self, data):
        return json.loads((data / "node-binding.json").read_text())

    def exclusive_available(self):
        self.status()
        try:
            descriptor = os.open(self.raw, os.O_RDWR | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC)
        except OSError as error:
            if error.errno == errno.EBUSY:
                return False
            raise
        os.close(descriptor)
        return True

    def verify_tails(self):
        self.status()
        for path in (self.backing, self.file, self.bad, self.raw):
            assert ownership(path) == self.permissions[path.name], f"external source ownership/mode changed: {path.name}"
        for path in (self.backing, self.file, self.bad):
            with path.open("rb") as file:
                assert os.fstat(file.fileno()).st_size == CAPACITY
                file.seek(TAIL_OFFSET)
                assert file.read(len(TAIL)) == TAIL
                if path == self.bad:
                    file.seek(TAIL_OFFSET - 1)
                    assert file.read(1) == b"X"

    def probe(self, state, mode):
        specification = self.root / f"probe-{mode}-{uuid.uuid4().hex}.json"
        specification.write_text(json.dumps({**state, "mode": mode}))
        env = os.environ.copy()
        client = Path(os.environ.get("CROWDB_MEDIA_CLIENT", "/opt/crowdb-e2e/client"))
        env.update(CROWDB_SUPPLIED_MEDIA_PROBE=str(specification), LD_LIBRARY_PATH=str(client / "lib"))
        subprocess.run([str(client / "domain-test"), "--ignored", "--exact",
                        "deployment::supplied_media::retained_data_probe", "--nocapture"],
                       env=env, check=True, timeout=30)

    def close(self):
        if self.descriptor is None:
            return
        try:
            if self.bound:
                self.status()
                fcntl.ioctl(self.descriptor, LOOP_CLR_FD)
        finally:
            os.close(self.descriptor)
            self.descriptor = None


def provision(media, data, node, cluster, request, wait):
    request("/api/stores", {"store_id": 41, "nodes": [node]})
    request("/api/stores/41/groups", {"group_id": 1, "replica_id": 41, "nodes": [node]})
    request(f"/api/nodes/{node}/disk-groups", {"id": 1, "name": "Supplied media", "store_id": 41, "group_id": 1})
    request(f"/api/nodes/{node}/diskdb/deploy", {"rpc_port": 7100, "keepalive_interval_secs": 1})
    inventory = request(f"/api/nodes/{node}/disk-inventory")
    operations = []
    for name in ("supplied-file.img", "supplied-loop", "supplied-nonblank.img"):
        disk = next(disk for disk in inventory["disks"] if disk["path"] == f"/opt/crowdb/data/disks/{name}")
        assert disk["supplied"] and disk["error"] is None and disk["capacity_bytes"] == CAPACITY, disk
        medium = {key: disk[key] for key in ("path", "backend", "backing_identity", "resource_identity", "capacity_bytes")}
        medium.update(media_type=None, initialize=True)
        accepted = request(f"/api/nodes/{node}/disk-candidates", {
            "operation_id": str(uuid.uuid4()), "disk_group_id": 1, "medium": medium})
        assert accepted["medium"] == medium and accepted["disk_group_id"] == 1, accepted
        operations.append(accepted)

    def outcomes():
        return [request(f"/api/nodes/{node}/disk-candidates/{row['operation_id']}") for row in operations]

    waiting = wait(outcomes, lambda rows: [row["phase"] for row in rows] == ["waiting-for-diskio", "waiting-for-diskio", "failed"],
                   "supplied media waits for actual DiskIO and rejects nonblank bytes", seconds=10)
    assert "not blank" in waiting[2]["error"], waiting
    defaults = request(f"/api/deployment-defaults?node_id={node}")["diskio"]
    defaults.update(kind="diskio", instance_id=str(node), disk_group_id=1, rpc_port=7200, local_deployment=False)
    request(f"/api/nodes/{node}/services/deploy", defaults)
    completed = wait(outcomes, lambda rows: [row["phase"] for row in rows] == ["registered", "registered", "failed"],
                     "real file/raw descriptor verification", seconds=10)
    assert not media.exclusive_available(), "DiskIO did not retain its exclusive raw-device claim"
    binding = media.binding(data)
    assert binding["bootstrap"]["cluster_id"] == cluster and binding["node_id"] == node
    state = {"version": 1, "cluster_id": cluster, "rack_id": 1, "node_id": node, "disk_group_id": 1,
             "physical_host_id": os.environ.get("CROWDB_MEDIA_HOST_ID", "containerd-acceptance-host"),
             "seeds": binding["management_seeds"], "disks": completed[:2]}
    media.probe(state, "write")
    media.verify_tails()
    (media.root / "accepted-media.json").write_text(json.dumps({"operations": completed, "cluster_id": cluster}, indent=2))
    return state


def remove_and_rejoin(media, state, runtime, request, wait):
    raw = state["disks"][1]
    path = f"/api/nodes/{state['node_id']}/disk-groups/1/disks/{raw['disk_id']}"
    original = request(path + "/lifecycle")
    request(path + "/offline", {"expected_revision": original["disk_revision"]})
    excluded = wait(lambda: request(path + "/lifecycle"),
                    lambda row: row["offline"] and not row["remove_conditions"], "raw owner/DiskIO quiescence", seconds=10)
    assert media.exclusive_available(), "Offline retained an active raw-device claim"
    removed = request(path + "/removal", {"expected_revision": excluded["disk_revision"]})
    assert removed["removed"]
    restored = request(path + "/rejoin", {"removal_operation_id": removed["operation_id"]})["state"]
    assert restored["offline"] and not restored["removed"] and restored["disk_id"] == raw["disk_id"]
    request(path + "/online", {"expected_revision": restored["disk_revision"]})
    wait(lambda: request(path + "/lifecycle"),
         lambda row: not row["offline"] and row["online"] and row["online"]["state"] == "complete", "retained raw Online", seconds=10)
    wait_for_descriptors(media, state, runtime, request, wait)
    assert not media.exclusive_available(), "Online did not reclaim its original raw descriptor"
    media.probe(state, "verify")
    media.verify_tails()


def wait_for_descriptors(media, state, runtime, request, wait):
    services = request("/api/servers")
    service = next(row for row in services if row["service_type"] == "diskio" and row["node_id"] == state["node_id"])
    pid = service["pid"]
    assert type(pid) is int and pid > 1
    raw = media.raw.stat().st_rdev
    file = media.file.stat()
    expected = {f"B:{os.major(raw):x}:{os.minor(raw):x}", f"F:{file.st_dev}:{file.st_ino}"}
    script = ('count=0; for descriptor in /proc/"$1"/fd/*; do '
              'count=$((count + 1)); test "$count" -le 4096 || exit 1; '
              'if test -b "$descriptor"; then stat -L --format=B:%t:%T "$descriptor" || exit 1; '
              'elif test -f "$descriptor"; then stat -L --format=F:%d:%i "$descriptor" || exit 1; fi; done')
    wait(lambda: set(runtime.invoke("exec", "--user", "10001:10001", "crowdb-node", "sh", "-c", script, "sh", str(pid)).splitlines()),
         lambda observed: expected.issubset(observed), "actual retained File/raw DiskIO descriptors", seconds=3)


def serve_owned_media():
    root = Path(os.environ["CROWDB_OWNED_MEDIA_ROOT"])
    stopped = False

    def stop(_signal, _frame):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    media = Media(root / "supplied-media")
    try:
        (root / "media-ready.json").write_text(json.dumps({"identity": media.identity, "permissions": media.permissions}))
        while not stopped:
            signal.pause()
    finally:
        media.close()
        (root / "media-closed.json").write_text(json.dumps({"identity": media.identity, "permissions": media.permissions}))


def ownership(path):
    metadata = path.stat()
    return [metadata.st_uid, metadata.st_gid, stat.S_IMODE(metadata.st_mode)]


if __name__ == "__main__":
    serve_owned_media()
