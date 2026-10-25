# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Production-policy release nodes with explicit admission and fixed ports."""

import json
import urllib.request
import urllib.error
from urllib.parse import urlparse
from allocation import LABEL, create_network, node_data, start_node
from fixture import Cluster, HTTP, wait
from console_shell_fixture import ShellBrowserCluster
from console_native_observation import services, process, retained_marker, disk_backing, BrowserObservations
from control import Control
from console_native_browser import INSPECTION_CASES
from console_native_storage import unbound_file_rejected, verify_file_preparation, verify_file_admission
from console_native_candidates import prepare as prepare_candidates, verify as verify_candidates
from profiles.setup_choices import diskless_setup
from profiles.node_resources import capture as capture_resources
from console_native_policy import accept as accept_policy


class NativeConsoleCluster(ShellBrowserCluster):
    def __init__(self, image, artifacts, bundle, *, disk_gib=(80, 8, 8),
                 zone_gib=(1, 1, 1), spare_disk_gib=0, bootstrap=True, rack_ids=None,
                 admission_count=None, diskless_nodes=(), auto_bootstrap=False, supplied_candidates=False, **kwargs):
        if type(supplied_candidates) is not bool:
            raise ValueError("supplied candidate selection must be boolean")
        if type(bootstrap) is not bool:
            raise ValueError("native bootstrap selection must be a boolean")
        count = len(disk_gib)
        if (not 3 <= count <= 8 or any(type(size) is not int or not 1 <= size <= 4096
                                     for size in disk_gib)):
            raise ValueError("native disk capacities require three to eight bounded GiB values")
        if (len(zone_gib) != count or any(type(size) is not int or not 1 <= size <= disk
                                    for size, disk in zip(zone_gib, disk_gib))):
            raise ValueError("native zone sizes must match the bounded disk capacities")
        if type(spare_disk_gib) is not int or not 0 <= spare_disk_gib <= 4096:
            raise ValueError("native spare disk requires a bounded GiB value")
        racks = (1,) * count if rack_ids is None else tuple(rack_ids)
        if len(racks) != count or any(type(rack) is not int or not 1 <= rack <= 8 for rack in racks):
            raise ValueError("native rack IDs must match the node profile")
        admitted = count if admission_count is None else admission_count
        if type(admitted) is not int or not 3 <= admitted <= count:
            raise ValueError("native initial admission must include three to all nodes")
        if any(type(node) is not int or not 1 <= node <= count for node in diskless_nodes):
            raise ValueError("diskless node is outside the native profile")
        Cluster.__init__(self, image, artifacts, node_count=count, wire_replicas=False,
                         advertise=True, **kwargs)
        # Native storage exercises the real io_uring-backed DiskIO path. Docker's
        # default seccomp profile blocks io_uring_setup even when the host supports it.
        self.security_options = ("seccomp=unconfined",)
        self.bundle = bundle
        self.node_ids = []
        self.rejected = False
        self.disk_gib = tuple(disk_gib)
        self.zone_gib = tuple(zone_gib)
        self.spare_disk_gib = spare_disk_gib
        self.supplied_candidates = supplied_candidates
        self.candidate_inputs = []
        self.bootstrap = bootstrap
        self.auto_bootstrap = auto_bootstrap
        self.rack_ids = racks
        self.admission_count = admitted
        self.diskless_nodes = tuple(diskless_nodes)
        self.pending_candidates = []

    def api(self, path, body=None, method=None, status=200):
        payload = None if body is None else json.dumps(body).encode()
        origin = getattr(self, "management_address", self.addresses[0])
        request = urllib.request.Request(f"http://{origin}:9090" + path,
            data=payload, method=method, headers={"Content-Type": "application/json"})
        try:
            with HTTP.open(request, timeout=20) as response:
                content = response.read().decode()
                if response.status != status:
                    raise AssertionError(f"{request.get_method()} {path}: {response.status}: {content}")
                return json.loads(content) if content else None
        except urllib.error.HTTPError as error:
            raise AssertionError(f"{request.get_method()} {path}: {error.code}: {error.read().decode()}") from error

    def start(self):
        subnet = create_network(self)
        host = json.loads(self.command("info", "--format", "{{json .}}"))["ID"]
        if not isinstance(host, str) or not host:
            raise AssertionError("Docker physical host identity is absent")
        for index in range(self.node_count):
            data = node_data(self, index, prepare_logs=False)
            environment, mounts = accept_policy(self, index, data, host)
            disks = data / "disks"
            disks.mkdir()
            disk = disks / f"disk-{index + 1}.img"
            if index + 1 not in self.diskless_nodes:
                with disk.open("wb") as file:
                    file.truncate(self.disk_gib[index] * 1024 ** 3)
            if index == 0 and self.spare_disk_gib:
                with (disks / "capacity-spare.img").open("wb") as file:
                    file.truncate(self.spare_disk_gib * 1024 ** 3)
            if index == 0 and self.supplied_candidates:
                self.candidate_inputs = prepare_candidates(data)
            name = f"{self.run_id}-prepare{index}"
            self.containers.append(name)
            self.command("run", "--rm", "--name", name, "--label", f"{LABEL}={self.run_id}",
                "--network", "none", "--user", "0", "--entrypoint", "/bin/chown",
                "--cpu-shares", "1024", "--env", "TOKIO_WORKER_THREADS=1", "--memory", "64m",
                "--mount", f"type=bind,source={data},target=/opt/crowdb/data", self.image,
                "-R", "crowdb:crowdb", "/opt/crowdb/data")
            self.addresses.append(start_node(self, index, data, (), memory="2048m", pids_limit=512,
                address=str(subnet.network_address + index + 2),
                mounts=mounts, environment=(*environment,
                    "CROWDB_DISK_INPUTS=" + json.dumps(self.candidate_inputs if index == 0 else []))))
        self.ready()
        for address in self.addresses:
            with HTTP.open(f"http://{address}:9090/api/node/auto-join", timeout=5) as response:
                assert json.load(response)["enabled"] is False
            request = urllib.request.Request(f"http://{address}:9090/api/node/auto-join",
                data=json.dumps({"enabled": False}).encode(),
                headers={"Content-Type": "application/json"})
            with HTTP.open(request, timeout=5) as response:
                assert json.load(response)["enabled"] is False
        if self.auto_bootstrap:
            discovered = wait(lambda: self.api("/api/node/candidates"),
                lambda result: len(result["candidates"]) == self.node_count,
                "complete automatic formation", seconds=10)["candidates"]
            coordinator = min(discovered, key=lambda entry: entry["advertisement"]["discovery_id"])
            self.management_address = next(
                urlparse(endpoint).hostname
                for endpoint in coordinator["advertisement"]["monitor_endpoints"]
                if urlparse(endpoint).hostname in self.addresses)
        for rack in sorted(set(self.rack_ids)):
            self.api("/api/racks", {"id": rack, "name": "native acceptance"}, status=201)
        if self.auto_bootstrap:
            unbound_file_rejected(self)
        candidates = wait(lambda: self.api("/api/node/candidates"),
                          lambda result: len(result["candidates"]) == self.node_count,
                          "isolated manual node candidates", seconds=10)["candidates"]
        for index, address in enumerate(self.addresses):
            candidate = next(entry for entry in candidates if any(
                urlparse(endpoint).hostname == address
                for endpoint in entry["advertisement"]["monitor_endpoints"]))
            if index >= self.admission_count:
                self.pending_candidates.append({"node_id": index + 1,
                    "discovery_id": candidate["advertisement"]["discovery_id"]})
                continue
            record = self.api("/api/node/admit", {
                "discovery_id": candidate["advertisement"]["discovery_id"],
                "rack_id": self.rack_ids[index], "ssh_user": "crowdb", "ssh_port": 2222, "ssh_password": "crowdb",
                "setup": diskless_setup()})
            self.node_ids.append(record["node_id"])
        if self.bootstrap:
            if self.auto_bootstrap:
                self.api("/api/node/auto-join", {"enabled": True})
            else:
                self.api("/api/cluster/init", {"nodes": self.node_ids}, status=202)
            wait(self.initialization_status, lambda result: result["node"]["phase"] == "active"
                 and result["operation"]["phase"] == "active",
                 "public Group 0 bootstrap", seconds=10)
            wait(lambda: self.api("/api/racks?recursive=1"),
                 lambda result: {node["id"] for rack in result["items"] for node in rack["nodes"]
                     if node.get("kv_server", {}).get("health") == "up"} == set(self.node_ids),
                 "live remote KV observations", seconds=10)
            if self.auto_bootstrap:
                self.verify_auto_bootstrap()
                verify_file_preparation(self)
        self.origins = [f"http://{address}:10000" for address in self.addresses]
        (self.artifacts / "native-endpoints.json").write_text(json.dumps({
            "nodes": self.node_ids, "addresses": self.addresses, "disk_gib": self.disk_gib,
            "zone_gib": self.zone_gib, "spare_disk_gib": self.spare_disk_gib, "racks": self.rack_ids, "ports": {
                "web": 9090, "ssh": 2222, "kv": 10000, "diskdb": 7100, "diskio": 7200}}, indent=2))
        return self

    def verify_auto_bootstrap(self):
        admissions = self.api("/api/node/admissions")
        ordered = sorted(admissions, key=lambda node: node["discovery_id"])
        operation = self.api("/api/cluster/initialization")
        assert operation["members"] == [node["node_id"] for node in ordered[:3]], operation
        initiator_address = ordered[0]["host"]
        initiator = self.servers[self.addresses.index(initiator_address)]
        accepted = json.loads(self.command("exec", initiator, "cat", "/opt/crowdb/data/initialization.json"))
        assert accepted["operation"]["identity"]["operation_id"] == operation["operation_id"]
        assert accepted["create_data_group"] is False, accepted
        for address in self.addresses:
            with HTTP.open(f"http://{address}:10000/stores/0/groups/0/ready", timeout=5) as response:
                readiness = json.load(response)
                assert readiness["ready"] and readiness["voting_replicas"] == 3, readiness
        self.api("/api/node/auto-join", {"enabled": False})
        assert self.api("/api/cluster/initialization")["operation_id"] == operation["operation_id"]

    def initialization_status(self):
        operation = self.api("/api/cluster/initialization")
        if operation["phase"] == "failed":
            raise AssertionError(f"monitor initialization failed: {operation}")
        return {"operation": operation, "node": self.api("/api/node/status")}

    def ready(self):
        for address in self.addresses:
            wait(lambda: HTTP.open(f"http://{address}:9090/healthz", timeout=5).read(),
                 lambda body: body == b"ok", "native release Web listener", seconds=20)

    def observed_browser(self, name, *, config):
        with Control(BrowserObservations(self)) as control:
            self.browser(name, config=config, extra_env={
                "CROWDB_NATIVE_OBSERVATION_CONTROL": str(control.directory),
                "CROWDB_NATIVE_PENDING_CANDIDATES": json.dumps(self.pending_candidates)})

    def control(self, body):
        if body.get("action") == "native-storage-profile":
            return {"disk_gib": self.disk_gib, "zone_gib": self.zone_gib,
                    "spare_disk_gib": self.spare_disk_gib,
                    "spare_backing": disk_backing(self, 0, spare=True) if self.spare_disk_gib else None,
                    "backings": [disk_backing(self, node) for node in range(3)]}
        if body.get("action") == "run-native-browser":
            if self.case_name == "console::native_browser::page_layout_and_iceberg_inspection":
                chunks = body.get("chunks")
                if (not isinstance(chunks, dict) or len(chunks.get("ids", [])) != 21
                        or chunks.get("mirror") != chunks["ids"][0]
                        or chunks.get("ec") != chunks["ids"][1]):
                    raise ValueError("inspection requires its actual 21 allocated chunks")
                self.browser_timeout = len(INSPECTION_CASES) * 180
                try:
                    self.browser(INSPECTION_CASES, config="native.config.ts",
                        extra_env={"CROWDB_NATIVE_CHUNK_FIXTURE": json.dumps(chunks)},
                        expected=len(INSPECTION_CASES))
                finally:
                    resources = capture_resources(self.command, self.servers,
                                                  self.artifacts / "node-resources.json")
                for name, node in resources.items():
                    assert node.get("pids.events") == "max 0", f"{name} exhausted its PID budget: {node}"
                    assert not node.get("OOMKilled", False), f"{name} was OOM killed: {node}"
                return {"passed": True}
            if self.case_name == "console::native_browser::production_split_and_transfer_journals":
                self.browser_timeout = 12 * 60
                self.browser("native diagnostics: production split displays actual inherited and current journals",
                    config="native.config.ts", extra_env={"CROWDB_NATIVE_TRANSITION_ACCEPTANCE": "1"})
                return {"passed": True}
            if self.case_name == "console::native_browser::large_journal_records_and_restart":
                self.browser_timeout = 180
                self.browser("native diagnostics: large Journal exposes a bounded tail chunk record across restart",
                    config="native.config.ts", extra_env={"CROWDB_NATIVE_JOURNAL_WINDOWS": "1"})
                return {"passed": True}
            if self.case_name == "console::native_browser::auxiliary_process_menus":
                observations = BrowserObservations(self)
                with Control(observations) as control:
                    self.browser_timeout = 180
                    self.browser("native diagnostics: auxiliary menus stop and restart the actual typed process",
                        config="native.config.ts", extra_env={
                            "CROWDB_NATIVE_OBSERVATION_CONTROL": str(control.directory)})
                return {"passed": True}
            if self.case_name == "console::native_browser::waiting_plan_prerequisites":
                self.browser_timeout = 180
                self.browser("native diagnostics: waiting plan resumes when DiskIO prerequisites arrive",
                    config="native.config.ts", extra_env={"CROWDB_NATIVE_PLAN_PREREQUISITES": "1"})
                return {"passed": True}
            if self.case_name == "console::native_browser::storage_windows_and_access_recovery":
                chunks = body.get("chunks")
                if not isinstance(chunks, dict) or len(chunks.get("ids", [])) != 21:
                    raise ValueError("storage windows require actual allocated chunks")
                self.browser_timeout = 180
                self.browser((
                    "native diagnostics: Zone canvas colors match every actual allocation bit",
                    "native diagnostics: Chunk replacement windows and real Mirror EC placement",
                    "native diagnostics: unavailable catalog retries the actual Access services",
                    "native diagnostics: browser history preserves S3 windows and rejects replaced or deleted resources",
                ), config="native.config.ts", expected=4,
                    extra_env={"CROWDB_NATIVE_CHUNK_FIXTURE": json.dumps(chunks)})
                return {"passed": True}
            cases = {
                "console::browser_dataset::browse": "Dataset native: browse",
                "console::browser_dataset::import": "Dataset native: import",
                "console::browser_dataset::split_revisions": "Dataset native: split_revisions",
                "console::browser_dataset::recovery": "Dataset native: recovery",
                "console::browser_dataset::capabilities": "Dataset native: capabilities",

                "console::native_browser::disk_inspector_and_selection":
                    "native diagnostics: disk Inspector and Capacity selection retain two actual groups",
                "console::native_browser::disk_maintenance_and_status_badges":
                    "native diagnostics: disk maintenance menus route to owners and retain every hardware badge",
                "console::native_browser::hardware_capacity_and_dashed_status":
                    "native diagnostics: hardware capacity aggregates disks and dashed status updates",
                "console::native_browser::auxiliary_dialog_defaults":
                    "native diagnostics: auxiliary dialogs reopen with unused IDs ports and valid dependencies",
                "console::native_browser::interrupted_plan_reconciliation":
                    "native diagnostics: interrupted six-service plan reconciles without duplicate deployment",
                "console::native_browser::scanner_and_scoped_recalc":
                    "native diagnostics: scanner and scoped recalc show actual completed observations",
                "console::native_browser::multipart_upload_and_physical_navigation":
                    "S3 native multipart upload, HEAD, bounded preview and full round trip",
                "console::native_browser::chunk_ownership_scope":
                    "native diagnostics: Chunk ownership matches both durable maps and node scopes",
                "console::native_browser::journal_identity_and_owner_recovery":
                    "native diagnostics: journal identity survives Chunk navigation and owner interruption",
            }
            name = cases.get(self.case_name)
            if name is None:
                raise ValueError("native browser is not owned by this case")
            if self.case_name == "console::native_browser::hardware_capacity_and_dashed_status":
                verify_file_admission(self)
            self.browser_timeout = 180
            self.browser(name, config="native.config.ts")
            if self.supplied_candidates:
                verify_candidates(self)
            return {"passed": True}
        if body.get("action") == "observe-retained-marker":
            return retained_marker(self, body.get("node"), body.get("path"), body.get("offset"))
        if body.get("action") == "observe-native-services":
            return services(self, body.get("node"))
        if body.get("action") == "observe-native-process":
            return process(self, body.get("node"), body.get("pid"))
        return super().control(body)
