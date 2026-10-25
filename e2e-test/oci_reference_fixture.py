# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Reference Docker/Compose startup with host-only endpoint clients."""

import json
import os
from pathlib import Path
import subprocess
import time

from console_cli_client import CliClient
from fixture import Cluster, redact, request
import oci_reference_protocols as protocols
import oci_reference_runtime as runtime
import oci_reference_browser as browser
import oci_reference_workloads as workloads
import oci_reference_recovery as recovery
import oci_shortcuts


class OciReferenceCluster(CliClient, Cluster):
    def __init__(self, image, artifacts, bundle, reference, editable_ui=False, cli_workload=False, address_recovery=False, shortcuts=False, **kwargs):
        implementation, topology = reference.split("-")
        if implementation not in ("docker", "compose") or topology not in ("one", "three"):
            raise ValueError("unsupported OCI reference case")
        super().__init__(image, artifacts, node_count=1 if topology == "one" else 3,
                         wire_replicas=False, **kwargs)
        self.implementation = implementation
        self.editable_ui = editable_ui
        self.cli_workload = cli_workload
        self.address_recovery = address_recovery
        self.shortcuts = shortcuts
        self.bundle = Path(bundle).resolve()
        self.root = (self.data_root / "client-runtime").resolve()
        self.phase_number = 0
        self.runtime_accepted = False
        self.expected_names = [f"{self.run_id}-node{index + 1}" for index in range(self.node_count)]
        if implementation == "compose":
            services = ["node"] if self.node_count == 1 else ["node1", "node2", "node3"]
            self.expected_names = [f"{self.run_id}-{service}-1" for service in services]

    def start(self):
        self.root.mkdir(parents=True, mode=0o700)
        port_count = 34 if self.address_recovery else 24
        result = subprocess.run([str(self.bundle / "crowdb-cli"), "port-alloc", "--service", "web",
            "--count", str(port_count), "--owner-pid", str(os.getpid())], capture_output=True,
            text=True, timeout=10, check=True)
        allocated = [int(port) for port in result.stdout.split()]
        if len(allocated) != port_count or allocated != list(range(allocated[0], allocated[0] + port_count)):
            raise AssertionError("reference fixture needs its reserved contiguous host ports")
        self.port_base = allocated[0]
        self.image_id = json.loads(self.command("image", "inspect", self.image))[0]["Id"]
        runtime.start(self)
        runtime.refresh(self)
        return self

    def endpoint(self, index):
        return f"http://127.0.0.1:{self.port_base + index * 10}"

    def ready_report(self):
        report = json.loads(self.invoke("cluster", "check", "--wait", "180", "--json", endpoint=self.endpoint(0)))
        assert report["phase"] == "ready"
        return report

    def verify_reference(self):
        report = self.ready_report()
        if self.cli_workload:
            workloads.pending_free(self)
            return
        if self.editable_ui:
            browser.verify(self)
            for index in range(self.node_count):
                racks = request(self.endpoint(index), "/api/racks")
                assert any(rack["id"] == 61 and rack["name"] == "Rack Sixty-One" for rack in racks)
            return
        cluster_id = report["observation"]["cluster_id"]
        before = runtime.refresh(self)
        private = [protocols.credentials(self, index) for index in range(self.node_count)]
        witness = protocols.write_witnesses(self)
        protocols.read_witnesses(self, witness)
        for index in range(self.node_count):
            observed = json.loads(self.invoke("cluster", "check", "--json", endpoint=self.endpoint(index)))
            assert observed["phase"] == "ready" and observed["observation"]["cluster_id"] == cluster_id
        assert not (self.root / "persistent/oci-clusters").exists()
        runtime.recreate(self)
        after_report = self.ready_report()
        after = runtime.refresh(self)
        assert after_report["observation"]["cluster_id"] == cluster_id
        assert not {item["Id"] for item in before} & {item["Id"] for item in after}
        assert [protocols.credentials(self, index) for index in range(self.node_count)] == private
        protocols.read_witnesses(self, witness)
        (self.artifacts / "reference-proof.json").write_text(json.dumps({
            "cluster_id": cluster_id, "implementation": self.implementation,
            "nodes": self.node_count, "image_id": self.image_id,
            "container_ids": [item["Id"] for item in after],
            "published_ui_origins": [self.endpoint(index) for index in range(self.node_count)],
            "peer_addresses_supplied": False, "runtime_receipt": False,
            "s3_retained_reads": self.node_count * 2,
            "iceberg_table_reads": self.node_count * 2,
            "dataset_snapshot_reads": self.node_count * 2,
            "editable_management": True, "recreated_retained_identity": True,
            "retained_credentials": True}, indent=2))
        print("PASS OCI reference", self.implementation, self.node_count, cluster_id, flush=True)
        if self.address_recovery:
            recovery.verify(self, cluster_id, witness, private)
        if self.shortcuts:
            for name in self.expected_names:
                self.command("stop", "--time", "30", name)
            oci_shortcuts.verify(self)

    def diagnostics(self):
        if not self.runtime_accepted:
            return
        for index in range(self.node_count):
            try:
                value = request(self.endpoint(index), "/api/cluster/status")
                (self.artifacts / f"cluster-status-{index}.json").write_text(redact(value))
            except (OSError, ValueError) as error:
                (self.artifacts / f"cluster-status-{index}.error").write_text(redact(str(error)))
        for item in runtime.inventory(self, "container"):
            runtime.validate_container(self, item)
            target = self.artifacts / "runtime-logs" / item["Id"][:12]
            target.mkdir(parents=True, exist_ok=True)
            self.command("cp", f"{item['Id']}:/opt/crowdb/data/log", str(target))
        super().diagnostics()

    def close(self):
        if self.closed:
            return
        started = time.monotonic()
        runtime.close(self)
        self.closed = True
        self.timings["stop"] = time.monotonic() - started
        if self.started is not None:
            self.timings["total"] = time.monotonic() - self.started
        self.write_timings()
