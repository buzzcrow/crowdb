# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Native and no-device OCI SDK consumers of storage with no Access process."""

import json
import os
from pathlib import Path
import time

from chunk_kv_fixture import ChunkKvCluster
from fixture import LABEL


class NativeS3Cluster(ChunkKvCluster):
    def client(self, bundle, phase, test_name):
        super().client(bundle, phase, test_name)
        if test_name.startswith("access_s3::native::"):
            self.oci_client(bundle, phase, test_name)

    def oci_client(self, bundle, phase, test_name):
        bundle = Path(bundle).resolve()
        name = self.run_id + "-sdk-client"
        self.containers.append(name)
        environment = {
            "CROWDB_E2E_RPC": json.dumps(self.addresses),
            "CROWDB_E2E_PHASE": phase if getattr(self, "host_network", False) else "verify",
            "CROWDB_E2E_ARTIFACTS": str(self.artifacts),
            "CROWDB_NATIVE_S3_EXECUTION": "oci",
            "CROWDB_RDMA_ENABLED": "0",
            "LD_LIBRARY_PATH": str(bundle / "lib"),
        }
        environment.update(getattr(self, "sdk_environment", {}))
        if getattr(self, "host_network", False):
            environment.update(
                LD_LIBRARY_PATH=f"/usr/lib/x86_64-linux-gnu:{bundle / 'lib'}",
                LD_PRELOAD="/usr/lib/x86_64-linux-gnu/libibverbs.so.1:/usr/lib/x86_64-linux-gnu/librdmacm.so.1",
                RDMAV_DRIVERS_DIR="/usr/lib/x86_64-linux-gnu/libibverbs",
            )
        options = tuple(item for key, value in environment.items() for item in ("--env", f"{key}={value}"))
        started = time.monotonic()
        output = self.command("run", "--rm", "--name", name, "--label", f"{LABEL}={self.run_id}",
                              "--network", "host" if getattr(self, "host_network", False) else self.run_id,
                              *getattr(self, "hca_devices", ()), "--user", str(os.getuid()),
                              "--cpu-shares", "1024", "--memory", "1024m", "--pids-limit", "128",
                              "--ulimit", "memlock=-1:-1",
                              "--mount", f"type=bind,source={bundle},target={bundle},readonly",
                              "--mount", f"type=bind,source={self.artifacts},target={self.artifacts}",
                              *options, "--entrypoint", str(bundle / "domain-test"), self.image,
                              "--ignored", "--exact", test_name, "--nocapture", timeout=getattr(self, "client_timeout", 120))
        (self.artifacts / f"client-{phase}-oci.log").write_text(output)
        print(output, end="", flush=True)
        if "test result: ok. 1 passed; 0 failed;" not in output:
            raise RuntimeError(f"OCI native S3 client failed: {output}")
        (self.artifacts / f"client-{phase}-oci.json").write_text(json.dumps({
            "case": test_name, "phase": phase, "execution": "oci", "status": "pass",
            "exit_code": 0,
            "seconds": time.monotonic() - started, "rdma_devices": list(getattr(self, "hca_devices", ())), "access_process": False,
        }, indent=2))
        self.containers.remove(name)
