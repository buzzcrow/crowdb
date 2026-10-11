# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Two release Access frontends over persistent protected storage."""

import ipaddress
import json
import os
import subprocess
import time
from pathlib import Path

from allocation import node_data, start_node
from chunk_kv_fixture import ChunkKvCluster
from fixture import HTTP, wait
from s3_metrics import verify_native_metrics, verify_payload_copies


class AccessS3Cluster(ChunkKvCluster):
    def start(self):
        super().start()
        credentials = self.artifacts / "credentials.json"
        try:
            self.client(self.bundle, "credentials", "access_s3::credentials")
            self.credentials = json.loads(credentials.read_text())
        finally:
            credentials.unlink(missing_ok=True)
        for index in (12, 13):
            address = str(ipaddress.ip_address(self.addresses[0]) + index)
            environment = (
                f"CROWDB_S3_LISTEN={address}:7500",
                "CROWDB_ACCESS_HEALTH_LISTEN=0.0.0.0:7501",
                "CROWDB_ACCESS_LOG_DIR=/opt/crowdb/data/log/access",
                "CROWDB_MANAGEMENT_SEEDS=" + ",".join(self.origins[:3]),
                "CROWDB_S3_TENANT=boto3-e2e", "CROWDB_S3_MASTER_KEY=" + "11" * 32,
                "CROWDB_S3_REGION=us-east-1", "CROWDB_S3_EC_DATA=2", "CROWDB_S3_EC_CODE=1",
                "CROWDB_S3_NATIVE_BUDGET_BYTES=1048576", "CROWDB_S3_MAX_CHUNK_SIZE=4194304",
            )
            observed = start_node(self, index, node_data(self, index),
                                  ("server", "access", "s3"), address, environment=environment)
            if observed != address:
                raise AssertionError("Access inspected address differs from advertised endpoint")
            self.addresses.append(observed)
            self.ready_access(index)
        (self.artifacts / "access-endpoints.json").write_text(json.dumps({
            "http_port": 7500, "health_port": 7501, "ips": self.addresses[12:14]}, indent=2))
        return self

    def ready_access(self, index):
        def probe():
            state = json.loads(self.command("inspect", self.servers[index]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("Access exited: " + self.command("logs", self.servers[index]))
            with HTTP.open(f"http://{self.addresses[index]}:7501/_crowdb/health/ready", timeout=5) as response:
                return response.status == 200
        wait(probe, bool, "release Access dependency readiness")

    def client_environment(self):
        env = os.environ.copy()
        env.update(CROWDB_S3_E2E_ENDPOINT=f"http://{self.addresses[12]}:7500",
                   CROWDB_S3_E2E_SECOND_ENDPOINT=f"http://{self.addresses[13]}:7500",
                   CROWDB_S3_E2E_REGION="us-east-1",
                   CROWDB_S3_E2E_ACCESS_KEY=self.credentials["access_key"],
                   CROWDB_S3_E2E_SECRET_KEY=self.credentials["secret_key"])
        bypass = ",".join(filter(None, [env.get("NO_PROXY", ""), env.get("no_proxy", ""), *self.addresses]))
        env.update(NO_PROXY=bypass, no_proxy=bypass)
        env["CROWDB_MANAGEMENT_SEEDS"] = ",".join(self.origins[:3])
        env["CROWDB_S3_MASTER_KEY"] = "11" * 32
        env["LD_LIBRARY_PATH"] = str(self.artifacts / "release-cli-lib") + ":" + env.get("LD_LIBRARY_PATH", "")
        return env

    def sdk_client(self, method):
        script = Path(__file__).parent / "clients/s3/basic.py"
        self.run_sdk(method, script, ["BasicS3CompatibilityTest." + method], unittest_case=True)
        if method == "test_default_boto3_checksums_and_multipart":
            verify_native_metrics(self)
        elif method in ("test_multipart_copy_selects_ranges_and_replaces_parts",
                        "test_copy_captures_source_before_overwrite_and_delete"):
            verify_payload_copies(self, copying=True)
        elif method not in ("test_aws_cli_workflow", "test_rclone_workflow"):
            verify_payload_copies(self)

    def run_sdk(self, method, script, arguments, unittest_case=False, frontend=12,
                environment="s3-e2e", program="python", timeout=120):
        env = self.client_environment()
        env["CROWDB_S3_E2E_ENDPOINT"] = f"http://{self.addresses[frontend]}:7500"
        started = time.monotonic()
        status = "failed"
        exit_code = None
        try:
            result = subprocess.run(["pixi", "run", "-e", environment, program, str(script), *arguments],
                                    env=env, capture_output=True, text=True, timeout=timeout)
            output = result.stdout + result.stderr
            exit_code = result.returncode
            if result.returncode == 0 and (not unittest_case or ("Ran 1 test" in output and "\nOK\n" in output)):
                status = "pass"
        except subprocess.TimeoutExpired as error:
            output = "".join(value.decode(errors="replace") if isinstance(value, bytes) else value or ""
                             for value in (error.stdout, error.stderr))
            status = "timeout"
        for secret in self.credentials.values():
            output = output.replace(secret, "<redacted>")
        (self.artifacts / f"client-{method}.log").write_text(output)
        (self.artifacts / f"client-{method}.json").write_text(json.dumps({
            "phase": method, "case": getattr(self, "case_name", "s3.protocol::" + method), "seconds": time.monotonic() - started,
            "status": status, "exit_code": exit_code, "execution": "host"}, indent=2))
        print(output, end="", flush=True)
        if status != "pass":
            raise RuntimeError(f"host SDK {method} did not complete exactly one passing case")

    def script_client(self, name):
        script = Path(__file__).parent / f"clients/s3/{name}.py"
        arguments = []
        if name == "admin":
            binary = self.artifacts / "release-access-cli"
            self.command("cp", self.servers[12] + ":/opt/crowdb/bin/crowdb-access-server", str(binary))
            self.command("cp", self.servers[12] + ":/opt/crowdb/lib", str(self.artifacts / "release-cli-lib"))
            arguments = [str(binary)]
        if name == "benchmark":
            info = json.loads(self.command("inspect", self.servers[12]))[0]
            arguments = ["--endpoint", f"http://{self.addresses[12]}:7500", "--server-pid",
                         str(info["State"]["Pid"]), "--sizes", "65536,1048576",
                         "--concurrency", "1,2", "--samples", "1"]
        self.run_sdk(name, script, arguments)
        if name == "benchmark":
            samples = json.loads((self.artifacts / "client-benchmark.log").read_text())
            if "server_process" not in samples:
                raise AssertionError("S3 benchmark did not capture release Access CPU/RSS")
            (self.artifacts / "s3-request-path.json").write_text(json.dumps(samples, indent=2))

    def language_client(self, language):
        script = Path(__file__).parent / "clients/s3-sdk.sh"
        for phase in ("prepare", "verify"):
            self.run_sdk(language + "-" + phase, script, [language, phase],
                         environment="s3-" + language + "-sdk", program="bash", timeout=700)
