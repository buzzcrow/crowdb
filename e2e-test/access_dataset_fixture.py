# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Normal Dataset listener over persistent protected Chunk-KV storage."""

import ipaddress
import os
import subprocess
import urllib.error
import urllib.request

from allocation import node_data, start_node
from chunk_kv_fixture import ChunkKvCluster
from fixture import HTTP, wait


class AccessDatasetCluster(ChunkKvCluster):
    def start(self):
        super().start()
        index = 12
        address = str(ipaddress.ip_address(self.addresses[0]) + index)
        observed = start_node(self, index, node_data(self, index),
            ("server", "access", "dataset", "serve"), address,
            environment=(
                "CROWDB_MANAGEMENT_SEEDS=" + ",".join(self.origins[:3]),
                f"CROWDB_DATASET_LISTEN={address}:7503",
                "CROWDB_DATASET_READ_TOKEN=token",
                "CROWDB_DATASET_NAME=e2e",
                "CROWDB_ACCESS_LOG_DIR=/opt/crowdb/data/log/access",
            ))
        if observed != address:
            raise AssertionError("Dataset inspected address differs from its listener")
        self.addresses.append(observed)
        self.ready_dataset()
        return self

    def ready_dataset(self):
        def probe():
            try:
                HTTP.open(urllib.request.Request(
                    f"http://{self.addresses[12]}:7503/v1/dataset/open"), timeout=2)
            except urllib.error.HTTPError as error:
                return error.code == 401
            return False
        wait(probe, bool, "Dataset authenticated listener readiness")

    def control(self, body):
        result = super().control(body)
        if body.get("action") == "start":
            if body["node"] == 12:
                self.ready_dataset()
            elif body["node"] == self.chunk_kv_index:
                self.ready_chunk_kv(recovering=True)
        return result

    def sdk_roundtrip(self):
        endpoint=f"http://{self.addresses[12]}:7503"
        for language in ("rust","python"):
            root=self.artifacts / ("sdk-"+language)
            environment=dict(os.environ,CROWDB_DATASET_ENDPOINT=endpoint,CROWDB_DATASET_NAME="sdk-"+language,CROWDB_DATASET_READ_TOKEN="token",CROWDB_DATASET_CONSUMER_ROOT=str(root))
            bypass=",".join(filter(None,[environment.get("NO_PROXY",""),environment.get("no_proxy",""),*self.addresses]))
            environment.update(NO_PROXY=bypass,no_proxy=bypass)
            if language=="rust":
                command=[str(self.bundle/"dataset-rust-client")]
            else:
                command=[str(self.bundle/"dataset-sdk/consumer/bin/python"),str(self.bundle/"dataset-sdk/real_consumer.py")]
            for phase in ("write","verify"):
                environment["CROWDB_DATASET_PHASE"]=phase
                result=subprocess.run(command,env=environment,cwd=self.artifacts,text=True,capture_output=True,timeout=600)
                output=result.stdout+result.stderr
                (self.artifacts/f"sdk-{language}-{phase}.log").write_text(output)
                print(output,end="",flush=True)
                if result.returncode:raise RuntimeError(f"Dataset {language} SDK {phase} failed: {output}")
                if phase=="write":
                    for node in (12,11):self.control({"action":"kill","node":node})
                    for node in (11,12):self.control({"action":"start","node":node})

    def sdk_split_roundtrip(self):
        endpoint = f"http://{self.addresses[12]}:7503"
        for language in ("rust", "python"):
            environment = dict(os.environ, CROWDB_DATASET_ENDPOINT=endpoint,
                CROWDB_DATASET_NAME="split-sdk-" + language, CROWDB_DATASET_READ_TOKEN="token")
            bypass = ",".join(filter(None, [environment.get("NO_PROXY", ""), environment.get("no_proxy", ""), *self.addresses]))
            environment.update(NO_PROXY=bypass, no_proxy=bypass)
            command = ([str(self.bundle / "dataset-rust-split-client")] if language == "rust" else
                [str(self.bundle / "dataset-sdk/consumer/bin/python"), str(self.bundle / "dataset-sdk/split_consumer.py")])
            result = subprocess.run(command, env=environment, cwd=self.artifacts,
                text=True, capture_output=True, timeout=600)
            output = result.stdout + result.stderr
            (self.artifacts / f"sdk-{language}-split.log").write_text(output)
            print(output, end="", flush=True)
            if result.returncode:
                raise RuntimeError(f"Dataset {language} Split SDK failed: {output}")
