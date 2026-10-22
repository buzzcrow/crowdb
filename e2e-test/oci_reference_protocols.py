# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host protocol operations through the reference deployment's published ports."""

import json
import uuid
import urllib.error
import urllib.request

from fixture import HTTP, redact, request


def call(origin, path, *, method="GET", body=None, token=None, expected=(200,)):
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    data = None if body is None else json.dumps(body).encode()
    try:
        with HTTP.open(urllib.request.Request(origin + path, data=data, headers=headers,
                                              method=method), timeout=30) as response:
            status, content = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, content = error.code, error.read()
    if status not in expected:
        raise AssertionError(f"{method} {origin}{path}: HTTP {status}: {redact(content.decode(errors='replace'))[:2048]}")
    return json.loads(content) if content else None


def credentials(cluster, index):
    value = request(cluster.endpoint(index), "/api/access/credentials")
    base = cluster.port_base + index * 10
    assert value["s3"]["endpoint"] == f"http://127.0.0.1:{base + 1}"
    assert value["iceberg"]["uri"] == f"http://127.0.0.1:{base + 2}"
    assert value["dataset"]["uri"] == f"http://127.0.0.1:{base + 3}"
    return value


def write_witnesses(cluster):
    # Normal management writes must be allowed in both one- and three-node UIs.
    endpoint = cluster.endpoint(0)
    cluster.invoke("cluster", "rack", "add", "--id", "71", "--name", "reference-edit", endpoint=endpoint)
    assert "reference-edit" in cluster.invoke("cluster", "rack", "list", endpoint=endpoint)
    cluster.invoke("s3", "bucket", "put", "reference-witness", endpoint=endpoint)
    cluster.invoke("s3", "object", "put", "reference-witness", "retained", "--text",
                   "published-port-witness", endpoint=endpoint)
    private = credentials(cluster, 0)
    iceberg = private["iceberg"]
    call(iceberg["uri"], "/v1/config", expected=(401, 403))
    call(iceberg["uri"], "/v1/namespaces", method="POST", token=iceberg["token"],
         body={"namespace": ["reference"], "properties": {"witness": "persisted"}}, expected=(200, 201))
    table = call(iceberg["uri"], "/v1/namespaces/reference/tables", method="POST", token=iceberg["token"],
         body={"name": "events", "schema": {"type": "struct", "schema-id": 0,
               "fields": [{"id": 1, "name": "id", "type": "long", "required": True}]}},
         expected=(200, 201))
    committed = call(iceberg["uri"], "/v1/namespaces/reference/tables/events", method="POST",
        token=iceberg["token"], body={"requirements": [{"type": "assert-table-uuid",
        "uuid": table["metadata"]["table-uuid"]}], "updates": [{"action": "set-properties",
        "updates": {"reference": "committed"}}]})
    assert committed["metadata"]["properties"]["reference"] == "committed"
    dataset = private["dataset"]
    path = "/api/dataset/v2/namespaces/dataset-ns/datasets/reference"
    call(dataset["uri"], "/api/dataset/v2/capabilities", expected=(401, 403))
    created = call(dataset["uri"], path, method="PUT", token=dataset["token"], expected=(200, 201))
    assert created["name"] == "reference" and created["id"]
    publication = {"operation": uuid.uuid4().hex, "parent": None, "manifest": {
        "schema": [{"name": "value", "required": True}], "samples": [{"id": "c2FtcGxl",
        "fields": {"value": {"kind": "inline", "data": "aGVsbG8="}}}]}}
    published = call(dataset["uri"], path.replace("/v2/", "/v1/") + "/snapshots/publish",
                     method="POST", token=dataset["token"], body=publication)
    assert published["state"] == "published" and published["is_latest"]
    return {"dataset_id": created["id"], "dataset_snapshot": published["id"],
            "dataset_manifest": publication["manifest"]}


def read_witnesses(cluster, witness):
    for index in range(cluster.node_count):
        endpoint = cluster.endpoint(index)
        private = credentials(cluster, index)
        assert cluster.invoke("s3", "object", "get", "reference-witness", "retained", endpoint=endpoint) == "published-port-witness"
        assert "reference-edit" in cluster.invoke("cluster", "rack", "list", endpoint=endpoint)
        iceberg = private["iceberg"]
        namespace = call(iceberg["uri"], "/v1/namespaces/reference", token=iceberg["token"])
        assert namespace["namespace"] == ["reference"]
        assert namespace["properties"]["witness"] == "persisted"
        table = call(iceberg["uri"], "/v1/namespaces/reference/tables/events", token=iceberg["token"])
        assert table["metadata"]["schemas"][0]["fields"][0]["name"] == "id"
        assert table["metadata"]["properties"]["reference"] == "committed"
        dataset = private["dataset"]
        opened = call(dataset["uri"], "/api/dataset/v2/namespaces/dataset-ns/datasets/reference", token=dataset["token"])
        assert opened["id"] == witness["dataset_id"] and opened["name"] == "reference"
        assert opened["latest"] == witness["dataset_snapshot"]
        manifest = call(dataset["uri"], "/api/dataset/v1/namespaces/dataset-ns/datasets/reference/snapshots/"
                        + witness["dataset_snapshot"] + "/manifest", token=dataset["token"])
        assert manifest == witness["dataset_manifest"]
