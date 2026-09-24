import sys
import uuid

import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import RESTError, UnauthorizedError, NamespaceAlreadyExistsError, NamespaceNotEmptyError


def main():
    uri = sys.argv[1]
    properties = {"type": "rest", "uri": uri, "token": "r" * 32}
    for extra in ({}, {"warehouse": ""}, {"token": "w" * 32}):
        catalog = load_catalog("crowdb", **(properties | extra))
        assert catalog.properties["crowdb.iceberg.v1.read"] == "false"
        assert catalog.properties["crowdb.iceberg.v3.write"] == "false"
    for extra, expected in (
        ({"warehouse": "unknown"}, RESTError),
        ({"token": "wrong"}, UnauthorizedError),
    ):
        try:
            load_catalog("crowdb", **(properties | extra))
        except expected as error:
            if "warehouse" in extra:
                assert "NoSuchWarehouseException" in str(error)
        else:
            raise AssertionError(f"expected {expected.__name__}")
    response = requests.get(
        uri + "/v1/config",
        headers={"Authorization": "Bearer " + "r" * 32},
        timeout=5,
    )
    response.raise_for_status()
    assert set(response.json()["endpoints"]) == {
        "GET /v1/{prefix}/namespaces",
        "GET /v1/{prefix}/namespaces/{namespace}",
        "HEAD /v1/{prefix}/namespaces/{namespace}",
        "POST /v1/{prefix}/namespaces",
        "POST /v1/{prefix}/namespaces/{namespace}/properties",
        "DELETE /v1/{prefix}/namespaces/{namespace}",
    }
    assert response.json()["idempotency-key-lifetime"] == "PT24H"
    catalog = load_catalog("crowdb", **properties)
    namespaces = catalog.list_namespaces()
    assert isinstance(namespaces, list)
    for namespace in namespaces:
        assert isinstance(catalog.load_namespace_properties(namespace), dict)
    complete = requests.get(uri + "/v1/namespaces", headers={"Authorization": "Bearer " + "r" * 32}, timeout=5)
    complete.raise_for_status()
    assert complete.json()["next-page-token"] is None
    assert len(complete.json()["namespaces"]) == len(namespaces)
    missing = requests.head(uri + "/v1/namespaces/missing-namespace", headers={"Authorization": "Bearer " + "r" * 32}, timeout=5)
    assert missing.status_code == 404 and missing.content == b""
    if "--read-only" in sys.argv[2:]:
        print("PyIceberg config, authentication and namespace reads passed")
    else:
        verify_namespaces(uri, properties)
        print("PyIceberg config, authentication and namespace CRUD passed")


def verify_namespaces(uri, properties):
    writer = load_catalog("crowdb", **(properties | {"token": "w" * 32}))
    namespace = ("client-" + uuid.uuid4().hex,)
    child = namespace + ("冰+a%2F",)
    writer.create_namespace(namespace, {"owner": "original"})
    assert writer.namespace_exists(namespace)
    assert writer.load_namespace_properties(namespace) == {"owner": "original"}
    try:
        writer.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass
    else:
        raise AssertionError("duplicate namespace was accepted")
    writer.create_namespace(child)
    assert writer.list_namespaces(namespace) == [child]
    update = writer.update_namespace_properties(namespace, removals={"owner", "absent"}, updates={"owner2": "retained"})
    assert update.removed == ["owner"] and update.missing == ["absent"]
    assert writer.load_namespace_properties(namespace) == {"owner2": "retained"}
    try:
        writer.drop_namespace(namespace)
    except NamespaceNotEmptyError:
        pass
    else:
        raise AssertionError("nonempty namespace was dropped")
    denied = requests.post(uri + "/v1/namespaces", json={"namespace": ["denied"]}, headers={"Authorization": "Bearer " + "r" * 32}, timeout=5)
    assert denied.status_code == 403
    writer.drop_namespace(child)
    writer.drop_namespace(namespace)
    assert not writer.namespace_exists(namespace)


if __name__ == "__main__":
    main()
