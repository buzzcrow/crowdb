import sys

import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import RESTError, UnauthorizedError


def main():
    uri = sys.argv[1]
    properties = {"type": "rest", "uri": uri, "token": "r" * 32}
    for extra in ({}, {"warehouse": ""}):
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
    assert response.json()["endpoints"] == []
    assert "idempotency-key-lifetime" not in response.json()
    print("PyIceberg config, warehouse selection and authentication passed")


if __name__ == "__main__":
    main()
