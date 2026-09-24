import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import ServiceUnavailableError


def main():
    endpoint, mode, count = sys.argv[1:]
    catalog = load_catalog("crowdb", type="rest", uri=endpoint, token="r" * 32)
    if mode == "concurrency":
        barrier = Barrier(5, timeout=10)

        def list_once(index):
            reader = load_catalog(f"reader-{index}", type="rest", uri=endpoint, token="r" * 32)
            barrier.wait()
            try:
                assert reader.list_namespaces() == [("analytics",)]
                return 200
            except ServiceUnavailableError:
                return 503

        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(list_once, range(5)))
        assert sorted(results) == [200, 200, 200, 200, 503]
    elif mode == "overflow":
        for attempt in range(5):
            try:
                catalog.list_namespaces()
            except ServiceUnavailableError as error:
                assert "ServiceUnavailableException" in str(error)
            else:
                raise AssertionError(f"exhausted complete list returned success on request {attempt}")
    else:
        namespaces = catalog.list_namespaces()
        assert len(namespaces) == int(count)
        assert len(set(namespaces)) == len(namespaces)
        assert ("analytics",) in namespaces
    print(f"PyIceberg namespace {mode} passed")


if __name__ == "__main__":
    main()
