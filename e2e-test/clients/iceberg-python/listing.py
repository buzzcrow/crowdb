# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import sys
from urllib.parse import urlsplit

import pyarrow
import pyarrow.fs as fs
import pyiceberg
from pyiceberg.io.pyarrow import PyArrowFile


def main():
    config = json.load(sys.stdin)
    endpoint = urlsplit(config["endpoint"])
    filesystem = fs.S3FileSystem(
        access_key=config["access_key"], secret_key=config["secret_key"],
        session_token=config["session_token"], region="us-east-1", scheme=endpoint.scheme,
        endpoint_override=endpoint.netloc, force_virtual_addressing=False,
    )
    prefix = f'{config["bucket"]}/{config["prefix"]}data/'
    for suffix in ["a.json", "nested/b.json", "雪&<>+%.json"]:
        path = prefix + suffix
        file = PyArrowFile("s3://" + path, path, filesystem)
        assert not file.exists()
        with file.create(overwrite=False) as output:
            output.write(b"{}")
        assert file.exists() and len(file) == 2
        with file.open() as stream:
            assert stream.read() == b"{}"
    selected = filesystem.get_file_info(fs.FileSelector(prefix.removesuffix("/"), recursive=True))
    names = sorted(file.path for file in selected if file.type == fs.FileType.File)
    assert names == sorted(prefix + suffix for suffix in ["a.json", "nested/b.json", "雪&<>+%.json"]), names
    print(f"PyIceberg {pyiceberg.__version__} / PyArrow {pyarrow.__version__}: exact creation, HEAD/GET and prefix discovery passed")


if __name__ == "__main__":
    main()
