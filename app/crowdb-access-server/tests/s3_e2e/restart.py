# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import os
import sys
from hashlib import md5

import boto3
from botocore.config import Config


def main():
    endpoint = os.environ["CROWDB_S3_E2E_ENDPOINT"]
    phase = sys.argv[1]
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("CROWDB_S3_E2E_REGION", "us-east-1"),
        aws_access_key_id=os.environ["CROWDB_S3_E2E_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CROWDB_S3_E2E_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
    )
    bucket = "crowdb-e2e-restart"
    key = "persisted/across-access-restart.bin"
    payload = bytes(range(256)) * 257
    etag = f'"{md5(payload).hexdigest()}"'

    if phase == "prepare":
        client.create_bucket(Bucket=bucket)
        assert client.put_object(Bucket=bucket, Key=key, Body=payload)["ETag"] == etag
    elif phase == "verify":
        assert client.head_object(Bucket=bucket, Key=key)["ETag"] == etag
        assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == payload
    elif phase == "cleanup":
        client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=bucket)
    else:
        raise ValueError(f"unknown restart phase: {phase}")


if __name__ == "__main__":
    main()
