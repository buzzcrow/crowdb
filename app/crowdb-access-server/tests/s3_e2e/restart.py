# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import os
import sys
import time
from hashlib import md5

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


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
    overwritten_key = "persisted/overwritten-before-restart.bin"
    old_payload = b"old-generation" * 8192
    new_payload = b"new-generation" * 8192
    new_etag = f'"{md5(new_payload).hexdigest()}"'
    deleted_key = "persisted/deleted-before-restart.bin"

    if phase == "prepare":
        client.create_bucket(Bucket=bucket)
        assert client.put_object(Bucket=bucket, Key=key, Body=payload)["ETag"] == etag
        client.put_object(Bucket=bucket, Key=overwritten_key, Body=old_payload)
        assert client.put_object(Bucket=bucket, Key=overwritten_key, Body=new_payload)["ETag"] == new_etag
        client.put_object(Bucket=bucket, Key=deleted_key, Body=old_payload)
        client.delete_object(Bucket=bucket, Key=deleted_key)
    elif phase in ("verify", "verify-after-chunkdb-restart", "verify-after-diskdb-restart"):
        deadline = time.monotonic() + (20 if phase.endswith("db-restart") else 0)
        while True:
            try:
                assert client.head_object(Bucket=bucket, Key=key)["ETag"] == etag
                assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == payload
                assert client.head_object(Bucket=bucket, Key=overwritten_key)["ETag"] == new_etag
                assert client.get_object(Bucket=bucket, Key=overwritten_key)["Body"].read() == new_payload
                keys = {entry["Key"] for entry in client.list_objects_v2(Bucket=bucket).get("Contents", [])}
                assert keys == {key, overwritten_key}, keys
                break
            except (BotoCoreError, ClientError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.5)
    elif phase == "cleanup":
        client.delete_object(Bucket=bucket, Key=key)
        client.delete_object(Bucket=bucket, Key=overwritten_key)
        client.delete_bucket(Bucket=bucket)
    else:
        raise ValueError(f"unknown restart phase: {phase}")


if __name__ == "__main__":
    main()
