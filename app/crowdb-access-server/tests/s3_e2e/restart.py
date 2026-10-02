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
    multipart_key = "persisted/incomplete-before-restart.bin"
    multipart_payload = payload
    multipart_etag = f'"{md5(multipart_payload).hexdigest()}"'
    copied_key = "persisted/copied-before-restart.bin"

    if phase == "prepare":
        client.create_bucket(Bucket=bucket)
        assert client.put_object(Bucket=bucket, Key=key, Body=payload)["ETag"] == etag
        client.put_object(Bucket=bucket, Key=overwritten_key, Body=old_payload)
        assert client.put_object(Bucket=bucket, Key=overwritten_key, Body=new_payload)["ETag"] == new_etag
        client.put_object(Bucket=bucket, Key=deleted_key, Body=old_payload)
        client.delete_object(Bucket=bucket, Key=deleted_key)
        client.copy_object(Bucket=bucket, Key=copied_key, CopySource={"Bucket": bucket, "Key": key})
        upload_id = client.create_multipart_upload(Bucket=bucket, Key=multipart_key)["UploadId"]
        client.upload_part(Bucket=bucket, Key=multipart_key, UploadId=upload_id,
                           PartNumber=1, Body=b"obsolete part generation")
        assert client.upload_part_copy(Bucket=bucket, Key=multipart_key, UploadId=upload_id,
                                       PartNumber=1, CopySource={"Bucket": bucket, "Key": key})["CopyPartResult"]["ETag"] == multipart_etag
    elif phase in (
        "verify",
        "verify-after-group0-restart",
        "verify-after-chunkdb-restart",
        "verify-after-diskdb-restart",
        "verify-after-diskio-restart",
        "verify-after-chunk-kv-restart",
    ):
        deadline = time.monotonic() + (20 if phase.startswith("verify-after-") else 0)
        while True:
            try:
                assert client.head_object(Bucket=bucket, Key=key)["ETag"] == etag
                assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == payload
                assert client.head_object(Bucket=bucket, Key=overwritten_key)["ETag"] == new_etag
                assert client.get_object(Bucket=bucket, Key=overwritten_key)["Body"].read() == new_payload
                keys = {entry["Key"] for entry in client.list_objects_v2(Bucket=bucket).get("Contents", [])}
                assert keys == {key, overwritten_key, copied_key}, keys
                assert client.get_object(Bucket=bucket, Key=copied_key)["Body"].read() == payload
                uploads = [item for item in client.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
                           if item["Key"] == multipart_key]
                assert len(uploads) == 1, uploads
                parts = client.list_parts(Bucket=bucket, Key=multipart_key,
                                          UploadId=uploads[0]["UploadId"])["Parts"]
                assert [(part["PartNumber"], part["ETag"]) for part in parts] == [(1, multipart_etag)]
                break
            except (BotoCoreError, ClientError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.5)
    elif phase == "cleanup":
        uploads = [item for item in client.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
                   if item["Key"] == multipart_key]
        assert len(uploads) == 1, uploads
        completed = client.complete_multipart_upload(
            Bucket=bucket, Key=multipart_key, UploadId=uploads[0]["UploadId"],
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": multipart_etag}]},
        )
        assert completed["ETag"] == f'"{md5(md5(multipart_payload).digest()).hexdigest()}-1"'
        assert client.get_object(Bucket=bucket, Key=multipart_key)["Body"].read() == multipart_payload
        client.delete_object(Bucket=bucket, Key=multipart_key)
        client.delete_object(Bucket=bucket, Key=key)
        client.delete_object(Bucket=bucket, Key=overwritten_key)
        client.delete_object(Bucket=bucket, Key=copied_key)
        client.delete_bucket(Bucket=bucket)
    else:
        raise ValueError(f"unknown restart phase: {phase}")


if __name__ == "__main__":
    main()
