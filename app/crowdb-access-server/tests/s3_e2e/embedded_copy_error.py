# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Exercise the production copy response body with a controlled operation failure."""
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

client = boto3.client("s3", endpoint_url=sys.argv[1], region_name="us-east-1",
                      aws_access_key_id="test-access", aws_secret_access_key="test-secret",
                      config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}))
try:
    client.copy_object(Bucket="bucket", Key="key", CopySource="bucket/source")
except ClientError as error:
    assert error.response["Error"]["Code"] == "ServiceUnavailable", error.response["Error"]["Code"]
    print("boto3 recognized the production embedded copy error after HTTP 200 and keepalives")
else:
    raise AssertionError("boto3 reported an unsuccessful copy as successful")
