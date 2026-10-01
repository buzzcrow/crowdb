import base64
import os
from pathlib import Path
import re
import sys

import boto3
from botocore.config import Config


BUCKET = "crowdb-preview-e2e"
KEY = "objects/persisted.parquet"
LARGE_KEY = "objects/large.bin"
fixture = Path(__file__).resolve().parents[3] / "lib/crowdb-access-iceberg/tests/common/parquet_scalar_official.rs"
encoded = re.search(r'pub const PARQUET_1_0_FALSE: &str = "([^"]+)"', fixture.read_text()).group(1)
BODY = base64.b64decode(encoded)
LARGE_BODY = bytes(range(256)) * (9 * 1024 * 1024 // 256)
assert BODY.startswith(b"PAR1") and BODY.endswith(b"PAR1")


def main():
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["CROWDB_PREVIEW_S3_ENDPOINT"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        config=Config(
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )
    if sys.argv[1] == "write":
        client.create_bucket(Bucket=BUCKET)
        client.put_object(Bucket=BUCKET, Key=KEY, Body=BODY)
        client.put_object(Bucket=BUCKET, Key=LARGE_KEY, Body=LARGE_BODY)
    assert BUCKET in {item["Name"] for item in client.list_buckets()["Buckets"]}
    listed = client.list_objects_v2(Bucket=BUCKET, Prefix="objects/")
    assert {item["Key"] for item in listed["Contents"]} == {KEY, LARGE_KEY}
    head = client.head_object(Bucket=BUCKET, Key=KEY)
    assert head["ContentLength"] == len(BODY)
    assert head["LastModified"] is not None
    assert client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read() == BODY
    assert client.get_object(Bucket=BUCKET, Key=KEY, Range="bytes=5-13")["Body"].read() == BODY[5:14]
    assert client.head_object(Bucket=BUCKET, Key=LARGE_KEY)["ContentLength"] == len(LARGE_BODY)
    assert client.get_object(Bucket=BUCKET, Key=LARGE_KEY)["Body"].read() == LARGE_BODY


if __name__ == "__main__":
    main()
