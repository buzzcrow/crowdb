# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import os
import unittest
from base64 import b64encode
from hashlib import md5

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class BasicS3CompatibilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        endpoint = os.environ.get("CROWDB_S3_E2E_ENDPOINT")
        if not endpoint:
            raise unittest.SkipTest("set CROWDB_S3_E2E_ENDPOINT to a running access server")
        cls.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=os.environ.get("CROWDB_S3_E2E_REGION", "us-east-1"),
            aws_access_key_id=os.environ.get("CROWDB_S3_E2E_ACCESS_KEY", "test-access"),
            aws_secret_access_key=os.environ.get("CROWDB_S3_E2E_SECRET_KEY", "test-secret"),
            config=Config(
                s3={"addressing_style": "path"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
        cls.bucket = os.environ.get("CROWDB_S3_E2E_BUCKET", "crowdb-basic-e2e")

    def test_basic_bucket_object_matrix(self):
        client = self.client
        bucket = self.bucket
        key = "prefix/object.bin"
        second_key = "prefix/second.bin"
        payload = bytes(range(256)) * 17

        client.create_bucket(Bucket=bucket)
        client.head_bucket(Bucket=bucket)
        content_md5 = b64encode(md5(payload).digest()).decode("ascii")
        put = client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentMD5=content_md5,
            ContentType="application/octet-stream",
        )
        self.assertEqual(put["ETag"], f'"{md5(payload).hexdigest()}"')
        client.put_object(Bucket=bucket, Key=second_key, Body=b"second")

        head = client.head_object(Bucket=bucket, Key=key)
        self.assertEqual(head["ContentLength"], len(payload))
        self.assertEqual(head["ETag"], f'"{md5(payload).hexdigest()}"')
        self.assertEqual(client.get_object(Bucket=bucket, Key=key)["Body"].read(), payload)
        self.assertEqual(
            client.get_object(Bucket=bucket, Key=key, IfMatch=head["ETag"])["Body"].read(),
            payload,
        )
        ranged = client.get_object(Bucket=bucket, Key=key, Range="bytes=7-31")
        self.assertEqual(ranged["Body"].read(), payload[7:32])

        page = client.list_objects_v2(Bucket=bucket, Prefix="prefix/", MaxKeys=1)
        self.assertEqual([item["Key"] for item in page.get("Contents", [])], [key])
        self.assertTrue(page["IsTruncated"])
        next_page = client.list_objects_v2(
            Bucket=bucket,
            Prefix="prefix/",
            MaxKeys=1,
            ContinuationToken=page["NextContinuationToken"],
        )
        self.assertEqual(
            [item["Key"] for item in next_page.get("Contents", [])],
            [second_key],
        )
        self.assertIn(bucket, [item["Name"] for item in client.list_buckets()["Buckets"]])

        with self.assertRaises(ClientError) as not_empty:
            client.delete_bucket(Bucket=bucket)
        self.assertEqual(not_empty.exception.response["Error"]["Code"], "BucketNotEmpty")

        with self.assertRaises(ClientError) as unsupported:
            client.put_object(Bucket=bucket, Key="unsupported", Body=b"x", StorageClass="GLACIER")
        self.assertEqual(unsupported.exception.response["ResponseMetadata"]["HTTPStatusCode"], 501)

        with self.assertRaises(ClientError) as bad_digest:
            client.put_object(
                Bucket=bucket,
                Key="bad-digest",
                Body=b"payload",
                ContentMD5=b64encode(bytes(16)).decode("ascii"),
            )
        self.assertEqual(bad_digest.exception.response["Error"]["Code"], "BadDigest")
        with self.assertRaises(ClientError) as absent:
            client.head_object(Bucket=bucket, Key="bad-digest")
        self.assertEqual(absent.exception.response["ResponseMetadata"]["HTTPStatusCode"], 404)

        client.delete_object(Bucket=bucket, Key=key)
        client.delete_object(Bucket=bucket, Key=second_key)
        client.delete_object(Bucket=bucket, Key="missing")
        client.delete_bucket(Bucket=bucket)


if __name__ == "__main__":
    unittest.main()
