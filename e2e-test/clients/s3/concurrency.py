# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from base import *


class ConcurrencyCases:
    def test_independent_frontends_share_one_namespace(self):
        second_endpoint = os.environ.get("CROWDB_S3_E2E_SECOND_ENDPOINT")
        if not second_endpoint:
            self.skipTest("second access-server endpoint not supplied")
        second = boto3.client(
            "s3",
            endpoint_url=second_endpoint,
            region_name=os.environ.get("CROWDB_S3_E2E_REGION", "us-east-1"),
            aws_access_key_id=os.environ.get("CROWDB_S3_E2E_ACCESS_KEY", "test-access"),
            aws_secret_access_key=os.environ.get("CROWDB_S3_E2E_SECRET_KEY", "test-secret"),
            config=Config(s3={"addressing_style": "path"}),
        )
        bucket = f"{self.bucket}-scaleout"
        key = "alternate/one.bin"
        self.client.create_bucket(Bucket=bucket)
        self.assertIn(bucket, [item["Name"] for item in second.list_buckets()["Buckets"]])
        self.client.put_object(Bucket=bucket, Key=key, Body=b"first")
        self.assertEqual(second.get_object(Bucket=bucket, Key=key)["Body"].read(), b"first")
        second.put_object(Bucket=bucket, Key=key, Body=b"second")
        self.assertEqual(self.client.get_object(Bucket=bucket, Key=key)["Body"].read(), b"second")
        listed = second.list_objects_v2(Bucket=bucket, Prefix="alternate/")
        self.assertEqual([item["Key"] for item in listed["Contents"]], [key])
        self.client.delete_object(Bucket=bucket, Key=key)
        with self.assertRaises(ClientError) as absent:
            second.head_object(Bucket=bucket, Key=key)
        self.assertEqual(absent.exception.response["ResponseMetadata"]["HTTPStatusCode"], 404)
        second.delete_bucket(Bucket=bucket)

    def test_concurrent_overwrite_delete_and_get_are_portable(self):
        second_endpoint = os.environ.get("CROWDB_S3_E2E_SECOND_ENDPOINT")
        if not second_endpoint:
            self.skipTest("second access-server endpoint not supplied")
        second = boto3.client(
            "s3",
            endpoint_url=second_endpoint,
            region_name=os.environ.get("CROWDB_S3_E2E_REGION", "us-east-1"),
            aws_access_key_id=os.environ.get("CROWDB_S3_E2E_ACCESS_KEY", "test-access"),
            aws_secret_access_key=os.environ.get("CROWDB_S3_E2E_SECRET_KEY", "test-secret"),
            config=Config(s3={"addressing_style": "path"}),
        )
        bucket = f"{self.bucket}-races"
        key = "concurrent/object.bin"
        old_payload = b"old" * 9001
        new_payload = b"new" * 17001
        final_payload = b"final" * 11003
        self.client.create_bucket(Bucket=bucket)
        self.client.put_object(Bucket=bucket, Key=key, Body=old_payload)
        barrier = Barrier(3)

        def overwrite():
            barrier.wait(timeout=5)
            for _ in range(8):
                self.client.put_object(Bucket=bucket, Key=key, Body=new_payload)
                self.client.put_object(Bucket=bucket, Key=key, Body=old_payload)

        def delete_and_restore():
            barrier.wait(timeout=5)
            for _ in range(8):
                second.delete_object(Bucket=bucket, Key=key)
                second.put_object(Bucket=bucket, Key=key, Body=new_payload)

        def read():
            barrier.wait(timeout=5)
            for _ in range(25):
                try:
                    response = second.get_object(Bucket=bucket, Key=key)
                except ClientError as error:
                    self.assertEqual(error.response["ResponseMetadata"]["HTTPStatusCode"], 404)
                    continue
                body = response["Body"].read()
                self.assertIn(body, (old_payload, new_payload))
                self.assertEqual(response["ETag"], f'"{md5(body).hexdigest()}"')

        with ThreadPoolExecutor(max_workers=3) as workers:
            writer = workers.submit(overwrite)
            deleter = workers.submit(delete_and_restore)
            reader = workers.submit(read)
            writer.result(timeout=30)
            deleter.result(timeout=30)
            reader.result(timeout=30)
        self.client.put_object(Bucket=bucket, Key=key, Body=final_payload)
        self.assertEqual(second.get_object(Bucket=bucket, Key=key)["Body"].read(), final_payload)
        listed = second.list_objects_v2(Bucket=bucket, Prefix="concurrent/")
        self.assertEqual([item["Key"] for item in listed["Contents"]], [key])
        self.client.delete_object(Bucket=bucket, Key=key)
        self.assertEqual(second.list_objects_v2(Bucket=bucket).get("KeyCount", 0), 0)
        self.client.delete_bucket(Bucket=bucket)
