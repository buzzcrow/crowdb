# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from base import *


class UploadCases:
    def test_slow_signed_upload_releases_native_buffers(self):
        bucket = f"{self.bucket}-slow"
        path = f"/{bucket}/slow.bin"
        second_path = f"/{bucket}/slow-second.bin"
        payload = bytes(range(256)) * (32768 + 1)
        status, _, _ = self.signed_http("PUT", f"/{bucket}")
        self.assertEqual(status, 200)
        barrier = Barrier(2)

        def upload(target):
            barrier.wait(timeout=5)
            return self.signed_http("PUT", target, payload, slow_chunk_size=8192)

        with ThreadPoolExecutor(max_workers=2) as workers:
            responses = list(workers.map(upload, (path, second_path)))
        for status, headers, data in responses:
            self.assertEqual(status, 200, data)
            self.assertEqual(headers["etag"], f'"{md5(payload).hexdigest()}"')
        self.assertEqual(self.client.get_object(Bucket=bucket, Key="slow.bin")["Body"].read(), payload)
        self.assertEqual(self.client.get_object(Bucket=bucket, Key="slow-second.bin")["Body"].read(), payload)
        status, _, metrics = self.signed_http("GET", "/_crowdb/metrics")
        self.assertEqual(status, 200)
        self.assertIn(b"crowdb_s3_native_retained_bytes 0\n", metrics)
        direct_bytes = next(
            int(line.split()[-1])
            for line in metrics.splitlines()
            if line.startswith(b"crowdb_s3_native_direct_bytes_total ")
        )
        self.assertGreater(direct_bytes, 0)
        self.client.delete_object(Bucket=bucket, Key="slow.bin")
        self.client.delete_object(Bucket=bucket, Key="slow-second.bin")
        self.client.delete_bucket(Bucket=bucket)

    def test_truncated_signed_upload_does_not_publish_and_releases_credit(self):
        bucket = f"{self.bucket}-truncated"
        key = "aborted-then-retried.bin"
        payload = bytes(range(256)) * 8192
        status, _, _ = self.signed_http("PUT", f"/{bucket}")
        self.assertEqual(status, 200)
        self.signed_http("PUT", f"/{bucket}/{key}", payload, truncate_after=131072)

        deadline = time.monotonic() + 10
        while True:
            _, _, exported = self.signed_http("GET", "/_crowdb/metrics")
            if b"crowdb_s3_native_retained_bytes 0\n" in exported:
                break
            self.assertLess(time.monotonic(), deadline, "aborted PUT retained a native owner")
            time.sleep(0.05)
        self.assertEqual(self.client.list_objects_v2(Bucket=bucket).get("KeyCount", 0), 0)
        self.assertEqual(
            self.client.put_object(Bucket=bucket, Key=key, Body=payload)["ETag"],
            f'"{md5(payload).hexdigest()}"',
        )
        self.assertEqual(self.client.get_object(Bucket=bucket, Key=key)["Body"].read(), payload)
        self.client.delete_object(Bucket=bucket, Key=key)
        self.client.delete_bucket(Bucket=bucket)

    def test_slow_response_reader_keeps_full_object_consistent(self):
        bucket = f"{self.bucket}-slow-get"
        key = "response/large.bin"
        payload = bytes(range(256)) * (16384 + 1)
        self.client.create_bucket(Bucket=bucket)
        self.client.put_object(Bucket=bucket, Key=key, Body=payload)
        response = self.client.get_object(Bucket=bucket, Key=key)
        first = response["Body"].read(1)
        time.sleep(0.25)
        self.client.delete_object(Bucket=bucket, Key=key)
        self.assertEqual(first + response["Body"].read(), payload)
        self.assertEqual(response["ETag"], f'"{md5(payload).hexdigest()}"')
        with self.assertRaises(ClientError) as absent:
            self.client.head_object(Bucket=bucket, Key=key)
        self.assertEqual(absent.exception.response["ResponseMetadata"]["HTTPStatusCode"], 404)
        self.client.delete_bucket(Bucket=bucket)
