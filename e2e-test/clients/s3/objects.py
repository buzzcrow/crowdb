# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from base import *


class ObjectsCases:
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

    def test_ordinary_put_size_matrix(self):
        client = self.client
        bucket = f"{self.bucket}-sizes"
        client.create_bucket(Bucket=bucket)
        sizes = [10 * 1024, 1024 * 1024, 12 * 1024 * 1024, 100 * 1024 * 1024]
        for size in sizes:
            key = f"size-{size}.bin"
            payload = bytes(range(256)) * (size // 256)
            digest = md5(payload)
            started = time.monotonic()
            result = client.put_object(
                Bucket=bucket,
                Key=key,
                Body=BytesIO(payload),
                ContentLength=size,
                ContentMD5=b64encode(digest.digest()).decode("ascii"),
            )
            self.assertEqual(result["ETag"], f'"{digest.hexdigest()}"')
            put_s = time.monotonic() - started
            self.assertEqual(client.head_object(Bucket=bucket, Key=key)["ContentLength"], size)
            get_started = time.monotonic()
            fetched = client.get_object(Bucket=bucket, Key=key)["Body"]
            downloaded = md5()
            while block := fetched.read(1024 * 1024):
                downloaded.update(block)
            self.assertEqual(downloaded.digest(), digest.digest())
            get_s = time.monotonic() - get_started
            for start in [0, min(size - 1, 65500), size - 1]:
                end = min(size - 1, start + 127)
                ranged = client.get_object(
                    Bucket=bucket, Key=key, Range=f"bytes={start}-{end}"
                )["Body"].read()
                self.assertEqual(ranged, payload[start : end + 1])
            print(f"S3 ordinary size={size} PUT={put_s:.3f}s GET={get_s:.3f}s")
            client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=bucket)
