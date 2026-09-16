# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import os
import random
import time
import unittest
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, timezone
from email.utils import format_datetime
from hashlib import md5, sha256
from http.client import HTTPConnection
from io import BytesIO
from socket import SHUT_WR
from threading import Barrier
from urllib.parse import quote, urlsplit
from xml.etree import ElementTree

import boto3
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError


class FragmentedBody(BytesIO):
    def __init__(self, payload, seed):
        super().__init__(payload)
        self.random = random.Random(seed)

    def read(self, size=-1):
        remaining = len(self.getbuffer()) - self.tell()
        if remaining == 0:
            return b""
        requested = remaining if size is None or size < 0 else min(size, remaining)
        if requested == 0:
            return b""
        fragment = self.random.randint(1, min(requested, 32 * 1024))
        return super().read(fragment)


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
        cls.endpoint = endpoint

    def signed_http(
        self, method, path, body=b"", headers=None, corrupt_signature=False, slow_chunk_size=0,
        truncate_after=None,
    ):
        parsed = urlsplit(self.endpoint)
        self.assertEqual(parsed.scheme, "http")
        url = f"{self.endpoint}{path}"
        signed_headers = {
            "Host": parsed.netloc,
            "x-amz-content-sha256": sha256(body).hexdigest(),
            **(headers or {}),
        }
        if slow_chunk_size or truncate_after is not None:
            signed_headers["Content-Length"] = str(len(body))
        request = AWSRequest(method=method, url=url, data=body, headers=signed_headers)
        credentials = Credentials(
            os.environ.get("CROWDB_S3_E2E_ACCESS_KEY", "test-access"),
            os.environ.get("CROWDB_S3_E2E_SECRET_KEY", "test-secret"),
        )
        S3SigV4Auth(credentials, "s3", os.environ.get("CROWDB_S3_E2E_REGION", "us-east-1")).add_auth(request)
        if corrupt_signature:
            authorization = request.headers["Authorization"]
            request.headers["Authorization"] = authorization[:-1] + (
                "0" if authorization[-1] != "0" else "1"
            )
        connection = HTTPConnection(parsed.hostname, parsed.port, timeout=15)
        try:
            if truncate_after is not None:
                connection.putrequest(method, path, skip_host=True)
                for name, value in request.headers.items():
                    connection.putheader(name, value)
                connection.endheaders()
                connection.send(body[:truncate_after])
                connection.sock.shutdown(SHUT_WR)
                return None
            if slow_chunk_size:
                connection.putrequest(method, path, skip_host=True)
                for name, value in request.headers.items():
                    connection.putheader(name, value)
                connection.endheaders()
                # Let Hyper finish the headers before body bytes arrive so
                # this case exercises async owner-credit waiting, not its
                # separate synchronous header read-ahead path.
                time.sleep(0.05)
                for offset in range(0, len(body), slow_chunk_size):
                    connection.send(body[offset : offset + slow_chunk_size])
                    time.sleep(0.002)
            else:
                connection.request(method, path, body=body, headers=dict(request.headers.items()))
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def test_signed_raw_http_wire_contract(self):
        bucket = f"{self.bucket}-raw"
        key = "raw/%25+边界.bin"
        path = f"/{bucket}/{quote(key, safe='/')}"
        payload = b"raw-http-object\x00payload"

        status, _, _ = self.signed_http("PUT", f"/{bucket}")
        self.assertEqual(status, 200)
        status, _, data = self.signed_http("HEAD", f"/{bucket}")
        self.assertEqual(status, 200)
        self.assertEqual(data, b"")
        status, _, data = self.signed_http("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn(bucket.encode(), data)
        status, headers, data = self.signed_http("PUT", path, payload)
        self.assertEqual(status, 200, data)
        self.assertEqual(headers["etag"], f'"{md5(payload).hexdigest()}"')
        status, headers, data = self.signed_http("GET", path, headers={"Range": "bytes=4-10"})
        self.assertEqual(status, 206)
        self.assertEqual(headers["content-range"], f"bytes 4-10/{len(payload)}")
        self.assertEqual(data, payload[4:11])
        status, _, data = self.signed_http("GET", path, headers={"Range": "bytes=-4"})
        self.assertEqual(status, 206)
        self.assertEqual(data, payload[-4:])
        status, _, data = self.signed_http("GET", path, headers={"Range": "bytes=4-"})
        self.assertEqual(status, 206)
        self.assertEqual(data, payload[4:])
        status, headers, data = self.signed_http("HEAD", path)
        self.assertEqual(status, 200)
        self.assertEqual(int(headers["content-length"]), len(payload))
        self.assertEqual(data, b"")
        status, _, data = self.signed_http("GET", path, headers={"If-None-Match": headers["etag"]})
        self.assertEqual(status, 304)
        self.assertEqual(data, b"")
        modified = self.client.head_object(Bucket=bucket, Key=key)["LastModified"].astimezone(timezone.utc)
        status, _, data = self.signed_http(
            "GET", path, headers={"If-Modified-Since": format_datetime(modified + timedelta(days=1), usegmt=True)}
        )
        self.assertEqual(status, 304)
        self.assertEqual(data, b"")
        status, _, data = self.signed_http(
            "GET", path, headers={"If-Unmodified-Since": format_datetime(modified - timedelta(days=1), usegmt=True)}
        )
        self.assertEqual(status, 412)
        self.assertIn(b"PreconditionFailed", data)
        status, _, data = self.signed_http("GET", path, headers={"If-Match": '"wrong"'})
        self.assertEqual(status, 412)
        self.assertIn(b"PreconditionFailed", data)
        status, _, data = self.signed_http("GET", path, headers={"Range": "bytes=999-1000"})
        self.assertEqual(status, 416)
        self.assertIn(b"InvalidRange", data)
        status, _, data = self.signed_http("GET", path, headers={"Range": "bytes=0-1,4-5"})
        self.assertEqual(status, 416)
        self.assertIn(b"InvalidRange", data)
        status, _, data = self.signed_http("GET", f"{path}?versionId=1")
        self.assertEqual(status, 501)
        self.assertIn(b"NotImplemented", data)
        status, _, data = self.signed_http("GET", path, corrupt_signature=True)
        self.assertEqual(status, 403)
        self.assertIn(b"AccessDenied", data)
        status, _, data = self.signed_http("GET", f"/{bucket}?list-type=2&prefix=raw%2F")
        self.assertEqual(status, 200)
        self.assertIn(b"<Key>raw/%25+", data)
        second_path = f"/{bucket}/raw/second.bin"
        status, _, _ = self.signed_http("PUT", second_path, b"second")
        self.assertEqual(status, 200)
        status, _, data = self.signed_http("GET", f"/{bucket}?list-type=2&max-keys=1")
        self.assertEqual(status, 200)
        first_page = ElementTree.fromstring(data)
        token = first_page.findtext(".//{*}NextContinuationToken")
        self.assertTrue(token)
        status, _, data = self.signed_http(
            "GET", f"/{bucket}?list-type=2&max-keys=1&continuation-token={quote(token, safe='')}"
        )
        self.assertEqual(status, 200)
        second_page = ElementTree.fromstring(data)
        self.assertEqual(second_page.findtext(".//{*}Key"), "raw/second.bin")
        status, _, data = self.signed_http("GET", f"/{bucket}?list-type=2&continuation-token=invalid")
        self.assertEqual(status, 400)
        self.assertIn(b"InvalidRequest", data)
        status, _, _ = self.signed_http("DELETE", path)
        self.assertEqual(status, 204)
        status, _, _ = self.signed_http("DELETE", path)
        self.assertEqual(status, 204)
        status, _, data = self.signed_http("GET", path)
        self.assertEqual(status, 404)
        self.assertIn(b"NoSuchKey", data)
        status, _, _ = self.signed_http("DELETE", second_path)
        self.assertEqual(status, 204)
        status, _, _ = self.signed_http("DELETE", f"/{bucket}")
        self.assertEqual(status, 204)

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

    def test_slow_signed_upload_releases_native_buffers(self):
        bucket = f"{self.bucket}-slow"
        path = f"/{bucket}/slow.bin"
        second_path = f"/{bucket}/slow-second.bin"
        payload = bytes(range(256)) * (4096 + 1)
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
        backpressure = next(
            int(line.split()[-1])
            for line in metrics.splitlines()
            if line.startswith(b"crowdb_s3_native_backpressure_events_total ")
        )
        self.assertGreater(backpressure, 0)
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

    def test_fragmentation_and_storage_boundaries(self):
        client = self.client
        bucket = f"{self.bucket}-boundaries"
        client.create_bucket(Bucket=bucket)
        sizes = [0, 1, 65505, 65506, 65507, 1024 * 1024 + 31, 4 * 1024 * 1024 + 127]
        keys = []
        for index, size in enumerate(sizes):
            key = f"encoded/边界-{index}-%00.bin"
            payload = bytes((offset * 31 + index) % 256 for offset in range(size))
            digest = md5(payload)
            result = client.put_object(
                Bucket=bucket,
                Key=key,
                Body=FragmentedBody(payload, index + 17),
                ContentLength=size,
                ContentMD5=b64encode(digest.digest()).decode("ascii"),
            )
            self.assertEqual(result["ETag"], f'"{digest.hexdigest()}"')
            fetched = client.get_object(Bucket=bucket, Key=key)["Body"].read()
            self.assertEqual(fetched, payload)
            if size:
                start = min(size - 1, 65500)
                end = min(size - 1, start + 31)
                ranged = client.get_object(
                    Bucket=bucket,
                    Key=key,
                    Range=f"bytes={start}-{end}",
                )["Body"].read()
                self.assertEqual(ranged, payload[start : end + 1])
            keys.append(key)

        listed = client.list_objects_v2(Bucket=bucket, Prefix="encoded/")
        self.assertEqual(len(listed.get("Contents", [])), len(keys))
        for key in keys:
            client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=bucket)


if __name__ == "__main__":
    unittest.main()
