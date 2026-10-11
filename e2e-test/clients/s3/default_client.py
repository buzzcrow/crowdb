# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import os
import time
from base64 import b64encode
from http.client import HTTPConnection
from io import BytesIO
from urllib.parse import urlsplit
from zlib import crc32

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class DefaultClientCases:
    def default_client(self):
        return boto3.client("s3", endpoint_url=self.endpoint, region_name="us-east-1",
                            aws_access_key_id=os.environ["CROWDB_S3_E2E_ACCESS_KEY"],
                            aws_secret_access_key=os.environ["CROWDB_S3_E2E_SECRET_KEY"],
                            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))

    def test_default_boto3_checksums_and_multipart(self):
        client = self.default_client()
        observed = []
        def record(request, **kwargs):
            # Retain only checksum declarations, never auth or complete headers.
            observed.append((request.headers.get("x-amz-sdk-checksum-algorithm"),
                             request.headers.get("x-amz-checksum-crc32")))
        client.meta.events.register("before-send.s3.PutObject", record)
        client.meta.events.register("before-send.s3.UploadPart", record)
        bucket = self.bucket + "-default"
        payload = bytes(range(256)) * (48 * 1024)
        client.create_bucket(Bucket=bucket)
        for key, body in [("ordinary", payload[:65537]), ("fragmented", BytesIO(payload))]:
            client.put_object(Bucket=bucket, Key=key, Body=body)
        client.upload_fileobj(BytesIO(payload), bucket, "multipart")
        self.assertTrue(observed)
        self.assertTrue(all(algorithm == b"CRC32" and value for algorithm, value in observed), observed)
        for key in ["fragmented", "multipart"]:
            self.assertEqual(client.get_object(Bucket=bucket, Key=key)["Body"].read(), payload)
        with self.assertRaises(ClientError) as bad:
            client.put_object(Bucket=bucket, Key="ordinary", Body=b"corrupt", ChecksumCRC32="AAAAAA==")
        self.assertEqual(bad.exception.response["Error"]["Code"], "BadDigest")
        for malformed in ["invalid", "YQ=="]:
            status, _, error = self.signed_http("PUT", f"/{bucket}/ordinary", b"corrupt",
                                               {"Content-MD5": malformed})
            self.assertEqual(status, 400)
            self.assertIn(b"<Code>InvalidDigest</Code>", error)
        self.assertEqual(client.get_object(Bucket=bucket, Key="ordinary")["Body"].read(), payload[:65537])
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": key} for key in
                              ["ordinary", "fragmented", "multipart"]]})
        client.delete_bucket(Bucket=bucket)

    def presigned_http(self, method, url, body=None):
        parsed = urlsplit(url)
        connection = HTTPConnection(parsed.hostname, parsed.port, timeout=15)
        try:
            connection.request(method, parsed.path + "?" + parsed.query, body=body)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def test_presigned_transfers_tamper_and_expiry(self):
        client = self.default_client()
        bucket = self.bucket + "-presign"
        client.create_bucket(Bucket=bucket)
        params = {"Bucket": bucket, "Key": "exact/%25+边界"}
        put = client.generate_presigned_url("put_object", Params=params, ExpiresIn=60)
        self.assertEqual(self.presigned_http("PUT", put, b"presigned bytes")[0], 200)
        get = client.generate_presigned_url("get_object", Params=params, ExpiresIn=60)
        self.assertEqual(self.presigned_http("GET", get), (200, b"presigned bytes"))
        self.assertEqual(self.presigned_http("GET", get + "&tampered=1")[0], 403)
        expired = client.generate_presigned_url("get_object", Params=params, ExpiresIn=1)
        time.sleep(2)
        self.assertEqual(self.presigned_http("GET", expired)[0], 403)
        client.delete_object(**params)
        client.delete_bucket(Bucket=bucket)

    def test_aws_chunked_trailers_are_verified_before_publication(self):
        bucket = self.bucket + "-trailers"
        self.client.create_bucket(Bucket=bucket)
        payload = b"chunked trailer payload" * 5001
        checksum = b64encode(crc32(payload).to_bytes(4, "big"))
        wire = (f"{len(payload):x}\r\n".encode() + payload + b"\r\n0\r\n"
                + b"x-amz-checksum-crc32:" + checksum + b"\r\n\r\n")
        headers = {"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                   "Content-Encoding": "aws-chunked", "x-amz-trailer": "x-amz-checksum-crc32",
                   "x-amz-decoded-content-length": str(len(payload))}
        status, _, response = self.signed_http("PUT", f"/{bucket}/selected", wire, headers,
                                              slow_chunk_size=8192)
        self.assertEqual(status, 200, response)
        status, _, _ = self.signed_http("GET", f"/{bucket}/selected", wire, headers)
        self.assertEqual(status, 403)
        status, _, _ = self.signed_http("PUT", f"/{bucket}-forbidden", wire, headers)
        self.assertEqual(status, 400)
        with self.assertRaises(ClientError) as missing:
            self.client.head_bucket(Bucket=bucket + "-forbidden")
        self.assertEqual(missing.exception.response["ResponseMetadata"]["HTTPStatusCode"], 404)
        for bad in [wire.replace(checksum, b"AAAAAA=="), wire[:-3], wire + b"suffix"]:
            status, _, _ = self.signed_http("PUT", f"/{bucket}/selected", bad, headers)
            self.assertEqual(status, 400)
            self.assertEqual(self.client.get_object(Bucket=bucket, Key="selected")["Body"].read(), payload)
        self.client.delete_object(Bucket=bucket, Key="selected")
        self.client.delete_bucket(Bucket=bucket)
