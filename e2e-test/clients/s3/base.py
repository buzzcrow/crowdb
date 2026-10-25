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
from botocore.auth import S3SigV4Auth, SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

from http_exchange import request_response


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


class S3ClientTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        endpoint = os.environ.get("CROWDB_S3_E2E_ENDPOINT")
        if not endpoint:
            raise RuntimeError("required release access endpoint is missing")
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
        signer = SigV4Auth if signed_headers["x-amz-content-sha256"].startswith("STREAMING-") else S3SigV4Auth
        signer(credentials, "s3", os.environ.get("CROWDB_S3_E2E_REGION", "us-east-1")).add_auth(request)
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
                response = connection.getresponse()
            else:
                response = request_response(connection, method, path, body,
                                            dict(request.headers.items()))
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()
