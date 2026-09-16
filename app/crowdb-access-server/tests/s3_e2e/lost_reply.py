# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Drop a completed PUT response at a loopback proxy, then retry the PUT."""

import os
import socket
from concurrent.futures import ThreadPoolExecutor
from hashlib import md5, sha256
from http.client import HTTPConnection, RemoteDisconnected
from urllib.parse import urlsplit

import boto3
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials


def swallow_put_reply(listener, endpoint):
    parsed = urlsplit(endpoint)
    with listener:
        client, _ = listener.accept()
        with client, socket.create_connection((parsed.hostname, parsed.port), timeout=15) as backend:
            client.settimeout(15)
            backend.settimeout(15)
            request = bytearray()
            while b"\r\n\r\n" not in request:
                received = client.recv(8192)
                assert received, "client closed before signed PUT headers"
                request.extend(received)
            headers, body = bytes(request).split(b"\r\n\r\n", 1)
            content_length = next(
                int(line.split(b":", 1)[1].strip())
                for line in headers.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            backend.sendall(headers + b"\r\n\r\n" + body)
            remaining = content_length - len(body)
            while remaining:
                chunk = client.recv(min(8192, remaining))
                assert chunk, "client closed before its complete signed PUT"
                backend.sendall(chunk)
                remaining -= len(chunk)
            response = bytearray()
            while chunk := backend.recv(8192):
                response.extend(chunk)
            assert response.startswith(b"HTTP/1.1 200 "), response[:256]
            return bytes(response)


def main():
    endpoint = os.environ["CROWDB_S3_E2E_ENDPOINT"]
    parsed = urlsplit(endpoint)
    credentials = Credentials(
        os.environ["CROWDB_S3_E2E_ACCESS_KEY"], os.environ["CROWDB_S3_E2E_SECRET_KEY"]
    )
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("CROWDB_S3_E2E_REGION", "us-east-1"),
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        config=Config(s3={"addressing_style": "path"}),
    )
    bucket = "crowdb-e2e-lost-reply"
    key = "retry/same-payload.bin"
    payload = bytes(range(256)) * 257
    etag = f'"{md5(payload).hexdigest()}"'
    client.create_bucket(Bucket=bucket)
    request = AWSRequest(
        method="PUT",
        url=f"{endpoint}/{bucket}/{key}",
        data=payload,
        headers={
            "Host": parsed.netloc,
            "Content-Length": str(len(payload)),
            "Connection": "close",
            "x-amz-content-sha256": sha256(payload).hexdigest(),
        },
    )
    S3SigV4Auth(credentials, "s3", os.environ.get("CROWDB_S3_E2E_REGION", "us-east-1")).add_auth(request)

    with socket.socket() as listener, ThreadPoolExecutor(max_workers=1) as workers:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        forwarded = workers.submit(swallow_put_reply, listener, endpoint)
        proxy = HTTPConnection("127.0.0.1", listener.getsockname()[1], timeout=15)
        try:
            proxy.request("PUT", f"/{bucket}/{key}", body=payload, headers=dict(request.headers.items()))
            try:
                proxy.getresponse()
            except RemoteDisconnected:
                pass
            else:
                raise AssertionError("proxy unexpectedly returned the completed PUT response")
        finally:
            proxy.close()
        response = forwarded.result(timeout=20)
        assert f"\r\netag: {etag}\r\n".lower().encode() in response.lower(), response[:512]

    assert client.put_object(Bucket=bucket, Key=key, Body=payload)["ETag"] == etag
    assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == payload
    listed = client.list_objects_v2(Bucket=bucket, Prefix="retry/")
    assert [item["Key"] for item in listed["Contents"]] == [key]
    client.delete_object(Bucket=bucket, Key=key)
    client.delete_bucket(Bucket=bucket)


if __name__ == "__main__":
    main()
