# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Drop committed PUT and multipart responses at a loopback proxy, then retry."""

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


def swallow_reply(listener, endpoint, expected_status):
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
            content_length = next((
                int(line.split(b":", 1)[1].strip())
                for line in headers.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            ), 0)
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
            assert response.startswith(f"HTTP/1.1 {expected_status} ".encode()), response[:256]
            return bytes(response)


def drop_reply(endpoint, credentials, method, path, payload, expected_status, headers=None):
    parsed = urlsplit(endpoint)
    request = AWSRequest(
        method=method,
        url=f"{endpoint}{path}",
        data=payload,
        headers={
            "Host": parsed.netloc,
            "Content-Length": str(len(payload)),
            "Connection": "close",
            "x-amz-content-sha256": sha256(payload).hexdigest(),
            **(headers or {}),
        },
    )
    S3SigV4Auth(credentials, "s3", os.environ.get("CROWDB_S3_E2E_REGION", "us-east-1")).add_auth(request)

    with socket.socket() as listener, ThreadPoolExecutor(max_workers=1) as workers:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        forwarded = workers.submit(swallow_reply, listener, endpoint, expected_status)
        proxy = HTTPConnection("127.0.0.1", listener.getsockname()[1], timeout=15)
        try:
            proxy.request(method, path, body=payload, headers=dict(request.headers.items()))
            try:
                proxy.getresponse()
            except RemoteDisconnected:
                pass
            else:
                raise AssertionError(f"proxy unexpectedly returned the completed {method} response")
        finally:
            proxy.close()
        return forwarded.result(timeout=20)


def main():
    endpoint = os.environ["CROWDB_S3_E2E_ENDPOINT"]
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
    response = drop_reply(endpoint, credentials, "PUT", f"/{bucket}/{key}", payload, 200)
    assert f"\r\netag: {etag}\r\n".lower().encode() in response.lower(), response[:512]

    assert client.put_object(Bucket=bucket, Key=key, Body=payload)["ETag"] == etag
    assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == payload
    listed = client.list_objects_v2(Bucket=bucket, Prefix="retry/")
    assert [item["Key"] for item in listed["Contents"]] == [key]
    copied = "retry/copied-response-loss.bin"
    drop_reply(endpoint, credentials, "PUT", f"/{bucket}/{copied}", b"", 200,
               {"x-amz-copy-source": f"/{bucket}/{key}"})
    assert client.get_object(Bucket=bucket, Key=copied)["Body"].read() == payload
    result = client.copy_object(Bucket=bucket, Key=copied, CopySource={"Bucket": bucket, "Key": key})
    assert result["CopyObjectResult"]["ETag"] == etag
    assert client.get_object(Bucket=bucket, Key=copied)["Body"].read() == payload
    client.delete_object(Bucket=bucket, Key=copied)
    client.delete_object(Bucket=bucket, Key=key)

    multipart_key = "retry/multipart.bin"
    part = b"multipart-response-loss" * 512
    part_etag = f'"{md5(part).hexdigest()}"'
    upload_id = client.create_multipart_upload(Bucket=bucket, Key=multipart_key)["UploadId"]
    query = f"?partNumber=1&uploadId={upload_id}"
    response = drop_reply(endpoint, credentials, "PUT", f"/{bucket}/{multipart_key}{query}", part, 200)
    assert f"\r\netag: {part_etag}\r\n".lower().encode() in response.lower(), response[:512]
    assert client.upload_part(Bucket=bucket, Key=multipart_key, UploadId=upload_id,
                              PartNumber=1, Body=part)["ETag"] == part_etag
    copy_source = "retry/part-copy-source.bin"
    client.put_object(Bucket=bucket, Key=copy_source, Body=part)
    drop_reply(endpoint, credentials, "PUT", f"/{bucket}/{multipart_key}{query}", b"", 200,
               {"x-amz-copy-source": f"/{bucket}/{copy_source}"})
    assert client.upload_part_copy(Bucket=bucket, Key=multipart_key, UploadId=upload_id,
                                   PartNumber=1, CopySource={"Bucket": bucket, "Key": copy_source})["CopyPartResult"]["ETag"] == part_etag
    client.delete_object(Bucket=bucket, Key=copy_source)
    assert len(client.list_parts(Bucket=bucket, Key=multipart_key, UploadId=upload_id)["Parts"]) == 1

    complete = (
        f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag>"
        "</Part></CompleteMultipartUpload>"
    ).encode()
    path = f"/{bucket}/{multipart_key}?uploadId={upload_id}"
    drop_reply(endpoint, credentials, "POST", path, complete, 200)
    published = client.complete_multipart_upload(
        Bucket=bucket, Key=multipart_key, UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part_etag}]},
    )
    assert published["ETag"] == f'"{md5(md5(part).digest()).hexdigest()}-1"'
    assert client.get_object(Bucket=bucket, Key=multipart_key)["Body"].read() == part
    client.delete_object(Bucket=bucket, Key=multipart_key)

    aborted = client.create_multipart_upload(Bucket=bucket, Key=multipart_key)["UploadId"]
    drop_reply(endpoint, credentials, "DELETE", f"/{bucket}/{multipart_key}?uploadId={aborted}", b"", 204)
    client.abort_multipart_upload(Bucket=bucket, Key=multipart_key, UploadId=aborted)
    assert all(item["UploadId"] != aborted for item in
               client.list_multipart_uploads(Bucket=bucket).get("Uploads", []))
    client.delete_bucket(Bucket=bucket)


if __name__ == "__main__":
    main()
