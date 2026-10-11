# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from base import *


class CopyCases:
    def test_server_side_copy_preserves_bytes_and_supported_metadata(self):
        source_bucket = f"{self.bucket}-copy-source"
        target_bucket = f"{self.bucket}-copy-target"
        source_key = "encoded/雪 %?+&.json"
        payload = b'{"copy":"immutable"}'
        self.client.create_bucket(Bucket=source_bucket)
        self.client.create_bucket(Bucket=target_bucket)
        source = self.client.put_object(Bucket=source_bucket, Key=source_key, Body=payload,
                                        ContentType="application/json")
        self.client.put_object(Bucket=target_bucket, Key="copy", Body=b"predecessor")
        copied = self.client.copy_object(Bucket=target_bucket, Key="copy",
                                         CopySource={"Bucket": source_bucket, "Key": source_key},
                                         CopySourceIfMatch=source["ETag"], StorageClass="STANDARD")
        self.assertEqual(copied["CopyObjectResult"]["ETag"], source["ETag"])
        self.assertIn("LastModified", copied["CopyObjectResult"])
        self.assertEqual(self.client.get_object(Bucket=target_bucket, Key="copy")["Body"].read(), payload)
        self.assertEqual(self.client.head_object(Bucket=target_bucket, Key="copy")["ContentType"], "application/json")
        for options, code in [({"CopySourceIfMatch": '"wrong"'}, "PreconditionFailed"),
                              ({"CopySourceIfNoneMatch": source["ETag"]}, "PreconditionFailed"),
                              ({"MetadataDirective": "INVALID"}, "InvalidRequest"),
                              ({"MetadataDirective": "REPLACE", "Metadata": {"oversized": "x" * 2048}}, "InvalidRequest")]:
            with self.assertRaises(ClientError) as error:
                self.client.copy_object(Bucket=target_bucket, Key="copy",
                                        CopySource={"Bucket": source_bucket, "Key": source_key}, **options)
            self.assertEqual(error.exception.response["Error"]["Code"], code)
            self.assertEqual(self.client.get_object(Bucket=target_bucket, Key="copy")["Body"].read(), payload)
        with self.assertRaises(ClientError) as error:
            self.client.copy_object(Bucket=source_bucket, Key=source_key,
                                    CopySource={"Bucket": source_bucket, "Key": source_key})
        self.assertEqual(error.exception.response["Error"]["Code"], "InvalidRequest")
        self.client.copy_object(Bucket=source_bucket, Key=source_key,
                                CopySource={"Bucket": source_bucket, "Key": source_key},
                                MetadataDirective="REPLACE", ContentType="text/plain")
        self.assertEqual(self.client.head_object(Bucket=source_bucket, Key=source_key)["ContentType"], "text/plain")
        self.assertEqual(self.client.get_object(Bucket=source_bucket, Key=source_key)["Body"].read(), payload)
        with self.assertRaises(ClientError) as error:
            self.client.copy_object(Bucket=target_bucket, Key="copy",
                                    CopySource={"Bucket": source_bucket, "Key": source_key, "VersionId": "old"})
        self.assertEqual(error.exception.response["Error"]["Code"], "NotImplemented")
        with self.assertRaises(ClientError) as error:
            self.client.copy_object(Bucket=target_bucket, Key="copy",
                                    CopySource={"Bucket": source_bucket, "Key": "missing"})
        self.assertEqual(error.exception.response["Error"]["Code"], "NoSuchKey")
        self.client.delete_object(Bucket=source_bucket, Key=source_key)
        self.client.delete_object(Bucket=target_bucket, Key="copy")
        self.client.delete_bucket(Bucket=source_bucket)
        self.client.delete_bucket(Bucket=target_bucket)

    def test_copy_captures_source_before_overwrite_and_delete(self):
        bucket = f"{self.bucket}-copy-generation"
        payload = bytes(range(256)) * (64 * 1024)
        self.client.create_bucket(Bucket=bucket)
        self.client.put_object(Bucket=bucket, Key="source", Body=payload)
        parsed = urlsplit(self.endpoint)
        path = f"/{bucket}/target"
        request = AWSRequest(method="PUT", url=self.endpoint + path, data=b"", headers={
            "Host": parsed.netloc, "x-amz-content-sha256": sha256(b"").hexdigest(),
            "x-amz-copy-source": f"/{bucket}/source", "Content-Length": "0"})
        credentials = Credentials(os.environ["CROWDB_S3_E2E_ACCESS_KEY"], os.environ["CROWDB_S3_E2E_SECRET_KEY"])
        S3SigV4Auth(credentials, "s3", "us-east-1").add_auth(request)
        connection = HTTPConnection(parsed.hostname, parsed.port, timeout=60)
        try:
            connection.request("PUT", path, body=b"", headers=dict(request.headers.items()))
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.client.put_object(Bucket=bucket, Key="source", Body=b"new generation")
            self.client.delete_object(Bucket=bucket, Key="source")
            root = ElementTree.fromstring(response.read())
            self.assertTrue(root.tag.endswith("CopyObjectResult"), root.tag)
            self.assertEqual(self.client.get_object(Bucket=bucket, Key="target")["Body"].read(), payload)
        finally:
            connection.close()
        self.client.delete_object(Bucket=bucket, Key="target")
        self.client.put_object(Bucket=bucket, Key="source", Body=payload)
        connection = HTTPConnection(parsed.hostname, parsed.port, timeout=60)
        try:
            connection.request("PUT", path, body=b"", headers=dict(request.headers.items()))
            self.assertEqual(connection.getresponse().status, 200)
        finally:
            connection.close()
        try:
            selected = self.client.get_object(Bucket=bucket, Key="target")
        except ClientError as error:
            self.assertEqual(error.response["Error"]["Code"], "NoSuchKey")
        else:
            self.assertEqual(selected["Body"].read(), payload)
        self.client.copy_object(Bucket=bucket, Key="target", CopySource={"Bucket": bucket, "Key": "source"})
        self.assertEqual(self.client.get_object(Bucket=bucket, Key="target")["Body"].read(), payload)
        self.client.delete_object(Bucket=bucket, Key="source")
        self.client.delete_object(Bucket=bucket, Key="target")
        self.client.delete_bucket(Bucket=bucket)
