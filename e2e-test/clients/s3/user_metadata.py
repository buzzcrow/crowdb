# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""User metadata belongs to the same immutable generation as object bytes."""

class UserMetadataCases:
    def test_user_metadata_publication_copy_and_multipart(self):
        bucket = "crowdb-user-metadata"
        self.client.create_bucket(Bucket=bucket)
        original = {"mtime": "1700000000.123", "origin": "source", "empty": ""}
        self.client.put_object(Bucket=bucket, Key="source", Body=b"first", Metadata=original)
        for operation in [self.client.head_object, self.client.get_object]:
            result = operation(Bucket=bucket, Key="source")
            self.assertEqual(result["Metadata"], original)
            if "Body" in result:
                self.assertEqual(result["Body"].read(), b"first")
        ranged = self.client.get_object(Bucket=bucket, Key="source", Range="bytes=1-2")
        self.assertEqual(ranged["Metadata"], original)
        self.assertEqual(ranged["Body"].read(), b"ir")
        self.client.copy_object(Bucket=bucket, Key="copied", CopySource=f"{bucket}/source")
        self.client.put_object(Bucket=bucket, Key="source", Body=b"second", Metadata={"origin": "new"})
        copied = self.client.get_object(Bucket=bucket, Key="copied")
        self.assertEqual(copied["Metadata"], original)
        self.assertEqual(copied["Body"].read(), b"first")
        for replacement in [{"mtime": "42", "other": "value"}, {}]:
            self.client.copy_object(Bucket=bucket, Key="copied", CopySource=f"{bucket}/copied",
                                    MetadataDirective="REPLACE", Metadata=replacement)
            self.assertEqual(self.client.head_object(Bucket=bucket, Key="copied")["Metadata"], replacement)
        self.client.put_object(Bucket=bucket, Key="source", Body=b"third")
        self.assertEqual(self.client.head_object(Bucket=bucket, Key="source")["Metadata"], {})
        upload = self.client.create_multipart_upload(Bucket=bucket, Key="mpu", Metadata=original)["UploadId"]
        for payload in [b"discarded", b"selected"]:
            part = self.client.upload_part(Bucket=bucket, Key="mpu", UploadId=upload, PartNumber=1, Body=payload)
        self.client.complete_multipart_upload(Bucket=bucket, Key="mpu", UploadId=upload,
                                              MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]})
        result = self.client.get_object(Bucket=bucket, Key="mpu")
        self.assertEqual(result["Metadata"], original)
        self.assertEqual(result["Body"].read(), b"selected")
        for key in ["source", "copied", "mpu"]:
            self.client.delete_object(Bucket=bucket, Key=key)
        self.client.delete_bucket(Bucket=bucket)

    def test_invalid_user_metadata_preserves_objects_and_sessions(self):
        bucket = "crowdb-invalid-metadata"
        self.client.create_bucket(Bucket=bucket)
        path = f"/{bucket}/key"
        status, _, _ = self.signed_http("PUT", path, b"original", {"X-Amz-Meta-Mtime": "123"})
        self.assertEqual(status, 200)
        self.assertEqual(self.client.head_object(Bucket=bucket, Key="key")["Metadata"], {"mtime": "123"})
        for headers in [{"x-amz-meta-k": "x" * 2048}, {"x-amz-meta-": "empty-name"},
                        {"x-amz-meta-k": "one", "X-Amz-Meta-K": "two"}]:
            for method, target, payload in [("PUT", path, b"bad"), ("POST", path + "?uploads", b"")]:
                status, _, _ = self.signed_http(method, target, payload, headers)
                self.assertEqual(status, 400)
            status, _, _ = self.signed_http("PUT", path, headers={**headers,
                "x-amz-copy-source": path, "x-amz-metadata-directive": "REPLACE"})
            self.assertEqual(status, 400)
            result = self.client.get_object(Bucket=bucket, Key="key")
            self.assertEqual(result["Body"].read(), b"original")
            self.assertEqual(result["Metadata"], {"mtime": "123"})
            self.assertEqual(self.client.list_multipart_uploads(Bucket=bucket).get("Uploads", []), [])
        self.client.delete_object(Bucket=bucket, Key="key")
        self.client.delete_bucket(Bucket=bucket)
